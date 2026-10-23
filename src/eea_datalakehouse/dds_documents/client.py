"""Thin HTTP client for the DDS document endpoints.

Documents are plain files stored in a DDS folder (an ISO 19115-3 XML in a
dataset's ``metadata`` folder, for instance). They are never registered as
Dremio tables, so this client makes **no ingest calls**: it shares
:class:`~eea_datalakehouse.dds_ingestion.client.IngestClient`'s conventions
(base URL, Bearer PAT, one method per endpoint, errors naming the call), not
its endpoints.

================================  ============================================
:meth:`DocumentsClient.put`       ``PUT /api/v1/files/{path}``
:meth:`~DocumentsClient.get`      ``GET /api/v1/files/{path}``
:meth:`~DocumentsClient.exists`   ``HEAD /api/v1/files/{path}``
:meth:`~DocumentsClient.list`     ``GET /api/v1/files/{folder}/`` (paged by ``cursor``)
:meth:`~DocumentsClient.folders`  ``GET /api/v1/folders``
================================  ============================================

These are the Dremio Document Service's own routes (its ``/openapi.json``):
the upload body is the raw file, and overwriting is asked for with the
``X-DDS-Overwrite: true`` header (an existing file otherwise answers 409).
``GET /api/v1/files/{path}`` downloads a file, or — when the path ends in
``/`` — lists that folder, a page (``limit``, default 200) at a time. The
service does not publish the listing's JSON shape, so :meth:`list` reads the
usual ones (see :func:`_listing_entries`) and says which keys it found when a
response is none of them.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any
from urllib.parse import quote

import httpx

from ..dds_ingestion.credentials import DremioCreds, load_base_url, load_creds

DEFAULT_TIMEOUT = 60.0
FILES_ENDPOINT = "/api/v1/files/{path}"
# A trailing slash asks DDS for the folder's listing instead of a download;
# without it, /api/v1/files redirects (307) to /api/v1/files/.
LIST_ENDPOINT = "/api/v1/files/{path}/"
FOLDERS_ENDPOINT = "/api/v1/folders"
OVERWRITE_HEADER = "X-DDS-Overwrite"
LIST_PAGE_SIZE = 200
# A listing longer than this many pages is a server that keeps handing back a cursor.
_MAX_LIST_PAGES = 10_000
# Where a listing page keeps its entries, its next-page cursor, and an entry its path.
_ENTRY_KEYS = ("items", "entries", "files", "children", "objects", "contents", "results", "data")
_CURSOR_KEYS = ("next_cursor", "nextCursor", "cursor", "next")
_PATH_KEYS = ("path", "key", "rel_path", "name", "filename")
_FOLDER_TYPES = {"dir", "directory", "folder", "prefix", "collection"}
# Error bodies are quoted back to the caller; cap them so an HTML error page
# does not bury the status line it came with.
_MAX_ERROR_CHARS = 500


class DocumentsApiError(RuntimeError):
    """A DDS document call returned a non-success status.

    ``where`` names the call that failed (e.g. ``PUT /api/v1/files/a/b.xml``).
    """

    def __init__(self, status_code: int, message: str, *, where: str | None = None) -> None:
        super().__init__(
            f"DDS documents API error {status_code}{f' on {where}' if where else ''}: {message}"
        )
        self.status_code = status_code
        self.message = message
        self.where = where


class DocumentExistsError(DocumentsApiError):
    """The file already exists and the upload did not ask to overwrite it (409)."""


class DocumentsClient:
    """Client for the DDS document endpoints."""

    def __init__(
        self,
        base_url: str,
        creds: DremioCreds,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(timeout=timeout)
        self._auth_headers = {"Authorization": f"Bearer {creds.password}"}

    @classmethod
    def from_env(
        cls, env: dict[str, str] | None = None, *, timeout: float = DEFAULT_TIMEOUT
    ) -> DocumentsClient:
        """Build from ``DDS_BASE_URL`` and ``_DREMIO_USER`` / ``_DREMIO_PWD``."""
        return cls(load_base_url(env), load_creds(env), timeout=timeout)

    def __repr__(self) -> str:
        # Deliberately omits _auth_headers so the PAT never reaches output.
        return f"DocumentsClient(base_url={self._base_url!r})"

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> DocumentsClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- API endpoints ----------------------------------------------------

    def put(
        self,
        path: str,
        data: bytes,
        *,
        content_type: str = "application/octet-stream",
        overwrite: bool = False,
    ) -> None:
        """Store ``data`` as the document at ``path``.

        Without ``overwrite`` an existing document is left alone and
        :class:`DocumentExistsError` is raised.
        """
        endpoint = self._file_endpoint(path)
        resp = self._http.put(
            f"{self._base_url}{endpoint}",
            content=data,
            headers={
                **self._auth_headers,
                "Content-Type": content_type,
                OVERWRITE_HEADER: "true" if overwrite else "false",
            },
        )
        if resp.status_code == 409:
            raise DocumentExistsError(409, _error_message(resp), where=f"PUT {endpoint}")
        _raise_for_status(resp, f"PUT {endpoint}")

    def get(self, path: str) -> bytes | None:
        """The document's bytes, or ``None`` if there is none at ``path``."""
        endpoint = self._file_endpoint(path)
        resp = self._http.get(f"{self._base_url}{endpoint}", headers=self._auth_headers)
        if resp.status_code == 404:
            return None
        _raise_for_status(resp, f"GET {endpoint}")
        return resp.content

    def exists(self, path: str) -> bool:
        """Whether a document is stored at ``path`` (a ``HEAD``: nothing is downloaded)."""
        endpoint = self._file_endpoint(path)
        resp = self._http.head(f"{self._base_url}{endpoint}", headers=self._auth_headers)
        if resp.status_code == 404:
            return False
        _raise_for_status(resp, f"HEAD {endpoint}")
        return True

    def list(self, folder: str) -> list[str]:
        """The files in ``folder``, as DDS names them (a path or a bare name),
        every page of them; subfolders are left out. Empty if DDS has no such
        folder (404)."""
        endpoint = LIST_ENDPOINT.format(path=quote(_clean_path(folder), safe="/"))
        params: dict[str, str | int] = {"limit": LIST_PAGE_SIZE}
        found: list[str] = []
        for _ in range(_MAX_LIST_PAGES):
            resp = self._http.get(
                f"{self._base_url}{endpoint}", params=params, headers=self._auth_headers
            )
            if resp.status_code == 404:
                return found
            _raise_for_status(resp, f"GET {endpoint}")
            try:
                payload: Any = resp.json()
            except ValueError as exc:
                raise DocumentsApiError(
                    resp.status_code,
                    f"expected a JSON folder listing, got {resp.headers.get('content-type')!r}",
                    where=f"GET {endpoint}",
                ) from exc
            entries, cursor = _listing_entries(payload, where=f"GET {endpoint}")
            found.extend(_entry_path(entry) for entry in entries if not _is_folder_entry(entry))
            if not cursor or cursor == params.get("cursor"):
                return found
            params["cursor"] = cursor
        raise DocumentsApiError(
            200, f"listing did not end after {_MAX_LIST_PAGES} pages", where=f"GET {endpoint}"
        )

    def folders(self) -> list[str]:
        """The document folder names DDS shows under every catalog node.

        DDS keeps this list in its settings (an admin may change it), so it is
        read live: ``{"folders": [{"key": "metadata", ...}, ...]}``.
        """
        resp = self._http.get(f"{self._base_url}{FOLDERS_ENDPOINT}", headers=self._auth_headers)
        _raise_for_status(resp, f"GET {FOLDERS_ENDPOINT}")
        payload: Any = resp.json()
        rows = payload.get("folders", []) if isinstance(payload, dict) else payload
        return [str(row["key"]) if isinstance(row, dict) else str(row) for row in rows]

    # -- internals --------------------------------------------------------

    @staticmethod
    def _file_endpoint(path: str) -> str:
        return FILES_ENDPOINT.format(path=quote(_clean_path(path), safe="/"))


def _clean_path(path: str) -> str:
    cleaned = path.strip().strip("/")
    if not cleaned:
        raise ValueError("DDS document path must not be empty")
    if any(part in ("", ".", "..") for part in cleaned.split("/")):
        raise ValueError(f"invalid DDS document path {path!r}")
    return cleaned


def _listing_entries(payload: Any, *, where: str) -> tuple[list[Any], str | None]:
    """``(entries, next cursor)`` of one listing page: a bare list, or an object
    keeping its entries under one of ``_ENTRY_KEYS`` and the next page's cursor
    under one of ``_CURSOR_KEYS`` (or none on the last page)."""
    if isinstance(payload, list):
        return payload, None
    if isinstance(payload, dict):
        key = next((k for k in _ENTRY_KEYS if isinstance(payload.get(k), list)), None)
        if key is not None:
            cursor = next((payload[k] for k in _CURSOR_KEYS if payload.get(k)), None)
            return payload[key], str(cursor) if cursor is not None else None
    keys = ", ".join(map(str, payload)) if isinstance(payload, dict) else type(payload).__name__
    raise DocumentsApiError(
        200, f"unrecognised folder listing (top-level keys: {keys or 'none'})", where=where
    )


def _entry_path(entry: Any) -> str:
    if isinstance(entry, dict):
        for key in _PATH_KEYS:
            if isinstance(entry.get(key), str) and entry[key]:
                return str(entry[key])
        raise DocumentsApiError(
            200, f"listing entry without a path or name (keys: {', '.join(map(str, entry))})"
        )
    return str(entry)


def _is_folder_entry(entry: Any) -> bool:
    if isinstance(entry, dict):
        kind = str(entry.get("type") or entry.get("kind") or "").lower()
        if kind in _FOLDER_TYPES or entry.get("is_dir") is True or entry.get("isDir") is True:
            return True
        return any(isinstance(entry.get(k), str) and entry[k].endswith("/") for k in _PATH_KEYS)
    return str(entry).endswith("/")


def _error_message(resp: httpx.Response) -> str:
    """The ``message`` of a DDS ``{error, message}`` body, else the capped text."""
    message = resp.text
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            message = str(payload.get("message") or payload.get("error") or message)
    except ValueError:
        pass
    message = message.strip()
    if len(message) > _MAX_ERROR_CHARS:
        message = message[:_MAX_ERROR_CHARS] + "… (truncated)"
    return message or f"(empty {resp.status_code} response body)"


def _raise_for_status(resp: httpx.Response, where: str) -> None:
    if not resp.is_success:
        raise DocumentsApiError(resp.status_code, _error_message(resp), where=where)
