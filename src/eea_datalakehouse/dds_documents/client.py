"""Thin HTTP client for the DDS document endpoints.

Documents are plain files stored in a DDS folder (an ISO 19115-3 XML in a
dataset's ``metadata`` folder, for instance). They are never registered as
Dremio tables, so this client makes **no ingest calls**: it shares
:class:`~eea_datalakehouse.dds_ingestion.client.IngestClient`'s conventions
(base URL, Bearer PAT, one method per endpoint, errors naming the call), not
its endpoints.

===============================  =============================================
:meth:`DocumentsClient.put`      ``PUT /api/v1/files/{path}``
:meth:`~DocumentsClient.get`     ``GET /api/v1/files/{path}``
:meth:`~DocumentsClient.exists`  ``GET /api/v1/files/{path}``
:meth:`~DocumentsClient.list`    ``GET /api/v1/files?prefix={folder}``
===============================  =============================================

The endpoint shapes below are **interface assumptions** still to confirm with
the DDS team (see ``docs/sdi-integration-plan.md``). Each one is a module
constant so a confirmed contract is a one-line change.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any
from urllib.parse import quote

import httpx

from ..dds_ingestion.credentials import DremioCreds, load_base_url, load_creds

DEFAULT_TIMEOUT = 60.0
# Assumed DDS document contract — to confirm with the DDS team.
FILES_ENDPOINT = "/api/v1/files/{path}"
LIST_ENDPOINT = "/api/v1/files"
OVERWRITE_PARAM = "overwrite"
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
            params={OVERWRITE_PARAM: "true" if overwrite else "false"},
            headers={**self._auth_headers, "Content-Type": content_type},
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
        """Whether a document is stored at ``path``."""
        return self.get(path) is not None

    def list(self, folder: str) -> list[str]:
        """Paths of the documents under ``folder``."""
        prefix = _clean_path(folder)
        resp = self._http.get(
            f"{self._base_url}{LIST_ENDPOINT}",
            params={"prefix": prefix},
            headers=self._auth_headers,
        )
        _raise_for_status(resp, f"GET {LIST_ENDPOINT}?prefix={prefix}")
        payload: Any = resp.json()
        rows = payload.get("files", []) if isinstance(payload, dict) else payload
        return [row["path"] if isinstance(row, dict) else str(row) for row in rows]

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
