"""Thin, unit-testable HTTP client for the DDS Ingest API (v0.1).

This layer knows nothing about local folders, parallelism or progress bars: it
only translates the ingest endpoints to/from the typed models in
:mod:`eea_datalakehouse.dds_ingestion.models`. The orchestration logic
lives in :mod:`eea_datalakehouse.dds_ingestion.folder`.

One method per endpoint:

===========================  ==========================================
:meth:`IngestClient.begin`   ``POST /api/v1/ingest/begin``
:meth:`~IngestClient.commit` ``POST /api/v1/ingest/commit``
:meth:`~IngestClient.get_status`    ``GET /api/v1/ingest/{id}``
:meth:`~IngestClient.list_sessions` ``GET /api/v1/ingest``
:meth:`~IngestClient.retry`         ``POST /api/v1/ingest/{id}/retry``
:meth:`~IngestClient.cancel`        ``DELETE /api/v1/ingest/{id}``
:meth:`~IngestClient.estimate`      ``POST /api/v1/ingest/estimate``
:meth:`~IngestClient.stage`         ``POST /api/v1/ingest/stage``
===========================  ==========================================

plus :meth:`~IngestClient.upload_file` / :meth:`~IngestClient.upload_file_multipart`,
which talk to S3 through the presigned targets ``begin`` issued.

Authentication uses a Bearer token (the Dremio PAT, which is the kernel's
``_DREMIO_PWD``) sent **only on the DDS API calls** — never on the presigned S3
upload, which carries its own auth in the URL. The token is held in a private
header dict and is never logged by this module.
"""

from __future__ import annotations

from types import TracebackType
from typing import Any

import httpx

from .credentials import DremioCreds
from .models import (
    BeginResult,
    CommitResult,
    DataFormat,
    EstimateResult,
    FileSpec,
    Intent,
    StageResult,
    StatusResult,
    UploadTarget,
)

DEFAULT_TIMEOUT = 60.0


class IngestApiError(RuntimeError):
    """A DDS ingest API call returned a non-success status."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f"DDS ingest API error {status_code}: {message}")
        self.status_code = status_code
        self.message = message


class IngestClient:
    """Client for the ``/api/v1/ingest/*`` endpoints.

    The same :class:`httpx.Client` is reused for the presigned uploads so we
    avoid creating a fresh connection pool per file.
    """

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
        # Bearer PAT, applied per-request to DDS calls only (not the S3 upload).
        self._auth_headers = {"Authorization": f"Bearer {creds.password}"}

    def __repr__(self) -> str:
        # Deliberately omits _auth_headers: printing/logging the client must
        # never render the Bearer PAT, even via a debugger or unhandled traceback.
        return f"IngestClient(base_url={self._base_url!r})"

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> IngestClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- API endpoints ----------------------------------------------------

    def begin(
        self,
        *,
        target_catalog_path: str,
        intent: Intent,
        data_format: DataFormat,
        conflict_mode: str,
        files: list[FileSpec],
        table_name: str | None = None,
        sub_path: str | None = None,
        idempotency_key: str | None = None,
        multipart: bool | None = None,
    ) -> BeginResult:
        body: dict[str, Any] = {
            "target_catalog_path": target_catalog_path,
            "intent": intent,
            "format": data_format,
            "conflict_mode": conflict_mode,
            "runner": "notebook",
            "files": [f.as_payload() for f in files],
        }
        if table_name is not None:
            body["table_name"] = table_name
        if sub_path:
            body["sub_path"] = sub_path
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        if multipart is not None:
            body["multipart"] = multipart
        data = self._post("/api/v1/ingest/begin", body)
        return BeginResult.from_json(data)

    def commit(
        self,
        *,
        session_id: str,
        definition: dict[str, Any] | None = None,
        multipart_etags: list[dict[str, Any]] | None = None,
    ) -> CommitResult:
        body: dict[str, Any] = {"session_id": session_id}
        if definition is not None:
            body["definition"] = definition
        if multipart_etags:
            # DI-7.2 commit shape: [{rel_path, upload_id, parts:[{part_number, etag}]}]
            body["multipart_etags"] = multipart_etags
        data = self._post("/api/v1/ingest/commit", body)
        return CommitResult.from_json(data)

    def get_status(self, session_id: str) -> StatusResult:
        """Current state of one session (``GET /api/v1/ingest/{id}``)."""
        resp = self._http.get(
            f"{self._base_url}/api/v1/ingest/{session_id}", headers=self._auth_headers
        )
        self._raise_for_status(resp)
        return StatusResult.from_json(resp.json())

    def list_sessions(self, state: str | None = None) -> list[StatusResult]:
        """Your ingest sessions (``GET /api/v1/ingest``).

        ``state`` filters server-side: ``"active"`` (still running),
        ``"failed"``, or ``"all"``. Omit for the server's default view.
        """
        params = {"state": state} if state else None
        resp = self._http.get(
            f"{self._base_url}/api/v1/ingest", params=params, headers=self._auth_headers
        )
        self._raise_for_status(resp)
        payload = resp.json()
        rows = payload if isinstance(payload, list) else payload.get("sessions", [])
        return [StatusResult.from_json(row) for row in rows]

    def retry(self, session_id: str) -> StatusResult:
        """Re-run a failed session's last stage (``POST /api/v1/ingest/{id}/retry``).

        Only a ``failed`` session can be retried, and only the Dremio load stage
        is resumable — the server kept the staged files for exactly that case
        (see :attr:`StatusResult.is_resumable`). A session whose upload never
        completed, or whose failure a re-run cannot fix, raises
        :class:`IngestApiError` telling you to upload again.
        """
        resp = self._http.post(
            f"{self._base_url}/api/v1/ingest/{session_id}/retry",
            headers=self._auth_headers,
        )
        self._raise_for_status(resp)
        return StatusResult.from_json(resp.json())

    def cancel(self, session_id: str) -> None:
        """Discard a session and its staged data (``DELETE /api/v1/ingest/{id}``).

        Use it to abandon a transfer: the staged objects are deleted and the
        session is dropped. It does NOT remove a table that a previous commit
        already created.
        """
        resp = self._http.delete(
            f"{self._base_url}/api/v1/ingest/{session_id}", headers=self._auth_headers
        )
        self._raise_for_status(resp)

    def estimate(self, session_id: str) -> EstimateResult:
        """Row-count + size class for the staged data (``POST .../estimate``).

        A pre-flight check between upload and commit. On a managed catalog the
        count comes from the staged Parquet footers (CSV/JSON have none and
        report 0).
        """
        data = self._post("/api/v1/ingest/estimate", {"session_id": session_id})
        return EstimateResult.from_json(data)

    def stage(self, session_id: str, rel_path: str, data: bytes) -> StageResult:
        """Upload one file THROUGH the server (``POST /api/v1/ingest/stage``).

        The fallback when the S3 endpoint is not reachable from this kernel: DDS
        writes the bytes with its own catalog credentials instead of handing back
        a presigned URL. Slower — the bytes transit the server — so prefer
        :meth:`upload_file`, and use this when that fails to connect.
        """
        resp = self._http.post(
            f"{self._base_url}/api/v1/ingest/stage",
            data={"session_id": session_id, "rel_path": rel_path},
            files={"file": (rel_path.rsplit("/", 1)[-1], data)},
            headers=self._auth_headers,
        )
        self._raise_for_status(resp)
        return StageResult.from_json(resp.json())

    def upload_file(self, target: UploadTarget, data: bytes) -> str | None:
        """Upload one file's bytes to its presigned target.

        For a pre-signed **POST policy** (the default, ``method == "POST"``) this
        submits the form ``fields`` plus a ``file`` part as multipart/form-data;
        for a legacy pre-signed PUT it streams the body. Returns the ETag header
        if supplied, else ``None``; raises :class:`IngestApiError` on non-2xx.

        Multipart targets (DI-7) are handled by :meth:`upload_file_multipart`,
        which returns the per-part ETags; this method covers the small-file path.

        This is the *only* path that touches object storage, and it uses the
        presigned URL/fields exactly as issued — no S3 SDK or credentials.
        """

        if target.max_bytes is not None and len(data) > target.max_bytes:
            raise IngestApiError(
                413,
                f"file {target.rel_path!r} ({len(data)} bytes) exceeds "
                f"max_bytes ({target.max_bytes})",
            )
        if target.method.upper() == "POST":
            # Pre-signed POST policy: form fields first, then the `file` part
            # (file MUST be last so S3 honours the policy conditions).
            resp = self._http.post(
                target.url,
                data=target.fields,
                files={"file": (target.rel_path, data)},
            )
        else:  # legacy pre-signed PUT
            resp = self._http.request(
                target.method, target.url, content=data, headers=target.headers
            )
        self._raise_for_status(resp)
        return resp.headers.get("ETag")

    def upload_file_multipart(
        self, target: UploadTarget, data: bytes
    ) -> list[dict[str, Any]]:
        """Upload one large file as S3 multipart parts (DI-7.3).

        Splits ``data`` into ``len(target.parts)`` chunks and ``PUT``s each chunk
        to its presigned part URL, collecting the ETag S3 returns per part. The
        chunk size is derived from the part count so the parts tile the bytes
        exactly (the last part takes the remainder). Returns the commit-shaped
        ``[{"part_number", "etag"}, ...]`` for the file's ``multipart_etags``.

        As with :meth:`upload_file`, this is the only object-storage path and it
        carries no DDS credentials — each part URL is self-authenticating.
        """
        if not target.is_multipart:
            raise IngestApiError(
                400, f"target {target.rel_path!r} is not a multipart upload"
            )
        if target.max_bytes is not None and len(data) > target.max_bytes:
            raise IngestApiError(
                413,
                f"file {target.rel_path!r} ({len(data)} bytes) exceeds "
                f"max_bytes ({target.max_bytes})",
            )
        num_parts = len(target.parts)
        # Ceil division so num_parts chunks cover all bytes; the final chunk is
        # whatever remains. An empty file still yields one (empty) part.
        chunk = max(1, -(-len(data) // num_parts)) if data else 0
        etags: list[dict[str, Any]] = []
        for index, part in enumerate(
            sorted(target.parts, key=lambda p: p.part_number)
        ):
            start = index * chunk
            body = data[start : start + chunk] if chunk else b""
            resp = self._http.put(part.url, content=body)
            self._raise_for_status(resp)
            etag = resp.headers.get("ETag")
            if etag is None:
                raise IngestApiError(
                    502,
                    f"S3 returned no ETag for part {part.part_number} "
                    f"of {target.rel_path!r}",
                )
            etags.append({"part_number": part.part_number, "etag": etag})
        return etags

    # -- internals --------------------------------------------------------

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        resp = self._http.post(
            f"{self._base_url}{path}", json=body, headers=self._auth_headers
        )
        self._raise_for_status(resp)
        result: dict[str, Any] = resp.json()
        return result

    @staticmethod
    def _raise_for_status(resp: httpx.Response) -> None:
        if resp.is_success:
            return
        message = resp.text
        try:
            payload = resp.json()
            if isinstance(payload, dict):
                message = str(payload.get("message") or payload.get("error") or message)
        except ValueError:
            pass
        raise IngestApiError(resp.status_code, message)
