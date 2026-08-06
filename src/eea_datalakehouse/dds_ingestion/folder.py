"""Folder-level ingest orchestration (DI-8.4 / DI-8.5).

:class:`FolderIngest` drives the full ``begin → upload(all files) → commit``
flow for a local folder:

* recurse the folder and keep exactly one data format (single-format scan);
* upload all files to their presigned URLs with configurable parallelism
  (default 4);
* show a progress bar (tqdm, degrading gracefully if absent);
* resume — re-running skips files already uploaded for the session.

The class depends only on :class:`~eea_datalakehouse.dds_ingestion.client.IngestClient`,
so the HTTP layer can be mocked or swapped in tests.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

from .client import IngestClient
from .credentials import DremioCreds, load_base_url, load_creds
from .models import (
    BeginResult,
    CommitResult,
    DataFormat,
    FileSpec,
    Intent,
    UploadTarget,
)
from .progress import make_progress_bar

logger = logging.getLogger("eea_datalakehouse.dds_ingestion")

DEFAULT_PARALLELISM = 4
_FORMAT_EXTENSIONS: dict[DataFormat, tuple[str, ...]] = {
    "parquet": (".parquet",),
    "csv": (".csv",),
    "json": (".json", ".ndjson"),
}


@dataclass(slots=True)
class IngestOutcome:
    """Summary returned by :meth:`FolderIngest.run`."""

    begin: BeginResult
    commit: CommitResult
    files_uploaded: int
    files_skipped: int
    etags: dict[str, str] = field(default_factory=dict)


def scan_folder(folder: Path, data_format: DataFormat) -> list[FileSpec]:
    """Recurse ``folder`` and return files matching ``data_format`` only.

    Other formats present in the folder are ignored (single-format scan). Paths
    are returned folder-relative with forward slashes so they are stable across
    platforms. Results are sorted for deterministic ordering.
    """

    exts = _FORMAT_EXTENSIONS[data_format]
    specs: list[FileSpec] = []
    for path in sorted(folder.rglob("*")):
        if not path.is_file():
            continue
        if path.suffix.lower() not in exts:
            continue
        rel = path.relative_to(folder).as_posix()
        specs.append(FileSpec(rel_path=rel, size=path.stat().st_size))
    return specs


class FolderIngest:
    """Orchestrate ingest of a local folder into a Dremio table via DDS."""

    def __init__(
        self,
        folder: str | Path,
        target_catalog_path: str,
        *,
        data_format: DataFormat,
        intent: Intent = "read_only",
        conflict_mode: str = "fail",
        table_name: str | None = None,
        parallelism: int = DEFAULT_PARALLELISM,
        idempotency_key: str | None = None,
        multipart: bool | None = None,
        show_progress: bool = True,
        client: IngestClient | None = None,
        base_url: str | None = None,
        creds: DremioCreds | None = None,
    ) -> None:
        if parallelism < 1:
            raise ValueError("parallelism must be >= 1")
        self.folder = Path(folder)
        if not self.folder.is_dir():
            raise NotADirectoryError(f"not a directory: {self.folder}")
        self.target_catalog_path = target_catalog_path
        self.data_format = data_format
        self.intent = intent
        self.conflict_mode = conflict_mode
        self.table_name = table_name
        self.parallelism = parallelism
        self.idempotency_key = idempotency_key
        self.multipart = multipart
        self.show_progress = show_progress

        # The client owns the credentials; if the caller did not inject one we
        # build it from the kernel environment. Creds never leave the client.
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            resolved_url = base_url or load_base_url()
            resolved_creds = creds or load_creds()
            self._client = IngestClient(resolved_url, resolved_creds)
            self._owns_client = True

    # -- orchestration ----------------------------------------------------

    def run(self) -> IngestOutcome:
        """Execute the full begin → upload → commit flow."""

        try:
            files = scan_folder(self.folder, self.data_format)
            if not files:
                raise FileNotFoundError(
                    f"no {self.data_format} files found under {self.folder}"
                )
            begin = self._client.begin(
                target_catalog_path=self.target_catalog_path,
                intent=self.intent,
                data_format=self.data_format,
                conflict_mode=self.conflict_mode,
                files=files,
                table_name=self.table_name,
                idempotency_key=self.idempotency_key,
                multipart=self.multipart,
            )
            etags, multipart_etags, uploaded, skipped = self._upload_all(begin)
            commit = self._client.commit(
                session_id=begin.session_id,
                multipart_etags=multipart_etags or None,
            )
            return IngestOutcome(
                begin=begin,
                commit=commit,
                files_uploaded=uploaded,
                files_skipped=skipped,
                etags=etags,
            )
        finally:
            if self._owns_client:
                self._client.close()

    # -- upload phase -----------------------------------------------------

    def _already_done(self, session_id: str) -> set[str]:
        """Return rel_paths already uploaded for this session (resume support).

        Queries the session status; the server reports ``files_done`` and may
        list completed rel_paths under ``raw["uploaded"]``. We only skip files
        the server explicitly names, so resume never wrongly drops a file.
        """

        try:
            status = self._client.get_status(session_id)
        except Exception:  # noqa: BLE001 — status is best-effort for resume
            logger.debug("could not fetch status for resume; uploading all files")
            return set()
        done = status.raw.get("uploaded")
        if isinstance(done, list):
            return {str(p) for p in done}
        return set()

    def _upload_all(
        self, begin: BeginResult
    ) -> tuple[dict[str, str], list[dict[str, object]], int, int]:
        targets = list(begin.s3.uploads)
        done = self._already_done(begin.session_id)
        pending = [t for t in targets if t.rel_path not in done]
        skipped = len(targets) - len(pending)

        # Single-shot ETags (rel_path -> ETag) and multipart commit entries
        # ({rel_path, upload_id, parts:[{part_number, etag}]}) are collected
        # separately: only the latter is sent to commit as ``multipart_etags``.
        etags: dict[str, str] = {}
        multipart_etags: list[dict[str, object]] = []
        bar = make_progress_bar(len(targets), desc="Uploading") if self.show_progress else None
        if bar is not None and skipped:
            bar.update(skipped)

        try:
            if not pending:
                return etags, multipart_etags, 0, skipped
            with ThreadPoolExecutor(max_workers=self.parallelism) as pool:
                futures = {pool.submit(self._upload_one, t): t for t in pending}
                for future in as_completed(futures):
                    target = futures[future]
                    etag, parts = future.result()
                    if parts is not None:
                        multipart_etags.append(
                            {
                                "rel_path": target.rel_path,
                                "upload_id": target.upload_id,
                                "parts": parts,
                            }
                        )
                    elif etag is not None:
                        etags[target.rel_path] = etag
                    if bar is not None:
                        bar.update(1)
        finally:
            if bar is not None:
                bar.close()

        return etags, multipart_etags, len(pending), skipped

    def _upload_one(
        self, target: UploadTarget
    ) -> tuple[str | None, list[dict[str, object]] | None]:
        """Upload one file; return ``(single_etag, multipart_parts)``.

        Exactly one element is non-``None``: a single-shot upload yields the ETag
        (or ``None`` if S3 omitted it); a multipart upload yields the per-part
        ETag list and the single ETag is ``None``.
        """
        data = (self.folder / target.rel_path).read_bytes()
        if target.is_multipart:
            parts = self._client.upload_file_multipart(target, data)
            return None, [dict(p) for p in parts]
        return self._client.upload_file(target, data), None


def ingest_folder(
    folder: str | Path,
    target_catalog_path: str,
    *,
    data_format: DataFormat,
    **kwargs: object,
) -> IngestOutcome:
    """Convenience wrapper: build a :class:`FolderIngest` and run it."""

    return FolderIngest(
        folder,
        target_catalog_path,
        data_format=data_format,
        **kwargs,  # type: ignore[arg-type]
    ).run()
