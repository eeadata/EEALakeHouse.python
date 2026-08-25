"""Folder-level ingest orchestration (DI-8.4 / DI-8.5).

:class:`FolderIngest` drives the full ``begin → upload(all files) → commit``
flow for a local folder:

* recurse the folder and keep exactly one data format (single-format scan);
* upload all files to their presigned URLs with configurable parallelism
  (default 4);
* show a progress bar (tqdm, degrading gracefully if absent);
* resume — re-running skips files already uploaded for the session.

The class depends only on
:class:`~eea_datalakehouse.dds_ingestion.client.IngestClient`, so the HTTP
layer can be mocked or swapped in tests.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .client import IngestClient
from .credentials import DremioCreds, load_base_url, load_creds
from .models import (
    BeginResult,
    CommitResult,
    DataFormat,
    EstimateResult,
    FileSpec,
    Intent,
    StatusResult,
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


class IngestStateError(RuntimeError):
    """An operation was asked for in a state that cannot support it.

    Raised locally, before any HTTP call — e.g. committing a transfer that never
    began, or retrying one that is still running.
    """


@dataclass(slots=True)
class IngestOutcome:
    """Summary returned by :meth:`FolderIngest.run` and :meth:`FolderIngest.retry`.

    ``begin`` is ``None`` when the transfer was resumed rather than started here
    (:meth:`FolderIngest.attach` had no ``begin`` of its own to report), and
    ``resumed`` says which happened.
    """

    begin: BeginResult | None
    commit: CommitResult
    files_uploaded: int
    files_skipped: int
    etags: dict[str, str] = field(default_factory=dict)
    resumed: bool = False


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
    """Orchestrate ingest of a local folder into a Dremio table via DDS.

    ``intent`` decides what the transfer leaves behind, and on a server
    configured for permanent read-only storage that includes **where the files
    end up**:

    * ``"read_only"`` — the upload is stored where the table lives and kept. The
      folder is registered as the dataset (which is also what builds Dremio's
      metadata: schema, file listing, Parquet statistics) and a view at the
      catalog path points at it. Nothing is copied, and the files keep the shape
      they were exported in. Use it for data that is published, not edited.
    * ``"editable"`` — the upload is staged, loaded into an Iceberg table in the
      catalog, and then deleted. Use it for a table that will be written to.

    Against a server that has not enabled permanent storage, both intents stage
    and load as before; the difference is then only the table's shape. Either way
    the destination is the server's to choose — this class uploads to the targets
    ``begin`` issues.

    ``sub_path`` files this upload under a named sub-folder of the table — the
    accumulating-dataset shape, one year at a time::

        FolderIngest(folder="./bw_2026", target_catalog_path=..., data_format="parquet",
                     intent="read_only", table_name="water_temperature",
                     sub_path="2026").run()

    It applies to a **read-only** ingest whose files are stored permanently; the
    server refuses it otherwise rather than filing the data somewhere else. A
    folder that already has the structure locally needs nothing: ``scan_folder``
    keeps sub-folders and the server preserves them.
    """

    def __init__(
        self,
        folder: str | Path,
        target_catalog_path: str,
        *,
        data_format: DataFormat,
        intent: Intent = "read_only",
        conflict_mode: str = "fail",
        table_name: str | None = None,
        sub_path: str | None = None,
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
        self.sub_path = sub_path
        self.parallelism = parallelism
        self.idempotency_key = idempotency_key
        self.multipart = multipart
        self.show_progress = show_progress

        # Set once ``begin`` runs (or by ``attach``); the handle every session
        # command below works from.
        self._session_id: str | None = None
        self._begin: BeginResult | None = None

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

    # -- session handle ---------------------------------------------------

    @property
    def session_id(self) -> str | None:
        """Id of the transfer this object is driving, once ``begin`` has run."""
        return self._session_id

    @classmethod
    def attach(
        cls,
        session_id: str,
        folder: str | Path,
        target_catalog_path: str,
        *,
        data_format: DataFormat,
        **kwargs: Any,
    ) -> FolderIngest:
        """Bind to an EXISTING session instead of starting a new one.

        For picking a transfer back up in a later kernel — inspect it with
        :meth:`status`, resume it with :meth:`retry`, or abandon it with
        :meth:`cancel`. ``folder`` still has to point at the same local data, so
        a re-upload is possible if the staged copy is gone.
        """
        job = cls(folder, target_catalog_path, data_format=data_format, **kwargs)
        job._session_id = session_id
        return job

    def close(self) -> None:
        """Release the HTTP client, if this object created it."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> FolderIngest:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    # -- orchestration ----------------------------------------------------

    def run(self) -> IngestOutcome:
        """Execute the full begin → upload → commit flow.

        The client is left open afterwards so the same object can still
        :meth:`status`, :meth:`retry` or :meth:`cancel` the transfer. Use it as a
        context manager (or call :meth:`close`) to release it.
        """
        begin = self.begin()
        etags, multipart_etags, uploaded, skipped = self._upload_all(begin)
        commit = self.commit(multipart_etags=multipart_etags or None)
        return IngestOutcome(
            begin=begin,
            commit=commit,
            files_uploaded=uploaded,
            files_skipped=skipped,
            etags=etags,
        )

    def begin(self) -> BeginResult:
        """Open the transfer and get the presigned upload targets (step 1 of 3).

        The server validates the target here — an unauthorised or non-existent
        catalog folder fails now, before a single byte is uploaded.
        """
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
            sub_path=self.sub_path,
            idempotency_key=self.idempotency_key,
            multipart=self.multipart,
        )
        self._session_id = begin.session_id
        self._begin = begin
        return begin

    def commit(
        self, *, multipart_etags: list[dict[str, object]] | None = None
    ) -> CommitResult:
        """Finalise the transfer: load the staged files into the table (step 3).

        This is where the Dremio work happens (``CREATE TABLE`` + ``COPY INTO``
        on a managed catalog), so it is the step most likely to fail — see
        :meth:`retry`.
        """
        if self._session_id is None:
            raise IngestStateError("no session to commit — call begin() or run() first")
        return self._client.commit(
            session_id=self._session_id, multipart_etags=multipart_etags
        )

    # -- session management -----------------------------------------------

    def status(self) -> StatusResult:
        """Current server-side state of this transfer."""
        if self._session_id is None:
            raise IngestStateError("no session yet — call begin() or run() first")
        return self._client.get_status(self._session_id)

    def estimate(self) -> EstimateResult:
        """Row count + size class of the staged data, between upload and commit."""
        if self._session_id is None:
            raise IngestStateError("no session yet — call begin() or run() first")
        return self._client.estimate(self._session_id)

    def cancel(self) -> None:
        """Abandon this transfer and delete its staged data.

        Does not drop a table an earlier successful commit already created.
        """
        if self._session_id is None:
            raise IngestStateError("no session to cancel")
        self._client.cancel(self._session_id)
        self._session_id = None

    def retry(self) -> IngestOutcome:
        """Re-run this transfer from the step that failed.

        Two cases, decided by the server's own report of where it broke:

        * **The load failed** (``CREATE TABLE`` / ``COPY INTO``) and the staged
          files were kept — the common case, e.g. Dremio was briefly unavailable
          or the target folder was fixed afterwards. Only that step re-runs; the
          upload is NOT repeated.
        * **Anything else** — the upload never finished, or the failure was one a
          re-run cannot fix (an unresolved collision, an unreadable file), so the
          staged copy is gone. A fresh session is opened and the folder is
          uploaded again. Any ``idempotency_key`` is dropped for that attempt, or
          the server would just replay the failed session.

        **A transfer whose files are stored permanently is never re-uploaded
        blindly.** There the upload landed in the table's own folder and stayed,
        so a fresh session would add a *second* copy — the server numbers an
        incoming name that already exists, precisely so an append can never
        overwrite live data, and that protection turns a silent re-run into
        duplicated rows. Such a transfer is resumable server-side by design, so
        the first branch handles it; if the server says it is not, this raises
        rather than guessing.

        Raises :class:`IngestStateError` if there is nothing to retry — no
        session, one that is still running, or one that already succeeded.
        """
        if self._session_id is None:
            raise IngestStateError("no session to retry — call run() first")
        status = self._client.get_status(self._session_id)
        if status.status == "done":
            raise IngestStateError(
                f"transfer {self._session_id} already succeeded; nothing to retry"
            )
        if status.status in ("pending", "uploading", "committing"):
            raise IngestStateError(
                f"transfer {self._session_id} is {status.status}; wait for it to "
                "finish before retrying"
            )
        if status.is_resumable:
            logger.info("resuming the load step of %s", self._session_id)
            resumed = self._client.retry(self._session_id)
            return IngestOutcome(
                begin=self._begin,
                commit=CommitResult.from_json(resumed.raw),
                files_uploaded=0,
                files_skipped=len(self._begin.s3.uploads) if self._begin else 0,
                resumed=True,
            )
        if status.stores_permanently:
            raise IngestStateError(
                f"transfer {self._session_id} stored its files permanently and "
                "cannot be resumed, so re-running it would upload a second copy "
                "alongside the first. Inspect the table, then re-ingest "
                "deliberately — with conflict_mode='replace' to redo it, or a "
                "sub_path for data that belongs beside what is already there."
            )
        # Nothing to resume server-side: start over with a new session, which
        # means dropping the idempotency key that would replay the failed one.
        logger.info(
            "transfer %s cannot be resumed (%s); re-uploading under a new session",
            self._session_id,
            status.raw.get("failed_stage") or status.status,
        )
        self._session_id = None
        self._begin = None
        self.idempotency_key = None
        return self.run()

    # -- upload phase -----------------------------------------------------

    def _already_done(self, session_id: str) -> set[str]:
        """Return rel_paths already uploaded for this session (resume support).

        Queries the session status and skips only the files the server
        explicitly names under ``raw["uploaded"]``, so resume never wrongly drops
        a file. NOTE: the server populates that list for **server-run** transfers
        (``POST /ingest/folder``); a client-uploaded session reports nothing
        there, so re-running one uploads every file again.

        Re-running the SAME session is harmless — the keys are identical, so the
        second upload overwrites the first. Starting a NEW session is a different
        matter where the files are stored permanently: the server numbers an
        incoming name that already exists, precisely so an append cannot
        overwrite live data, and that turns a re-run into a second copy rather
        than an overwrite. :meth:`retry` refuses exactly that case; prefer
        :meth:`FolderIngest.attach` + :meth:`retry` over re-running :meth:`run`.
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
    """Convenience wrapper: build a :class:`FolderIngest`, run it, close it.

    One-shot. If the transfer might need a :meth:`FolderIngest.retry`, drive the
    class directly (ideally as a context manager) so the session handle survives.
    """
    with FolderIngest(
        folder,
        target_catalog_path,
        data_format=data_format,
        **kwargs,  # type: ignore[arg-type]
    ) as job:
        return job.run()
