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
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa

from .client import IngestApiError, IngestClient, UnsupportedByServerError
from .credentials import DremioCreds, load_base_url, load_creds
from .formats import FORMATS, compare_columns, local_columns
from .models import (
    BeginPlan,
    BeginResult,
    ColumnDef,
    CommitResult,
    DataFormat,
    DryRunResult,
    EstimateResult,
    FileSpec,
    Intent,
    StatusResult,
    TableDefinition,
    UploadMode,
    UploadTarget,
    definition_payload,
)
from .progress import make_progress_bar

logger = logging.getLogger("eea_datalakehouse.dds_ingestion")

DEFAULT_PARALLELISM = 4


class IngestStateError(RuntimeError):
    """An operation was asked for in a state that cannot support it.

    Raised locally, before any HTTP call — e.g. committing a transfer that never
    began, or retrying one that is still running.
    """


@dataclass(frozen=True, slots=True)
class DiagnosisStep:
    """One check of :meth:`FolderIngest.diagnose`; ``ok`` is ``None`` when skipped."""

    name: str
    ok: bool | None
    detail: str


@dataclass(slots=True)
class Diagnosis:
    """What :meth:`FolderIngest.diagnose` found, one step per check, in order."""

    steps: list[DiagnosisStep] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Whether every check that ran passed."""
        return all(step.ok is not False for step in self.steps)


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
    upload_mode: str = "direct"  # how the files went: "direct" (presigned S3) or "proxy" (DDS)


def scan_folder(folder: Path, data_format: DataFormat) -> list[FileSpec]:
    """Recurse ``folder`` and return files matching ``data_format`` only.

    Other formats present in the folder are ignored (single-format scan). Paths
    are returned folder-relative with forward slashes so they are stable across
    platforms. Results are sorted for deterministic ordering.
    """

    exts = FORMATS[data_format].extensions
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
        table_definition: TableDefinition | Mapping[str, Any] | None = None,
        cleanup_on_failure: bool = False,
        csv_options: Mapping[str, Any] | None = None,
        json_options: Mapping[str, Any] | None = None,
        infer_schema: bool = True,
        show_progress: bool = True,
        upload_mode: UploadMode = "auto",
        client: IngestClient | None = None,
        base_url: str | None = None,
        creds: DremioCreds | None = None,
    ) -> None:
        if parallelism < 1:
            raise ValueError("parallelism must be >= 1")
        if upload_mode not in ("auto", "direct", "proxy"):
            raise ValueError(
                f"upload_mode must be 'auto', 'direct' or 'proxy', not {upload_mode!r}"
            )
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
        # Optional table definition (contract v0.8): column types for CSV/JSON
        # into an editable table, and room for what later servers apply. Sent on
        # begin; the begin plan says which parts this server uses.
        self.table_definition = table_definition
        # DI-12.26: after a commit failure the server could retry, drop the
        # session (and its uploaded files) instead of keeping it for retry().
        self.cleanup_on_failure = cleanup_on_failure
        # Parsing hints sent on begin (the server already accepts them).
        self.csv_options = csv_options
        self.json_options = json_options
        # PY-6: for CSV/JSON into an editable table without a table_definition,
        # infer the columns locally from every file and send them.
        self.infer_schema = infer_schema
        # The definition actually sent on begin (the caller's, or inferred); a
        # server that did not take it on begin gets it again on commit.
        self._sent_definition: TableDefinition | Mapping[str, Any] | None = None
        self.show_progress = show_progress
        self.upload_mode = upload_mode

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
        try:
            etags, multipart_etags, uploaded, skipped = self._upload_all(begin)
        except Exception as err:
            self._cancel_after_failure(err)
            raise
        try:
            commit = self.commit(multipart_etags=multipart_etags or None)
        except IngestApiError as err:
            self._after_failed_commit(err)
            raise
        return IngestOutcome(
            begin=begin,
            commit=commit,
            files_uploaded=uploaded,
            files_skipped=skipped,
            etags=etags,
            upload_mode="proxy" if self._uses_proxy(begin) else "direct",
        )

    def _uses_proxy(self, begin: BeginResult) -> bool:
        """Whether to send the files through DDS rather than to S3 directly:
        always with ``upload_mode="proxy"``, never with ``"direct"``, and with
        ``"auto"`` when ``begin`` says ``upload_mode: "proxy"``."""
        if self.upload_mode == "auto":
            return begin.upload_mode == "proxy"
        return self.upload_mode == "proxy"

    def _falls_back_to_proxy(self, begin: BeginResult) -> bool:
        """Whether a presigned upload that fails at the network layer is sent
        through DDS instead: only with ``upload_mode="auto"`` (follow the
        server) and when ``begin`` says ``upload_mode: "auto"``."""
        return self.upload_mode == "auto" and begin.upload_mode == "auto"

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
        self._sent_definition = self._definition_to_send(files)
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
            definition=self._sent_definition,
            csv_options=self.csv_options,
            json_options=self.json_options,
        )
        self._session_id = begin.session_id
        self._begin = begin
        if begin.plan is not None:
            self._report_plan(begin.plan)
        return begin

    def dry_run(self) -> DryRunResult:
        """Check this transfer without starting it (DI-12.3).

        Runs every check ``begin`` runs — privileges, the target folder, storage,
        collisions, the table definition — and returns (and shows) what commit
        would do. Nothing is uploaded and no session is created. Raises what a
        real begin would raise, or :class:`UnsupportedByServerError` against a
        DDS server that predates dry runs.
        """
        files = scan_folder(self.folder, self.data_format)
        if not files:
            raise FileNotFoundError(
                f"no {self.data_format} files found under {self.folder}"
            )
        result = self._client.dry_run(
            target_catalog_path=self.target_catalog_path,
            intent=self.intent,
            data_format=self.data_format,
            conflict_mode=self.conflict_mode,
            files=files,
            table_name=self.table_name,
            sub_path=self.sub_path,
            definition=self._definition_to_send(files),
            csv_options=self.csv_options,
            json_options=self.json_options,
        )
        self._report_plan(result.plan)
        return result

    def _definition_to_send(
        self, files: list[FileSpec]
    ) -> TableDefinition | Mapping[str, Any] | None:
        """The table definition begin should carry (PY-6).

        * The caller's own, if given — for Parquet, its columns are first checked
          against the local footers, so a mismatch fails here, before any upload,
          with the server's ``schema_mismatch`` wording.
        * Otherwise, for CSV/JSON into an **editable** table (the one case the
          server creates the table from the given columns), the columns inferred
          locally from every file, unless ``infer_schema=False``.
        * Otherwise none.
        """
        fmt = FORMATS[self.data_format]
        rel_paths = [f.rel_path for f in files]
        if self.table_definition is not None:
            given = (definition_payload(self.table_definition) or {}).get("columns") or []
            if fmt.has_footer and given:
                self._check_parquet_columns(rel_paths, given)
            return self.table_definition
        if fmt.has_footer or self.intent != "editable" or not self.infer_schema:
            return None
        options = self.csv_options if self.data_format == "csv" else self.json_options
        try:
            columns = local_columns(self.folder, rel_paths, self.data_format, options)
        except (ValueError, pa.ArrowException) as exc:
            logger.warning(
                "could not infer the %s columns locally (%s); the server will ask for "
                "them — pass table_definition=…",
                self.data_format,
                exc,
            )
            return None
        inferred = TableDefinition(columns=tuple(ColumnDef(n, t) for n, t in columns))
        summary = ", ".join(f"{n} {t}" for n, t in columns)
        logger.info("inferred %s columns: %s", self.data_format, summary)
        if self.show_progress:
            print(f"Inferred columns: {summary}")
        return inferred

    def _check_parquet_columns(self, rel_paths: list[str], given: list[Mapping[str, Any]]) -> None:
        """Refuse given Parquet columns that contradict the local footers, before
        anything is uploaded — the same check the server runs at commit (DI-12.1d)."""
        actual = local_columns(self.folder, rel_paths, "parquet", None)
        problems = compare_columns(
            [(str(c.get("name", "")), str(c.get("type", ""))) for c in given], actual
        )
        if problems:
            err = IngestApiError(
                422,
                "the given columns do not match the Parquet files: " + "; ".join(problems),
                where="local Parquet footer check (nothing was uploaded)",
            )
            err.error_slug = "schema_mismatch"
            err.retryable = False
            raise err

    def diagnose(self) -> Diagnosis:
        """Check, in plain language, whether this transfer would work — without
        starting it (PY-7). Nothing is uploaded and no session is created.

        In order: DDS reachable; the credential accepted; DDS's own S3 write test;
        and a dry run of this exact folder, which covers the privileges on the
        target, the target folder, collisions and the table definition. A check
        that cannot run (DDS unreachable, a server predating it) is reported as
        skipped, with the reason, instead of failing the ones after it.
        """
        result = Diagnosis()

        def add(name: str, ok: bool | None, detail: str) -> None:
            result.steps.append(DiagnosisStep(name, ok, detail))
            if self.show_progress:
                mark = {True: "✓", False: "✗", None: "–"}[ok]
                print(f"{mark} {name}: {detail}")

        try:
            self._client.health()
        except (httpx.HTTPError, IngestApiError) as exc:
            add("DDS reachable", False, str(exc))
            for name in ("Credential accepted", "Storage write test", "Dry run of this folder"):
                add(name, None, "skipped: DDS is not reachable")
            return result
        add("DDS reachable", True, "the service answers")

        try:
            self._client.limits("notebook")
            add("Credential accepted", True, "DDS accepts your Dremio credential")
        except IngestApiError as exc:
            if exc.status_code == 404:  # a server predating GET /ingest/limits
                try:
                    self._client.list_sessions()
                    add("Credential accepted", True, "DDS accepts your Dremio credential")
                except IngestApiError as inner:
                    add("Credential accepted", False, inner.message)
            else:
                add("Credential accepted", False, exc.message)

        try:
            verdict = self._client.storage_selftest().get("storage_write_test") or {}
            if verdict.get("ok"):
                add("Storage write test", True, "DDS can write to its S3 storage")
            elif verdict:
                failures = "; ".join(str(f) for f in verdict.get("failures") or []) or "failed"
                add("Storage write test", False, failures)
            else:
                add("Storage write test", None, "skipped: no verdict reported yet")
        except IngestApiError as exc:
            add("Storage write test", None, f"skipped: {exc.message}")

        try:
            dry = self.dry_run()
            add("Dry run of this folder", True, dry.plan.describe())
        except UnsupportedByServerError:
            add("Dry run of this folder", None, "skipped: this DDS server has no dry run")
        except (IngestApiError, FileNotFoundError, ValueError) as exc:
            add("Dry run of this folder", False, str(getattr(exc, "message", exc)))
        return result

    def _report_plan(self, plan: BeginPlan) -> None:
        """Show what commit will do: printed when ``show_progress`` (the same
        switch as the progress bar), always logged; the parts of the table
        definition this server will not apply are a warning, never silent."""
        logger.info("ingest plan: %s", plan.describe())
        if self.show_progress:
            print(plan.describe())
        for line in plan.definition_ignored:
            logger.warning("table definition not applied — %s", line)

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
        # A server that returned a plan took the definition on begin; an older
        # one (or an attached session, whose begin we did not see) gets it here.
        resend = self._begin is None or self._begin.plan is None
        sent = self._sent_definition if self._begin is not None else self.table_definition
        return self._client.commit(
            session_id=self._session_id,
            multipart_etags=multipart_etags,
            definition=sent if resend else None,
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

    def _after_failed_commit(self, err: IngestApiError) -> None:
        """Describe a failed commit the way the server records it, and apply
        ``cleanup_on_failure``. Best-effort: nothing here replaces ``err``.

        The note is the session's own failure text (:meth:`StatusResult.
        describe_failure`), the same the Transfers panel shows. A failure the
        server cannot retry was already cleaned up server-side; a retryable one
        is kept for :meth:`retry` unless ``cleanup_on_failure`` is set.
        """
        session_id = self._session_id
        if session_id is None:
            return
        try:
            err.add_note(self._client.get_status(session_id).describe_failure())
        except Exception:  # noqa: BLE001 — the commit error is what matters
            logger.debug("could not fetch the failed session's status", exc_info=True)
        if self.cleanup_on_failure and err.retryable is True:
            self._cancel_after_failure(err)

    def _cancel_after_failure(self, err: Exception) -> None:
        """Cancel the session after a failure, and say so on ``err``.

        After a failed upload (PY-2): an incomplete client upload cannot be
        resumed — the server only re-runs the load (:meth:`retry`) — and the
        session would stay ``uploading`` until its TTL. After a failed commit,
        only with ``cleanup_on_failure`` (see :meth:`_after_failed_commit`).
        Cancelling deletes whatever did land — for a read-only ingest those files
        sit in the table's own folder — instead of leaving it for the server's
        sweep. Best-effort: a failing cancel is noted, not raised, so the
        original error stays the one the caller sees.
        """
        session_id = self._session_id
        if session_id is None:
            return
        try:
            self.cancel()
        except Exception as cancel_err:  # noqa: BLE001 — reported on err instead
            logger.warning("could not cancel %s after a failed upload: %s", session_id, cancel_err)
            err.add_note(
                f"DDS could not cancel session {session_id} ({cancel_err}); the "
                "server's sweep removes its uploaded files after the session's TTL."
            )
        else:
            err.add_note(
                f"Session {session_id} was cancelled and the files it had uploaded "
                "were deleted; run the ingest again."
            )

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
        proxy = self._uses_proxy(begin)
        fallback = self._falls_back_to_proxy(begin)
        if proxy:
            logger.info("uploading through DDS (proxy), not to the presigned S3 targets")
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
                futures = {
                    pool.submit(self._upload_one, t, begin.session_id, proxy, fallback): t
                    for t in pending
                }
                try:
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
                except BaseException:
                    # Stop at the first failure: leaving the ``with`` block would
                    # otherwise still run every queued upload before re-raising.
                    for queued in futures:
                        queued.cancel()
                    raise
        finally:
            if bar is not None:
                bar.close()

        return etags, multipart_etags, len(pending), skipped

    def _upload_one(
        self,
        target: UploadTarget,
        session_id: str,
        proxy: bool = False,
        fallback: bool = False,
    ) -> tuple[str | None, list[dict[str, object]] | None]:
        """Upload one file; return ``(single_etag, multipart_parts)``.

        At most one element is non-``None``: a single-shot upload yields the
        ETag (or ``None`` if S3 omitted it); a multipart upload yields the
        per-part ETag list and the single ETag is ``None``. A proxied upload
        (`proxy`) sends the whole file through DDS — DDS writes it to S3 with
        its own connection, so this process never contacts the storage — and
        yields neither: there are no S3 ETags to hand to commit.

        With `fallback` (see `_falls_back_to_proxy`), a presigned upload that
        fails at the **network** layer (unreachable endpoint, DNS, TLS,
        timeout) is sent through DDS instead — the contract upload-mode rule
        the JupyterLab extension implements. An HTTP error from S3 is not
        retried through DDS: that is a signing or permission problem the proxy
        would not fix.
        """
        data = (self.folder / target.rel_path).read_bytes()
        if proxy:
            self._client.stage(session_id, target.rel_path, data)
            return None, None
        try:
            if target.is_multipart:
                parts = self._client.upload_file_multipart(target, data)
                return None, [dict(p) for p in parts]
            return self._client.upload_file(target, data), None
        except httpx.TransportError as exc:
            if not fallback:
                raise
            logger.warning(
                "presigned upload of %s failed at the network layer (%s); "
                "sending it through DDS instead",
                target.rel_path,
                exc,
            )
            return self._stage(session_id, target, data)

    def _stage(
        self, session_id: str, target: UploadTarget, data: bytes
    ) -> tuple[None, None]:
        """Send one file through DDS (``POST /ingest/stage``), which writes it to
        S3 with its own credentials. A file the server planned as multipart goes
        through whole; DDS accepts that at commit (DI-12.10a)."""
        self._client.stage(session_id, target.rel_path, data)
        return None, None


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
