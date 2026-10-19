"""`IngestSession` — queue folder ingests, then run them as one batch.

Prototype of the design in `docs/notebook-facade-for-data-scientists.md`
("Queue calls, then commit" / "Two sessions, not one")::

    session = IngestSession()
    session.ingest(folder="./bw_2026", target_catalog_path="bwd.reference",
                    data_format="parquet", table_name="water_temperature")
    session.commit(retry=True)

Deliberately a **separate** object from `eea_datalakehouse.catalog.session.
CatalogSession`, not a shared queue: a catalog operation can't run before its
target has actually been ingested, so the two were never one atomic batch —
run an `IngestSession.commit()` to completion first, then build a
`CatalogSession` for whatever comes after.

**No rollback.** Unlike `CatalogSession`, a `commit()` here cannot undo an
ingest that already succeeded — once a folder's files are uploaded and
loaded into a table, removing them needs a delete-that-also-removes-the-
backing-data operation this package does not have yet (see
`docs/read-only-ingest-client-plan.md`, "Noted for later: deleting a
read-only table"). So a partial failure is *reported*, not undone:
`IngestCommitError.succeeded` lists every ingest that already landed and
stays landed — check it before deciding what to do next.

This "session" is unrelated to `FolderIngest`'s own transfer session
(`session_id`, `attach`, `retry`) — that's a server-side handle for one
ingest; this is a client-side queue of possibly many.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .client import IngestClient
from .credentials import DremioCreds
from .folder import DEFAULT_PARALLELISM, FolderIngest, IngestOutcome, IngestStateError
from .models import DataFormat, Intent, TableDefinition, UploadMode


class IngestSessionError(RuntimeError):
    """Base for `IngestSession`-specific errors."""


class IngestCommitError(IngestSessionError):
    """`commit()` failed partway through the batch.

    `succeeded` holds the `IngestOutcome` of every ingest that already
    completed before the failure — those cannot be undone (see the module
    docstring) and stay exactly as ingested.
    """

    def __init__(
        self,
        message: str,
        *,
        failed_index: int,
        original_error: Exception,
        succeeded: list[IngestOutcome],
    ) -> None:
        super().__init__(message)
        self.failed_index = failed_index
        self.original_error = original_error
        self.succeeded = succeeded


@dataclass
class IngestCommitReport:
    """What a successful `commit()` actually did, in order."""

    outcomes: list[IngestOutcome] = field(default_factory=list)


@dataclass
class _QueuedIngest:
    folder: str | Path
    target_catalog_path: str
    data_format: DataFormat
    kwargs: dict[str, Any]

    def describe(self) -> str:
        return f"ingest {self.folder!r} -> {self.target_catalog_path!r}"


class IngestSession:
    """Queue `FolderIngest` runs, then `commit()` them as one batch.

    `ingest(...)` takes exactly `FolderIngest`'s own constructor arguments
    (`folder`, `target_catalog_path`, `data_format`, plus anything else it
    accepts — `intent`, `table_name`, `sub_path`, ...) and only records the
    intent; nothing runs until `commit()`. `client`/`base_url`/`creds`,
    if given here, are shared across every queued ingest exactly like
    passing them to `FolderIngest` directly (kernel-environment defaults
    otherwise — see `credentials.load_base_url`/`load_creds`).
    """

    def __init__(
        self,
        *,
        client: IngestClient | None = None,
        base_url: str | None = None,
        creds: DremioCreds | None = None,
    ) -> None:
        self._client = client
        self._base_url = base_url
        self._creds = creds
        self._pending: list[_QueuedIngest] = []

    def __repr__(self) -> str:
        return f"IngestSession(pending={len(self._pending)})"

    def ingest(
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
        multipart: bool | None = None,
        show_progress: bool = True,
        table_definition: TableDefinition | Mapping[str, Any] | None = None,
        cleanup_on_failure: bool = False,
        csv_options: Mapping[str, Any] | None = None,
        json_options: Mapping[str, Any] | None = None,
        upload_mode: UploadMode = "auto",
    ) -> IngestSession:
        """Queue one folder ingest — see `FolderIngest` for what each argument
        means. No `idempotency_key` here: unlike the catalog side, a queued
        ingest's own key would only ever be used once per `commit()`, so
        there is nothing to encapsulate — `FolderIngest` already defaults
        it to `None` (a fresh session every run).

        Each of the last five still fits on the one `%ingest` line that queues
        the ingest::

            upload_mode="proxy"                   # S3 is unreachable from this kernel
            csv_options={"delimiter": ";"}        # CSV parsing hints
            json_options={"format": "array"}      # a JSON array rather than JSON lines
            table_definition={"columns": [{"name": "value", "type": "DOUBLE"}]}
            cleanup_on_failure=True               # a failed commit leaves nothing behind

        With `cleanup_on_failure=True`, `commit(retry=True)` uploads a cleaned-up
        ingest again under a new session (see `FolderIngest.retry`).
        `FolderIngest`'s `infer_schema` is left out on purpose: turning
        inference off only makes DDS ask for the columns, which
        `table_definition` gives anyway."""
        self._pending.append(
            _QueuedIngest(
                folder=folder,
                target_catalog_path=target_catalog_path,
                data_format=data_format,
                kwargs={
                    "intent": intent,
                    "conflict_mode": conflict_mode,
                    "table_name": table_name,
                    "sub_path": sub_path,
                    "parallelism": parallelism,
                    "multipart": multipart,
                    "show_progress": show_progress,
                    "table_definition": table_definition,
                    "cleanup_on_failure": cleanup_on_failure,
                    "csv_options": csv_options,
                    "json_options": json_options,
                    "upload_mode": upload_mode,
                },
            )
        )
        return self

    def diagnose(self) -> None:
        """Check every queued ingest without running it — `FolderIngest.diagnose`
        for each, one printed line per check: DDS reachable, the credential, the
        file sizes, DDS's storage, and a dry run of that folder into its target.
        Nothing is uploaded and the queue is kept, so a `commit()` afterwards
        runs exactly these ingests."""
        if not self._pending:
            print("nothing queued — queue an ingest(...) first, then diagnose")
            return
        for item in self._pending:
            print(f"{item.describe()}:")
            try:
                job = FolderIngest(
                    item.folder,
                    item.target_catalog_path,
                    data_format=item.data_format,
                    client=self._client,
                    base_url=self._base_url,
                    creds=self._creds,
                    **{**item.kwargs, "show_progress": True},  # the lines are the point
                )
            except (OSError, ValueError, RuntimeError) as exc:
                # A missing folder or credential is itself the finding.
                print(f"✗ cannot check it: {exc}")
                continue
            try:
                job.diagnose()
            finally:
                job.close()

    def commit(self, *, retry: bool = False, max_retries: int = 3) -> IngestCommitReport:
        """Run every queued ingest, in order. Stops at the first failure —
        see the module docstring: nothing already succeeded can be rolled
        back, so `IngestCommitError.succeeded` is the record of what to deal
        with by hand.

        `retry=True` calls `FolderIngest.retry()` (resume-the-load-step or
        re-upload-under-a-new-session, whichever the server says applies —
        see that method's own docstring) up to `max_retries` times before
        giving up on a failed ingest.

        The queue is cleared either way.
        """
        succeeded: list[IngestOutcome] = []
        pending, self._pending = self._pending, []
        for index, item in enumerate(pending):
            job = FolderIngest(
                item.folder,
                item.target_catalog_path,
                data_format=item.data_format,
                client=self._client,
                base_url=self._base_url,
                creds=self._creds,
                **item.kwargs,
            )
            try:
                outcome = self._run_with_retry(job, retry=retry, max_retries=max_retries)
            except Exception as exc:
                raise IngestCommitError(
                    f"{item.describe()} failed ({exc}); {len(succeeded)} earlier ingest(s) in "
                    "this batch already committed and CANNOT be undone",
                    failed_index=index,
                    original_error=exc,
                    succeeded=succeeded,
                ) from exc
            finally:
                job.close()
            succeeded.append(outcome)
        return IngestCommitReport(outcomes=succeeded)

    @staticmethod
    def _run_with_retry(job: FolderIngest, *, retry: bool, max_retries: int) -> IngestOutcome:
        try:
            return job.run()
        except Exception as exc:
            if not retry:
                raise
            last_error: Exception = exc
            for _ in range(max_retries):
                try:
                    return job.retry()
                except IngestStateError:
                    raise  # retry() itself says this isn't resumable — retrying again won't help
                except Exception as retry_exc:  # noqa: BLE001 — keep trying up to max_retries
                    last_error = retry_exc
            raise last_error from exc
