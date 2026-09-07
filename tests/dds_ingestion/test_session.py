"""Session management on :class:`FolderIngest` — status, retry, cancel, attach.

The important behaviour is :meth:`FolderIngest.retry`, which resumes at the step
that failed. The server tells it which step that was; these tests pin both
branches:

* the load failed and the staged files were kept → re-run ONLY the load, no
  re-upload;
* anything else → the staged copy is gone, so upload again under a NEW session
  (and drop the idempotency key, or the server would replay the failed one).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from eea_datalakehouse.dds_ingestion.folder import FolderIngest, IngestStateError
from eea_datalakehouse.dds_ingestion.models import (
    BeginResult,
    CommitResult,
    S3Plan,
    StatusResult,
    UploadTarget,
)


class SessionClient:
    """Fake IngestClient that serves a scripted sequence of session states."""

    def __init__(self, *statuses: dict[str, Any]) -> None:
        self._statuses = list(statuses)
        self.begin_calls: list[dict[str, Any]] = []
        self.commit_calls: list[str] = []
        self.retry_calls: list[str] = []
        self.cancel_calls: list[str] = []
        self.uploaded: list[str] = []
        self.closed = False
        self._next_session = 0

    # -- endpoints used by FolderIngest --
    def begin(self, *, files: list[Any], **kwargs: Any) -> BeginResult:
        self.begin_calls.append({"files": files, **kwargs})
        self._next_session += 1
        return BeginResult(
            session_id=f"sess-{self._next_session}",
            status="uploading",
            s3=S3Plan(
                bucket="b",
                key_prefix="p/",
                uploads=tuple(
                    UploadTarget(rel_path=f.rel_path, url=f"https://s3.test/{f.rel_path}")
                    for f in files
                ),
            ),
        )

    def get_status(self, session_id: str) -> StatusResult:
        payload = self._statuses.pop(0) if self._statuses else {"status": "done"}
        return StatusResult.from_json({"session_id": session_id, **payload})

    def upload_file(self, target: UploadTarget, data: bytes) -> str | None:
        self.uploaded.append(target.rel_path)
        return f'"etag-{target.rel_path}"'

    def commit(self, *, session_id: str, **kwargs: Any) -> CommitResult:
        self.commit_calls.append(session_id)
        return CommitResult(
            session_id=session_id, status="done", table_path="t", record_count=2
        )

    def retry(self, session_id: str) -> StatusResult:
        self.retry_calls.append(session_id)
        return StatusResult.from_json(
            {
                "session_id": session_id,
                "status": "done",
                "table_path": "bio.uploads.t",
                "record_count": 7,
            }
        )

    def cancel(self, session_id: str) -> None:
        self.cancel_calls.append(session_id)

    def close(self) -> None:
        self.closed = True


def _job(folder: Path, client: SessionClient, **kwargs: Any) -> FolderIngest:
    return FolderIngest(
        folder,
        "bio.uploads",
        data_format="parquet",
        show_progress=False,
        client=client,  # type: ignore[arg-type]
        **kwargs,
    )


# --- the session handle ------------------------------------------------------


def test_session_id_is_exposed_after_begin(data_folder: Path) -> None:
    client = SessionClient()
    job = _job(data_folder, client)
    assert job.session_id is None
    job.begin()
    assert job.session_id == "sess-1"


def test_attach_binds_to_an_existing_session(data_folder: Path) -> None:
    client = SessionClient({"status": "failed", "failed_stage": "load"})
    job = FolderIngest.attach(
        "sess-earlier",
        data_folder,
        "bio.uploads",
        data_format="parquet",
        show_progress=False,
        client=client,
    )
    assert job.session_id == "sess-earlier"
    assert job.status().status == "failed"


def test_commit_before_begin_is_a_state_error(data_folder: Path) -> None:
    job = _job(data_folder, SessionClient())
    with pytest.raises(IngestStateError):
        job.commit()


def test_cancel_discards_the_session(data_folder: Path) -> None:
    client = SessionClient()
    job = _job(data_folder, client)
    job.begin()
    job.cancel()
    assert client.cancel_calls == ["sess-1"]
    assert job.session_id is None


# --- retry -------------------------------------------------------------------


def test_retry_resumes_the_load_without_re_uploading(data_folder: Path) -> None:
    # The stranded-transfer case: the upload landed, Dremio failed, the server
    # kept the staged files. Only the load re-runs.
    client = SessionClient(
        {
            "status": "failed",
            "failed_stage": "load",
            "resumable": True,
            "error": "Dremio load failed: boom",
        }
    )
    job = _job(data_folder, client)
    job.begin()
    client.uploaded.clear()

    outcome = job.retry()

    assert client.retry_calls == ["sess-1"]
    assert client.uploaded == []          # nothing re-uploaded
    assert client.begin_calls == [client.begin_calls[0]]  # no new session
    assert outcome.resumed is True
    assert outcome.commit.record_count == 7


def test_retry_re_uploads_a_load_failure_that_kept_nothing(data_folder: Path) -> None:
    # A load-stage failure the server could NOT hold the bytes for still reports
    # failed_stage="load". Inferring resumability from that sent retry down the
    # resume path to be told the staged data was gone; the server's own verdict
    # sends it to re-upload instead.
    client = SessionClient(
        {
            "status": "failed",
            "failed_stage": "load",
            "resumable": False,
            "error": "Dremio load failed: 'x.parquet' could not be read as Parquet",
        }
    )
    job = _job(data_folder, client)
    job.begin()
    client.uploaded.clear()

    outcome = job.retry()

    assert client.retry_calls == []                     # server retry not attempted
    assert len(client.begin_calls) == 2                 # a second session
    assert sorted(client.uploaded) == ["a.parquet", "sub/b.parquet"]
    assert outcome.resumed is False


def test_retry_after_an_upload_failure_starts_a_fresh_session(data_folder: Path) -> None:
    # The staged files were cleaned up, so there is nothing to resume: upload
    # again under a new session.
    client = SessionClient(
        {"status": "failed", "failed_stage": "upload", "error": "S3 upload failed"}
    )
    job = _job(data_folder, client)
    job.begin()
    client.uploaded.clear()

    outcome = job.retry()

    assert client.retry_calls == []                     # server retry not attempted
    assert len(client.begin_calls) == 2                 # a second session
    assert sorted(client.uploaded) == ["a.parquet", "sub/b.parquet"]
    assert outcome.resumed is False
    assert job.session_id == "sess-2"


def test_retry_drops_the_idempotency_key_when_re_uploading(data_folder: Path) -> None:
    # Reusing the key would make the server replay the SAME failed session, which
    # is exactly the loop that made a retry look like it did nothing.
    client = SessionClient({"status": "failed", "failed_stage": "upload"})
    job = _job(data_folder, client, idempotency_key="key-1")
    job.begin()
    assert client.begin_calls[0]["idempotency_key"] == "key-1"

    job.retry()

    assert client.begin_calls[1]["idempotency_key"] is None


@pytest.mark.parametrize("state", ["pending", "uploading", "committing"])
def test_retry_refuses_while_still_running(data_folder: Path, state: str) -> None:
    client = SessionClient({"status": state})
    job = _job(data_folder, client)
    job.begin()
    with pytest.raises(IngestStateError, match=state):
        job.retry()


def test_retry_refuses_a_finished_transfer(data_folder: Path) -> None:
    client = SessionClient({"status": "done"})
    job = _job(data_folder, client)
    job.begin()
    with pytest.raises(IngestStateError, match="already succeeded"):
        job.retry()


def test_retry_without_a_session_is_a_state_error(data_folder: Path) -> None:
    job = _job(data_folder, SessionClient())
    with pytest.raises(IngestStateError):
        job.retry()


# --- status flags ------------------------------------------------------------


def test_is_resumable_follows_the_servers_verdict() -> None:
    kept = StatusResult.from_json(
        {"status": "failed", "failed_stage": "load", "resumable": True}
    )
    # Same stage, but the server cleaned up: NOT resumable. Deriving this from
    # failed_stage alone is what made retry ask to resume data that was gone.
    cleaned = StatusResult.from_json(
        {"status": "failed", "failed_stage": "load", "resumable": False}
    )
    swept = StatusResult.from_json({"status": "failed", "failed_stage": "upload"})
    running = StatusResult.from_json({"status": "committing", "resumable": True})
    # A server too old to send the field reads as not resumable — the safe way to
    # be wrong, since the transfer is simply re-uploaded.
    silent = StatusResult.from_json({"status": "failed", "failed_stage": "load"})
    assert kept.is_resumable is True
    assert cleaned.is_resumable is False
    assert swept.is_resumable is False
    assert running.is_resumable is False
    assert silent.is_resumable is False


def test_is_terminal_covers_every_end_state() -> None:
    for state in ("done", "failed", "cancelled"):
        assert StatusResult.from_json({"status": state}).is_terminal is True
    assert StatusResult.from_json({"status": "uploading"}).is_terminal is False
