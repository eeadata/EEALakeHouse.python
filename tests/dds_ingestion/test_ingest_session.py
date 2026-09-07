"""IngestSession: queue folder ingests, run them as one batch, no rollback.

See src/eea_datalakehouse/dds_ingestion/session.py and
docs/notebook-facade-for-data-scientists.md ("Two sessions, not one").
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from eea_datalakehouse.dds_ingestion.models import (
    BeginResult,
    CommitResult,
    S3Plan,
    StatusResult,
    UploadTarget,
)
from eea_datalakehouse.dds_ingestion.session import IngestCommitError, IngestSession


def _make_folder(base: Path, name: str) -> Path:
    folder = base / name
    folder.mkdir()
    (folder / "a.parquet").write_bytes(b"PAR1-a")
    return folder


class _StubClient:
    """Fake IngestClient shared across every queued ingest in a test."""

    def __init__(self) -> None:
        self.begin_calls = 0
        self.commit_calls: list[str] = []
        self.retry_calls: list[str] = []
        self.closed = False
        self._next_session = 0

    def begin(self, *, files: list[Any], **kwargs: Any) -> BeginResult:
        self.begin_calls += 1
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

    def upload_file(self, target: UploadTarget, data: bytes) -> str | None:
        return f'"etag-{target.rel_path}"'

    def commit(self, *, session_id: str, **kwargs: Any) -> CommitResult:
        self.commit_calls.append(session_id)
        return CommitResult(session_id=session_id, status="done", table_path="t", record_count=1)

    def close(self) -> None:
        self.closed = True


def test_nothing_runs_until_commit(tmp_path: Path) -> None:
    client = _StubClient()
    session = IngestSession(client=client)  # type: ignore[arg-type]

    session.ingest(
        _make_folder(tmp_path, "a"), "bio.uploads", data_format="parquet", show_progress=False
    )

    assert client.begin_calls == 0
    assert repr(session) == "IngestSession(pending=1)"


def test_commit_runs_every_queued_ingest_and_clears_the_queue(tmp_path: Path) -> None:
    client = _StubClient()
    session = IngestSession(client=client)  # type: ignore[arg-type]
    session.ingest(
        _make_folder(tmp_path, "a"), "bio.uploads.a", data_format="parquet", show_progress=False
    )
    session.ingest(
        _make_folder(tmp_path, "b"), "bio.uploads.b", data_format="parquet", show_progress=False
    )

    report = session.commit()

    assert len(report.outcomes) == 2
    assert client.begin_calls == 2
    assert client.commit_calls == ["sess-1", "sess-2"]
    assert repr(session) == "IngestSession(pending=0)"


def test_commit_stops_at_the_first_failure_and_reports_what_already_landed(tmp_path: Path) -> None:
    class _FailsOnSecondBegin(_StubClient):
        def begin(self, *, files: list[Any], **kwargs: Any) -> BeginResult:
            if self.begin_calls == 1:
                raise RuntimeError("target catalog folder does not exist")
            return super().begin(files=files, **kwargs)

    client = _FailsOnSecondBegin()
    session = IngestSession(client=client)  # type: ignore[arg-type]
    session.ingest(
        _make_folder(tmp_path, "a"), "bio.uploads.a", data_format="parquet", show_progress=False
    )
    session.ingest(
        _make_folder(tmp_path, "b"), "bio.uploads.b", data_format="parquet", show_progress=False
    )

    with pytest.raises(IngestCommitError) as exc_info:
        session.commit()

    error = exc_info.value
    assert error.failed_index == 1
    assert len(error.succeeded) == 1
    assert error.succeeded[0].commit.table_path == "t"
    assert "1 earlier ingest(s)" in str(error)
    assert repr(session) == "IngestSession(pending=0)"  # queue cleared even on failure


def test_retry_resumes_a_failed_commit_before_giving_up(tmp_path: Path) -> None:
    class _FlakyClient(_StubClient):
        def __init__(self) -> None:
            super().__init__()
            self._commit_attempts = 0

        def commit(self, *, session_id: str, **kwargs: Any) -> CommitResult:
            self._commit_attempts += 1
            if self._commit_attempts == 1:
                raise RuntimeError("Dremio load failed: engine was restarting")
            return super().commit(session_id=session_id, **kwargs)

        def get_status(self, session_id: str) -> StatusResult:
            return StatusResult.from_json(
                {
                    "session_id": session_id,
                    "status": "failed",
                    "failed_stage": "load",
                    "resumable": True,
                }
            )

        def retry(self, session_id: str) -> StatusResult:
            self.retry_calls.append(session_id)
            return StatusResult.from_json(
                {
                    "session_id": session_id,
                    "status": "done",
                    "table_path": "bio.uploads.t",
                    "record_count": 3,
                }
            )

    client = _FlakyClient()
    session = IngestSession(client=client)  # type: ignore[arg-type]
    session.ingest(
        _make_folder(tmp_path, "a"), "bio.uploads", data_format="parquet", show_progress=False
    )

    report = session.commit(retry=True)

    assert len(report.outcomes) == 1
    assert report.outcomes[0].resumed is True
    assert client.retry_calls == ["sess-1"]
