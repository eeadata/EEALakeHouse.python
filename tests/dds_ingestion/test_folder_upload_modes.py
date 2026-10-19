"""PY-1 (upload_mode + /stage fallback), PY-2 (cancel when an upload fails) and
PY-8 (retry after that cancel).

Companion to the DDS DI-12 plan; see docs/dds-ingest-robustness-client-plan.md.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

import httpx
import pytest
from eea_datalakehouse.dds_ingestion.client import S3UploadError
from eea_datalakehouse.dds_ingestion.folder import IngestStateError
from eea_datalakehouse.dds_ingestion.models import (
    BeginResult,
    FileSpec,
    StageResult,
    UploadTarget,
)
from eea_datalakehouse.dds_ingestion.session import IngestCommitError, IngestSession

from .test_folder import FakeClient, _make_ingest


class ModeClient(FakeClient):
    """FakeClient with a server ``upload_mode``, a failing presigned path, and
    the ``stage`` / ``cancel`` calls recorded."""

    def __init__(
        self,
        *,
        upload_mode: str = "direct",
        fail: dict[str, Exception] | None = None,
        cancel_error: Exception | None = None,
    ) -> None:
        super().__init__()
        self.upload_mode = upload_mode
        self.fail = fail or {}
        self.cancel_error = cancel_error
        self.attempted: list[str] = []
        self.staged: list[str] = []
        self.cancelled: list[str] = []

    def begin(self, *, files: list[FileSpec], **kwargs: object) -> BeginResult:
        return dataclasses.replace(
            super().begin(files=files, **kwargs), upload_mode=self.upload_mode
        )

    def upload_file(self, target: UploadTarget, data: bytes) -> str | None:
        self.attempted.append(target.rel_path)
        if target.rel_path in self.fail:
            raise self.fail[target.rel_path]
        return super().upload_file(target, data)

    def stage(self, session_id: str, rel_path: str, data: bytes) -> StageResult:
        self.staged.append(rel_path)
        return StageResult(rel_path=rel_path, key=f"p/{rel_path}", bytes_written=len(data))

    def cancel(self, session_id: str) -> None:
        self.cancelled.append(session_id)
        if self.cancel_error is not None:
            raise self.cancel_error


def _s3_error(rel: str) -> S3UploadError:
    return S3UploadError(403, "AccessDenied", rel_path=rel, url=f"https://s3.test/{rel}")


def _network_error() -> httpx.ConnectError:
    return httpx.ConnectError("connection refused")


# --- BeginResult.upload_mode ----------------------------------------------------


def test_upload_mode_is_read_from_begin_and_is_none_when_absent() -> None:
    base = {"session_id": "s", "status": "uploading", "s3": {"bucket": "b", "key_prefix": "p/"}}
    assert BeginResult.from_json(base).upload_mode is None
    assert BeginResult.from_json({**base, "upload_mode": "auto"}).upload_mode == "auto"


# --- PY-1: upload_mode -------------------------------------------------------------


def test_proxy_mode_sends_every_file_through_dds(data_folder: Path) -> None:
    client = ModeClient(upload_mode="proxy")
    outcome = _make_ingest(data_folder, client).run()

    assert sorted(client.staged) == ["a.parquet", "sub/b.parquet"]
    assert client.attempted == []
    assert outcome.commit.status == "committed"
    assert (outcome.upload_mode, outcome.files_staged) == ("proxy", 2)


def test_auto_mode_falls_back_to_dds_on_a_network_error(data_folder: Path) -> None:
    client = ModeClient(upload_mode="auto", fail={"a.parquet": _network_error()})
    outcome = _make_ingest(data_folder, client).run()

    assert client.staged == ["a.parquet"]
    assert "sub/b.parquet" in client.uploaded
    assert outcome.commit.status == "committed"
    assert client.cancelled == []
    # The fallback shows: one file of two went through DDS (PY-13).
    assert (outcome.upload_mode, outcome.files_uploaded, outcome.files_staged) == ("direct", 2, 1)


def test_a_plain_direct_upload_stages_nothing(data_folder: Path) -> None:
    outcome = _make_ingest(data_folder, ModeClient(upload_mode="auto")).run()
    assert outcome.files_staged == 0


def test_auto_mode_does_not_retry_an_s3_http_error_through_dds(data_folder: Path) -> None:
    # A rejected signature or permission is not fixed by sending the bytes via DDS.
    client = ModeClient(upload_mode="auto", fail={"a.parquet": _s3_error("a.parquet")})
    with pytest.raises(S3UploadError):
        _make_ingest(data_folder, client).run()
    assert client.staged == []


def test_direct_mode_never_falls_back(data_folder: Path) -> None:
    client = ModeClient(upload_mode="direct", fail={"a.parquet": _network_error()})
    with pytest.raises(httpx.ConnectError):
        _make_ingest(data_folder, client).run()
    assert client.staged == []


# --- PY-2: cancel when an upload fails --------------------------------------------


def test_a_failed_upload_cancels_the_session(data_folder: Path) -> None:
    client = ModeClient(fail={"a.parquet": _s3_error("a.parquet")})
    job = _make_ingest(data_folder, client)

    with pytest.raises(S3UploadError) as exc:
        job.run()

    assert client.cancelled == ["sess-X"]
    assert client.commit_calls == []
    assert job.session_id is None
    assert any("was cancelled" in note for note in exc.value.__notes__)


def test_a_failing_cancel_does_not_hide_the_upload_error(data_folder: Path) -> None:
    client = ModeClient(
        fail={"a.parquet": _s3_error("a.parquet")},
        cancel_error=RuntimeError("DDS unreachable"),
    )
    with pytest.raises(S3UploadError) as exc:
        _make_ingest(data_folder, client).run()

    assert client.cancelled == ["sess-X"]
    assert any("could not cancel" in note for note in exc.value.__notes__)


def test_queued_uploads_stop_after_the_first_failure(tmp_path: Path) -> None:
    for i in range(5):
        (tmp_path / f"part-{i}.parquet").write_bytes(b"PAR1")
    client = ModeClient(fail={"part-0.parquet": _s3_error("part-0.parquet")})
    job = _make_ingest(tmp_path, client, parallelism=1)

    with pytest.raises(S3UploadError):
        job.run()

    # One worker: it may already have picked up the next file when the failure
    # is seen, but every file still queued after that is cancelled, not uploaded.
    assert client.attempted[0] == "part-0.parquet"
    assert len(client.attempted) <= 2
    assert client.cancelled == ["sess-X"]


def test_a_successful_ingest_is_never_cancelled(data_folder: Path) -> None:
    client = ModeClient()
    _make_ingest(data_folder, client).run()
    assert client.cancelled == []


# --- PY-8: retry after a cancelled upload ----------------------------------------


class FlakyClient(ModeClient):
    """ModeClient whose failures happen once: the next attempt at that file works."""

    def upload_file(self, target: UploadTarget, data: bytes) -> str | None:
        error = self.fail.pop(target.rel_path, None)
        if error is not None:
            self.attempted.append(target.rel_path)
            raise error
        return super().upload_file(target, data)


def test_retry_uploads_again_after_a_network_error(data_folder: Path) -> None:
    client = FlakyClient(fail={"a.parquet": _network_error()})
    job = _make_ingest(data_folder, client)
    with pytest.raises(httpx.ConnectError):
        job.run()

    outcome = job.retry()

    assert outcome.commit.status == "committed"
    assert len(client.begin_calls) == 2  # a new session
    assert client.cancelled == ["sess-X"]  # only the first one


def test_retry_does_not_upload_again_what_would_fail_the_same_way(data_folder: Path) -> None:
    client = FlakyClient(fail={"a.parquet": _s3_error("a.parquet")})
    job = _make_ingest(data_folder, client)
    with pytest.raises(S3UploadError):
        job.run()

    with pytest.raises(IngestStateError, match="HTTP 403") as exc:
        job.retry()

    assert isinstance(exc.value.__cause__, S3UploadError)
    assert len(client.begin_calls) == 1


def test_retry_does_not_upload_again_when_the_cancel_failed(data_folder: Path) -> None:
    client = FlakyClient(
        fail={"a.parquet": _network_error()}, cancel_error=RuntimeError("DDS unreachable")
    )
    job = _make_ingest(data_folder, client)
    with pytest.raises(httpx.ConnectError):
        job.run()

    with pytest.raises(IngestStateError, match="could not be cancelled"):
        job.retry()

    assert len(client.begin_calls) == 1


def test_retry_before_any_run_still_says_there_is_no_session(data_folder: Path) -> None:
    with pytest.raises(IngestStateError, match="no session to retry"):
        _make_ingest(data_folder, ModeClient()).retry()


def test_ingest_session_retry_shows_the_upload_error(data_folder: Path) -> None:
    client = FlakyClient(fail={"a.parquet": _s3_error("a.parquet")})
    session = IngestSession(client=client)  # type: ignore[arg-type]
    session.ingest(data_folder, "bio.uploads", data_format="parquet", show_progress=False)

    with pytest.raises(IngestCommitError) as exc:
        session.commit(retry=True)

    assert "AccessDenied" in str(exc.value)
    assert "no session to retry" not in str(exc.value)


def test_ingest_session_retry_recovers_from_a_network_error(data_folder: Path) -> None:
    client = FlakyClient(fail={"a.parquet": _network_error()})
    session = IngestSession(client=client)  # type: ignore[arg-type]
    session.ingest(data_folder, "bio.uploads", data_format="parquet", show_progress=False)

    report = session.commit(retry=True)

    assert [o.commit.status for o in report.outcomes] == ["committed"]
