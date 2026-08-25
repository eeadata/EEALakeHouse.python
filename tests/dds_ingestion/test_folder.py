from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

import pytest
from eea_datalakehouse.dds_ingestion.folder import (
    FolderIngest,
    IngestStateError,
    scan_folder,
)
from eea_datalakehouse.dds_ingestion.models import (
    BeginResult,
    CommitResult,
    DataFormat,
    FileSpec,
    S3Plan,
    StatusResult,
    UploadPart,
    UploadTarget,
)


class FakeClient:
    """In-memory stand-in for IngestClient that records calls."""

    def __init__(self, *, already_uploaded: list[str] | None = None) -> None:
        self.begin_calls: list[dict[str, object]] = []
        self.uploaded: list[str] = []
        self.commit_calls: list[dict[str, object]] = []
        self.closed = False
        self._already_uploaded = already_uploaded or []
        # Set to drive ``get_status`` from a test (e.g. a failed, unresumable
        # session); ``None`` keeps the default "open" shape.
        self.status_payload: dict[str, object] | None = None
        # concurrency instrumentation
        self._lock = threading.Lock()
        self._active = 0
        self.max_concurrent = 0

    def begin(self, *, files: list[FileSpec], **kwargs: object) -> BeginResult:
        self.begin_calls.append({"files": files, **kwargs})
        uploads = tuple(
            UploadTarget(rel_path=f.rel_path, url=f"https://s3.test/{f.rel_path}")
            for f in files
        )
        return BeginResult(
            session_id="sess-X",
            status="open",
            s3=S3Plan(bucket="b", key_prefix="p/", uploads=uploads),
        )

    def get_status(self, session_id: str) -> StatusResult:
        if self.status_payload is not None:
            return StatusResult.from_json(self.status_payload)
        return StatusResult.from_json(
            {
                "status": "open",
                "progress": {"files_done": len(self._already_uploaded), "files_total": 0},
                "uploaded": self._already_uploaded,
            }
        )

    def upload_file(self, target: UploadTarget, data: bytes) -> str | None:
        with self._lock:
            self._active += 1
            self.max_concurrent = max(self.max_concurrent, self._active)
        time.sleep(0.02)  # widen the window so concurrency is observable
        with self._lock:
            self._active -= 1
            self.uploaded.append(target.rel_path)
        return f'"etag-{target.rel_path}"'

    def commit(self, *, session_id: str, **kwargs: object) -> CommitResult:
        self.commit_calls.append({"session_id": session_id, **kwargs})
        return CommitResult(
            session_id=session_id,
            status="committed",
            table_path="bio.uploads.t",
            record_count=2,
        )

    def close(self) -> None:
        self.closed = True


def _make_ingest(folder: Path, client: FakeClient, **kwargs: object) -> FolderIngest:
    return FolderIngest(
        folder,
        "bio.uploads",
        data_format="parquet",
        show_progress=False,
        client=client,  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )


def test_scan_keeps_single_format_only(data_folder: Path) -> None:
    specs = scan_folder(data_folder, "parquet")
    paths = sorted(s.rel_path for s in specs)
    assert paths == ["a.parquet", "sub/b.parquet"]
    # csv/json/txt noise is ignored
    assert all(p.endswith(".parquet") for p in paths)


@pytest.mark.parametrize("fmt", ["csv", "json"])
def test_scan_other_formats(data_folder: Path, fmt: DataFormat) -> None:
    specs = scan_folder(data_folder, fmt)
    assert len(specs) == 1


def test_happy_path_begin_upload_commit(data_folder: Path) -> None:
    client = FakeClient()
    outcome = _make_ingest(data_folder, client).run()

    assert len(client.begin_calls) == 1
    assert sorted(client.uploaded) == ["a.parquet", "sub/b.parquet"]
    assert len(client.commit_calls) == 1
    assert outcome.commit.table_path == "bio.uploads.t"
    assert outcome.files_uploaded == 2
    assert outcome.files_skipped == 0
    # An *injected* client is caller-owned, so FolderIngest must NOT close it.
    assert client.closed is False


def test_parallel_uploads_run_concurrently(tmp_path: Path) -> None:
    for i in range(8):
        (tmp_path / f"f{i}.parquet").write_bytes(b"x")
    client = FakeClient()
    _make_ingest(tmp_path, client, parallelism=4).run()

    assert len(client.uploaded) == 8
    # with 8 files and 4 workers we must observe more than one concurrent upload
    assert client.max_concurrent > 1
    assert client.max_concurrent <= 4


def test_resume_skips_already_uploaded(data_folder: Path) -> None:
    client = FakeClient(already_uploaded=["a.parquet"])
    outcome = _make_ingest(data_folder, client).run()

    # only the not-yet-done file is uploaded
    assert client.uploaded == ["sub/b.parquet"]
    assert outcome.files_uploaded == 1
    assert outcome.files_skipped == 1


def test_no_matching_files_raises(tmp_path: Path) -> None:
    (tmp_path / "only.csv").write_text("a\n")
    client = FakeClient()
    with pytest.raises(FileNotFoundError):
        _make_ingest(tmp_path, client).run()


def test_credentials_never_logged(
    data_folder: Path, caplog: pytest.LogCaptureFixture
) -> None:
    client = FakeClient()
    with caplog.at_level(logging.DEBUG, logger="eea_datalakehouse.dds_ingestion"):
        _make_ingest(data_folder, client).run()
    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert "s3cr3t" not in blob
    assert "_password" not in blob


def test_invalid_parallelism_rejected(data_folder: Path) -> None:
    client = FakeClient()
    with pytest.raises(ValueError):
        _make_ingest(data_folder, client, parallelism=0)


class MultipartFakeClient(FakeClient):
    """FakeClient whose begin returns a multipart target per file (DI-7.3)."""

    def begin(self, *, files: list[FileSpec], **kwargs: object) -> BeginResult:
        self.begin_calls.append({"files": files, **kwargs})
        uploads = tuple(
            UploadTarget(
                rel_path=f.rel_path,
                method="PUT",
                upload_id=f"up-{f.rel_path}",
                parts=(UploadPart(part_number=1, url=f"https://s3.test/{f.rel_path}/1"),),
            )
            for f in files
        )
        return BeginResult(
            session_id="sess-MP",
            status="open",
            s3=S3Plan(bucket="b", key_prefix="p/", uploads=uploads),
        )

    def upload_file_multipart(
        self, target: UploadTarget, data: bytes
    ) -> list[dict[str, object]]:
        with self._lock:
            self.uploaded.append(target.rel_path)
        return [{"part_number": 1, "etag": f'"etag-{target.rel_path}"'}]


def test_multipart_etags_threaded_into_commit(data_folder: Path) -> None:
    client = MultipartFakeClient()
    outcome = _make_ingest(data_folder, client, multipart=True).run()

    assert sorted(client.uploaded) == ["a.parquet", "sub/b.parquet"]
    assert outcome.files_uploaded == 2
    # commit received the contract-shaped multipart_etags list.
    sent = client.commit_calls[0]["multipart_etags"]
    assert isinstance(sent, list)
    by_rel = {e["rel_path"]: e for e in sent}
    assert set(by_rel) == {"a.parquet", "sub/b.parquet"}
    entry = by_rel["a.parquet"]
    assert entry["upload_id"] == "up-a.parquet"
    assert entry["parts"] == [{"part_number": 1, "etag": '"etag-a.parquet"'}]


def test_sub_path_is_passed_through_to_begin(tmp_path: Path) -> None:
    """DI-11.12: the class carries the custodian's chosen sub-folder, nothing more.

    Normalising and scoping it is the server's job — the client would otherwise
    be a second opinion about where the data lives.
    """
    (tmp_path / "a.parquet").write_bytes(b"PAR1")
    client = FakeClient()
    job = FolderIngest(
        tmp_path,
        "catalog/water/bwd",
        data_format="parquet",
        table_name="water_temperature",
        sub_path="2026",
        client=client,
        show_progress=False,
    )
    job.run()
    assert client.begin_calls[0]["sub_path"] == "2026"


def test_sub_path_defaults_to_none(tmp_path: Path) -> None:
    (tmp_path / "a.parquet").write_bytes(b"PAR1")
    client = FakeClient()
    FolderIngest(
        tmp_path, "catalog/water/bwd", data_format="parquet",
        client=client, show_progress=False,
    ).run()
    assert client.begin_calls[0]["sub_path"] is None


def test_retry_refuses_to_re_upload_a_permanently_stored_transfer(
    tmp_path: Path,
) -> None:
    """DI-11.7: a blind re-run there adds a second copy, so it must not happen.

    The server numbers an incoming name that already exists — precisely so an
    append can never overwrite live data — which turns a silent "start over"
    into duplicated rows rather than a clean retry.
    """
    (tmp_path / "a.parquet").write_bytes(b"PAR1")
    client = FakeClient()
    client.status_payload = {
        "status": "failed",
        "progress": {},
        "failed_stage": "load",
        "resumable": False,
        "placement": "read_permanent",
    }
    job = FolderIngest(
        tmp_path, "catalog/water/bwd", data_format="parquet",
        intent="read_only", client=client, show_progress=False,
    )
    job._session_id = "sess-1"

    with pytest.raises(IngestStateError) as excinfo:
        job.retry()
    assert "second copy" in str(excinfo.value)
    # And nothing was re-uploaded behind the user's back.
    assert client.begin_calls == []
    assert client.uploaded == []


def test_retry_still_starts_over_for_a_staged_transfer(tmp_path: Path) -> None:
    # The staged path is unchanged: its files were discarded, so re-uploading is
    # the only way forward and is safe.
    (tmp_path / "a.parquet").write_bytes(b"PAR1")
    client = FakeClient()
    client.status_payload = {
        "status": "failed",
        "progress": {},
        "failed_stage": "upload",
        "resumable": False,
    }
    job = FolderIngest(
        tmp_path, "catalog/water/bwd", data_format="parquet",
        client=client, show_progress=False,
    )
    job._session_id = "sess-1"
    job.retry()
    assert len(client.begin_calls) == 1
