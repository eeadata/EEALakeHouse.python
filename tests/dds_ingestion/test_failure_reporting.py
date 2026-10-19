"""PY-5: failure text, retryable, cleanup_on_failure.

The expected sentences are the same as the JupyterLab extension's jest tests for
``describeIngestFailure`` (tests/ingestFiles.test.ts in EEALakeHouse): both
clients must describe a failure identically (DI-12.14).
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx
from eea_datalakehouse.dds_ingestion.client import IngestApiError, IngestClient
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds
from eea_datalakehouse.dds_ingestion.folder import FolderIngest
from eea_datalakehouse.dds_ingestion.models import CommitResult, StatusResult

from .conftest import BASE_URL
from .test_folder import FakeClient


def _status(**raw: object) -> StatusResult:
    return StatusResult.from_json({"status": "failed", "progress": {}, **raw})


def test_describe_failure_matches_the_extension() -> None:
    assert _status(
        error="Dremio load failed: no columns",
        failed_stage="load",
        resumable=True,
        error_code="schema_required",
    ).describe_failure() == (
        "Failed in the load stage (schema_required): Dremio load failed: no columns. "
        "Retry re-runs it with the files already uploaded."
    )
    assert _status(
        error="S3 upload failed: missing",
        failed_stage="upload",
        resumable=False,
        error_code="bad_request",
    ).describe_failure() == (
        "Failed in the upload stage (bad_request): S3 upload failed: missing. "
        "The uploaded files are gone; import the data again."
    )
    # An older server sends only `error`.
    assert _status(error="boom").describe_failure() == "Failed: boom."
    assert _status(error="already ends.").describe_failure() == "Failed: already ends."


@respx.mock
def test_an_api_error_carries_the_slug_and_retryable(creds: DremioCreds) -> None:
    respx.post(f"{BASE_URL}/api/v1/ingest/commit").mock(
        return_value=httpx.Response(
            422, json={"error": "schema_mismatch", "message": "no", "retryable": True}
        )
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError) as exc:
        client.commit(session_id="s")
    assert (exc.value.error_slug, exc.value.retryable) == ("schema_mismatch", True)


@respx.mock
def test_an_older_server_leaves_retryable_unknown(creds: DremioCreds) -> None:
    respx.post(f"{BASE_URL}/api/v1/ingest/commit").mock(
        return_value=httpx.Response(400, json={"error": "bad_request", "message": "no"})
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError) as exc:
        client.commit(session_id="s")
    assert exc.value.retryable is None


class FailingCommitClient(FakeClient):
    def __init__(self, *, retryable: bool) -> None:
        super().__init__()
        self.retryable = retryable
        self.cancelled: list[str] = []

    def commit(self, *, session_id: str, **kwargs: object) -> CommitResult:
        err = IngestApiError(422, "the given columns do not match", where="POST commit")
        err.error_slug, err.retryable = "schema_mismatch", self.retryable
        raise err

    def get_status(self, session_id: str) -> StatusResult:
        return _status(
            error="Dremio load failed: the given columns do not match",
            failed_stage="load",
            resumable=self.retryable,
            error_code="schema_mismatch",
        )

    def cancel(self, session_id: str) -> None:
        self.cancelled.append(session_id)


def _job(folder: Path, client: FakeClient, **kwargs: object) -> FolderIngest:
    return FolderIngest(
        folder, "bio/uploads", data_format="parquet", show_progress=False,
        client=client, **kwargs,  # type: ignore[arg-type]
    )


def test_a_failed_commit_is_described_like_the_transfers_panel(data_folder: Path) -> None:
    client = FailingCommitClient(retryable=True)
    with pytest.raises(IngestApiError) as exc:
        _job(data_folder, client).run()
    assert (
        "Failed in the load stage (schema_mismatch): Dremio load failed: the given columns "
        "do not match. Retry re-runs it with the files already uploaded."
    ) in exc.value.__notes__
    assert client.cancelled == []  # kept for retry by default


def test_a_failed_commit_carries_its_stage(data_folder: Path) -> None:
    # PY-13: the stage is an attribute too, not only part of the note.
    with pytest.raises(IngestApiError) as exc:
        _job(data_folder, FailingCommitClient(retryable=True)).run()
    assert exc.value.failed_stage == "load"


@respx.mock
def test_an_api_error_has_no_stage_of_its_own(creds: DremioCreds) -> None:
    respx.post(f"{BASE_URL}/api/v1/ingest/commit").mock(
        return_value=httpx.Response(400, json={"error": "bad_request", "message": "no"})
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError) as exc:
        client.commit(session_id="s")
    assert exc.value.failed_stage is None


@pytest.mark.parametrize(
    ("retryable", "cleanup", "cancelled"),
    [(True, True, True), (False, True, False), (True, False, False)],
)
def test_cleanup_on_failure(
    data_folder: Path, retryable: bool, cleanup: bool, cancelled: bool
) -> None:
    client = FailingCommitClient(retryable=retryable)
    with pytest.raises(IngestApiError):
        _job(data_folder, client, cleanup_on_failure=cleanup).run()
    assert (client.cancelled == ["sess-X"]) is cancelled
