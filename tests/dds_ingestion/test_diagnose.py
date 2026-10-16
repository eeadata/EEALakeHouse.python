"""PY-7: FolderIngest.diagnose(), through the real IngestClient over mocked HTTP."""

from __future__ import annotations

from pathlib import Path

import httpx
import respx
from eea_datalakehouse.dds_ingestion.client import IngestClient
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds
from eea_datalakehouse.dds_ingestion.folder import Diagnosis, FolderIngest

from .conftest import BASE_URL

PLAN = {
    "placement": "staged",
    "table_path": "bio/uploads/t",
    "storage_path": None,
    "action": "create",
    "definition_source": "parquet_footer",
}
DRY_RUN = {
    "session_id": None,
    "status": "dry_run",
    "s3": {"bucket": "b", "key_prefix": "p/", "uploads": []},
    "plan": PLAN,
}


def _diagnose(folder: Path, creds: DremioCreds) -> Diagnosis:
    with IngestClient(BASE_URL, creds) as client:
        job = FolderIngest(
            folder, "bio/uploads", data_format="parquet", show_progress=False, client=client
        )
        return job.diagnose()


def _summary(result: Diagnosis) -> list[tuple[str, bool | None]]:
    return [(step.name, step.ok) for step in result.steps]


@respx.mock
def test_everything_green(data_folder: Path, creds: DremioCreds) -> None:
    respx.get(f"{BASE_URL}/health").mock(return_value=httpx.Response(200, json={"status": "ok"}))
    respx.get(f"{BASE_URL}/api/v1/ingest/limits").mock(
        return_value=httpx.Response(200, json={"max_bytes": 1})
    )
    respx.post(f"{BASE_URL}/storage-selftest").mock(
        return_value=httpx.Response(200, json={"storage_write_test": {"ok": True}})
    )
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(200, json=DRY_RUN)
    )

    result = _diagnose(data_folder, creds)

    assert result.ok
    assert _summary(result) == [
        ("DDS reachable", True),
        ("Credential accepted", True),
        ("Storage write test", True),
        ("Dry run of this folder", True),
    ]
    assert result.steps[3].detail == "Importing as a new editable table at bio/uploads/t."


@respx.mock
def test_an_unreachable_dds_skips_the_rest(data_folder: Path, creds: DremioCreds) -> None:
    respx.get(f"{BASE_URL}/health").mock(side_effect=httpx.ConnectError("refused"))
    result = _diagnose(data_folder, creds)
    assert not result.ok
    assert _summary(result) == [
        ("DDS reachable", False),
        ("Credential accepted", None),
        ("Storage write test", None),
        ("Dry run of this folder", None),
    ]


@respx.mock
def test_a_rejected_credential(data_folder: Path, creds: DremioCreds) -> None:
    respx.get(f"{BASE_URL}/health").mock(return_value=httpx.Response(200))
    respx.get(f"{BASE_URL}/api/v1/ingest/limits").mock(
        return_value=httpx.Response(401, json={"error": "unauthorized", "message": "bad PAT"})
    )
    respx.post(f"{BASE_URL}/storage-selftest").mock(
        return_value=httpx.Response(200, json={"storage_write_test": {"ok": True}})
    )
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(401, json={"error": "unauthorized", "message": "bad PAT"})
    )
    result = _diagnose(data_folder, creds)
    assert dict(_summary(result))["Credential accepted"] is False
    assert result.steps[1].detail == "bad PAT"


@respx.mock
def test_an_older_server(data_folder: Path, creds: DremioCreds) -> None:
    respx.get(f"{BASE_URL}/health").mock(return_value=httpx.Response(200))
    respx.get(f"{BASE_URL}/api/v1/ingest/limits").mock(return_value=httpx.Response(404))
    respx.get(f"{BASE_URL}/api/v1/ingest").mock(return_value=httpx.Response(200, json=[]))
    respx.post(f"{BASE_URL}/storage-selftest").mock(return_value=httpx.Response(404))
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "s1",
                "status": "uploading",
                "s3": {"bucket": "b", "key_prefix": "p/"},
            },
        )
    )
    cancel = respx.delete(f"{BASE_URL}/api/v1/ingest/s1").mock(return_value=httpx.Response(204))

    result = _diagnose(data_folder, creds)

    assert result.ok  # nothing failed; two checks could not run
    assert _summary(result) == [
        ("DDS reachable", True),
        ("Credential accepted", True),
        ("Storage write test", None),
        ("Dry run of this folder", None),
    ]
    assert cancel.called  # the session the old server opened was not left behind


@respx.mock
def test_no_write_privilege_on_the_target(data_folder: Path, creds: DremioCreds) -> None:
    respx.get(f"{BASE_URL}/health").mock(return_value=httpx.Response(200))
    respx.get(f"{BASE_URL}/api/v1/ingest/limits").mock(return_value=httpx.Response(200, json={}))
    respx.post(f"{BASE_URL}/storage-selftest").mock(
        return_value=httpx.Response(
            200,
            json={"storage_write_test": {"ok": False, "failures": ["catalog/upload: HTTP 501"]}},
        )
    )
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(
            403, json={"error": "forbidden", "message": "no WRITE privilege on bio/uploads"}
        )
    )
    result = _diagnose(data_folder, creds)
    assert not result.ok
    steps = {step.name: step for step in result.steps}
    assert steps["Storage write test"].ok is False
    assert "HTTP 501" in steps["Storage write test"].detail
    assert steps["Dry run of this folder"].detail == "no WRITE privilege on bio/uploads"
