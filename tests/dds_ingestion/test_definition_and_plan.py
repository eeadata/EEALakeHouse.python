"""PY-3 (optional table definition), PY-4 (show the plan, dry run) and PY-9
(retry with a corrected definition).

Companion to the DDS DI-12 plan; see docs/dds-ingest-robustness-client-plan.md.
"""

from __future__ import annotations

import dataclasses
import json
import logging
from pathlib import Path

import httpx
import pytest
import respx
from eea_datalakehouse.dds_ingestion.client import IngestClient, UnsupportedByServerError
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds
from eea_datalakehouse.dds_ingestion.folder import FolderIngest
from eea_datalakehouse.dds_ingestion.models import (
    BeginPlan,
    BeginResult,
    ColumnDef,
    DryRunResult,
    FileSpec,
    StatusResult,
    TableDefinition,
    definition_payload,
)

from .conftest import BASE_URL
from .test_folder import FakeClient

PLAN_JSON = {
    "placement": "staged",
    "table_path": "bio/uploads/t",
    "storage_path": None,
    "action": "create",
    "definition_source": "begin",
    "definition_applied": ["columns: the types of the table that is created"],
    "definition_ignored": ["columns.nullable: not supported yet"],
}
DEFINITION = TableDefinition(columns=(ColumnDef("a", "INT", nullable=False),))


def _begin_json(**extra: object) -> dict[str, object]:
    return {
        "session_id": "sess-1",
        "status": "uploading",
        "s3": {"bucket": "b", "key_prefix": "p/", "uploads": []},
        **extra,
    }


def _begin_args() -> dict[str, object]:
    return {
        "target_catalog_path": "bio/uploads",
        "intent": "editable",
        "data_format": "csv",
        "conflict_mode": "fail",
        "files": [FileSpec("a.csv", 6)],
    }


# --- the model ---------------------------------------------------------------------


def test_definition_payload_sends_only_what_was_set() -> None:
    assert TableDefinition().as_payload() == {}
    assert DEFINITION.as_payload() == {"columns": [{"name": "a", "type": "INT", "nullable": False}]}
    full = TableDefinition(
        columns=(ColumnDef("a", "INT", description="id"),),
        partition_by=("year",),
        sort_by=("a",),
        version=1,
    )
    assert full.as_payload() == {
        "version": 1,
        "columns": [{"name": "a", "type": "INT", "description": "id"}],
        "partition_by": ["year"],
        "sort_by": ["a"],
    }
    # A plain dict in the contract's shape is accepted as is (copied).
    raw = {"columns": [{"name": "a", "type": "INT"}]}
    assert definition_payload(raw) == raw and definition_payload(raw) is not raw
    assert definition_payload(None) is None


def test_begin_result_reads_the_plan_and_tolerates_its_absence() -> None:
    plan = BeginResult.from_json(_begin_json(plan=PLAN_JSON)).plan
    assert plan is not None
    assert plan.placement == "staged"
    assert plan.definition_ignored == ("columns.nullable: not supported yet",)
    assert BeginResult.from_json(_begin_json()).plan is None


def test_plan_describe_matches_the_extension_wording() -> None:
    plan = BeginPlan.from_json(PLAN_JSON)
    assert plan.describe() == "Importing as a new editable table at bio/uploads/t."
    read_only = dataclasses.replace(
        plan, placement="read_permanent", action="append", storage_path="src/read/t"
    )
    assert read_only.describe() == (
        "Appending to the existing read-only table at bio/uploads/t "
        "(files kept at src/read/t)."
    )


# --- IngestClient: request bodies ----------------------------------------------------


@respx.mock
def test_begin_without_a_definition_sends_todays_body(creds: DremioCreds) -> None:
    """Golden: the request body is unchanged for every existing call."""
    route = respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(200, json=_begin_json())
    )
    with IngestClient(BASE_URL, creds) as client:
        client.begin(**_begin_args())  # type: ignore[arg-type]
    assert json.loads(route.calls.last.request.read()) == {
        "target_catalog_path": "bio/uploads",
        "intent": "editable",
        "format": "csv",
        "conflict_mode": "fail",
        "runner": "notebook",
        "files": [{"rel_path": "a.csv", "size": 6}],
    }


@respx.mock
def test_begin_and_commit_send_the_definition(creds: DremioCreds) -> None:
    begin = respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(200, json=_begin_json(plan=PLAN_JSON))
    )
    commit = respx.post(f"{BASE_URL}/api/v1/ingest/commit").mock(
        return_value=httpx.Response(200, json={"session_id": "sess-1", "status": "done"})
    )
    with IngestClient(BASE_URL, creds) as client:
        client.begin(**_begin_args(), definition=DEFINITION)  # type: ignore[arg-type]
        client.commit(session_id="sess-1", definition=DEFINITION)
    expected = {"columns": [{"name": "a", "type": "INT", "nullable": False}]}
    assert json.loads(begin.calls.last.request.read())["definition"] == expected
    assert json.loads(commit.calls.last.request.read())["definition"] == expected


@respx.mock
def test_dry_run_returns_the_plan(creds: DremioCreds) -> None:
    route = respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": None,
                "status": "dry_run",
                "s3": {"bucket": "b", "key_prefix": "p/", "uploads": []},
                "collision": {"exists": False},
                "plan": PLAN_JSON,
            },
        )
    )
    with IngestClient(BASE_URL, creds) as client:
        result = client.dry_run(**_begin_args())  # type: ignore[arg-type]
    assert json.loads(route.calls.last.request.read())["dry_run"] is True
    assert result.plan.table_path == "bio/uploads/t"
    assert result.key_prefix == "p/"


@respx.mock
def test_dry_run_against_an_older_server_cancels_what_it_opened(creds: DremioCreds) -> None:
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(200, json=_begin_json())  # a real session, no plan
    )
    cancel = respx.delete(f"{BASE_URL}/api/v1/ingest/sess-1").mock(
        return_value=httpx.Response(204)
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(UnsupportedByServerError):
        client.dry_run(**_begin_args())  # type: ignore[arg-type]
    assert cancel.called


# --- FolderIngest -----------------------------------------------------------------


class PlanClient(FakeClient):
    """FakeClient whose begin optionally returns a plan, recording definitions."""

    def __init__(self, *, plan: dict[str, object] | None) -> None:
        super().__init__()
        self.plan = plan
        self.dry_run_calls: list[dict[str, object]] = []

    def begin(self, *, files: list[FileSpec], **kwargs: object) -> BeginResult:
        result = super().begin(files=files, **kwargs)
        plan = BeginPlan.from_json(self.plan) if self.plan is not None else None
        return dataclasses.replace(result, plan=plan)

    def dry_run(self, **kwargs: object) -> DryRunResult:
        self.dry_run_calls.append(kwargs)
        assert self.plan is not None
        return DryRunResult(plan=BeginPlan.from_json(self.plan), key_prefix="p/")


def _csv_folder(tmp_path: Path) -> Path:
    (tmp_path / "a.csv").write_text("a\n1\n")
    return tmp_path


def _job(folder: Path, client: FakeClient, **kwargs: object) -> FolderIngest:
    return FolderIngest(
        folder,
        "bio/uploads",
        data_format="csv",
        intent="editable",
        client=client,  # type: ignore[arg-type]
        **kwargs,  # type: ignore[arg-type]
    )


def test_the_definition_goes_on_begin_and_not_again_when_the_server_took_it(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    client = PlanClient(plan=PLAN_JSON)
    _job(_csv_folder(tmp_path), client, table_definition=DEFINITION).run()
    assert client.begin_calls[0]["definition"] == DEFINITION
    assert client.commit_calls[0]["definition"] is None
    # The plan is shown before the upload (show_progress is on by default).
    assert "Importing as a new editable table at bio/uploads/t." in capsys.readouterr().out


def test_an_older_server_gets_the_definition_on_commit(tmp_path: Path) -> None:
    client = PlanClient(plan=None)
    _job(_csv_folder(tmp_path), client, table_definition=DEFINITION).run()
    assert client.commit_calls[0]["definition"] == DEFINITION


def test_without_a_definition_nothing_extra_is_sent(tmp_path: Path) -> None:
    client = PlanClient(plan=PLAN_JSON)
    # infer_schema=False: PY-6 would otherwise send inferred columns for an
    # editable CSV ingest (test_folder_formats.py).
    _job(_csv_folder(tmp_path), client, infer_schema=False).run()
    assert client.begin_calls[0]["definition"] is None
    assert client.commit_calls[0]["definition"] is None


def test_ignored_definition_parts_are_a_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    client = PlanClient(plan=PLAN_JSON)
    with caplog.at_level(logging.WARNING, logger="eea_datalakehouse.dds_ingestion"):
        _job(_csv_folder(tmp_path), client, table_definition=DEFINITION, show_progress=False).run()
    assert "columns.nullable: not supported yet" in caplog.text


def test_show_progress_false_prints_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _job(_csv_folder(tmp_path), PlanClient(plan=PLAN_JSON), show_progress=False).run()
    assert capsys.readouterr().out == ""


def test_folder_dry_run_uploads_nothing_and_shows_the_plan(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    client = PlanClient(plan=PLAN_JSON)
    job = _job(_csv_folder(tmp_path), client, table_definition=DEFINITION)
    result = job.dry_run()
    assert result.plan.action == "create"
    assert client.dry_run_calls[0]["definition"] == DEFINITION
    assert client.begin_calls == [] and client.uploaded == [] and client.commit_calls == []
    assert job.session_id is None
    assert "Importing as a new editable table" in capsys.readouterr().out


# --- PY-9: retry with a corrected definition ----------------------------------------

DEFINITION_JSON = {"columns": [{"name": "a", "type": "INT", "nullable": False}]}


@respx.mock
def test_retry_without_a_definition_sends_todays_request(creds: DremioCreds) -> None:
    """Golden: no body, as before DI-12.6."""
    route = respx.post(f"{BASE_URL}/api/v1/ingest/sess-1/retry").mock(
        return_value=httpx.Response(200, json={"session_id": "sess-1", "status": "committing"})
    )
    with IngestClient(BASE_URL, creds) as client:
        client.retry("sess-1")
    request = route.calls.last.request
    assert request.read() == b""
    assert "content-type" not in request.headers


@respx.mock
def test_retry_sends_a_corrected_definition(creds: DremioCreds) -> None:
    route = respx.post(f"{BASE_URL}/api/v1/ingest/sess-1/retry").mock(
        return_value=httpx.Response(200, json={"session_id": "sess-1", "status": "committing"})
    )
    with IngestClient(BASE_URL, creds) as client:
        client.retry("sess-1", definition=DEFINITION)
    assert json.loads(route.calls.last.request.read()) == {"definition": DEFINITION_JSON}


class FailedSessionClient(PlanClient):
    """A session that failed with ``schema_required``; ``retry`` records its arguments."""

    def __init__(self, *, resumable: bool) -> None:
        super().__init__(plan=PLAN_JSON)
        self.status_payload = {
            "status": "failed",
            "progress": {},
            "failed_stage": "load" if resumable else "upload",
            "resumable": resumable,
            "error_code": "schema_required",
        }
        self.retry_calls: list[dict[str, object]] = []

    def retry(self, session_id: str, **kwargs: object) -> StatusResult:
        self.retry_calls.append({"session_id": session_id, **kwargs})
        return StatusResult.from_json({"session_id": session_id, "status": "done", "progress": {}})


def _attached(folder: Path, client: FakeClient) -> FolderIngest:
    return FolderIngest.attach(
        "sess-1",
        folder,
        "bio/uploads",
        data_format="csv",
        intent="editable",
        show_progress=False,
        client=client,  # type: ignore[arg-type]
    )


def test_retry_resumes_the_load_with_the_corrected_definition(tmp_path: Path) -> None:
    client = FailedSessionClient(resumable=True)
    job = _attached(_csv_folder(tmp_path), client)

    outcome = job.retry(table_definition=DEFINITION)

    assert outcome.resumed
    assert client.retry_calls == [{"session_id": "sess-1", "definition": DEFINITION}]
    assert client.begin_calls == []  # nothing uploaded again


def test_retry_without_a_definition_calls_retry_as_before(tmp_path: Path) -> None:
    client = FailedSessionClient(resumable=True)
    _attached(_csv_folder(tmp_path), client).retry()
    assert client.retry_calls == [{"session_id": "sess-1"}]


def test_a_retry_that_uploads_again_sends_the_corrected_definition_on_begin(
    tmp_path: Path,
) -> None:
    client = FailedSessionClient(resumable=False)
    job = _attached(_csv_folder(tmp_path), client)

    job.retry(table_definition=DEFINITION)

    assert client.retry_calls == []
    assert client.begin_calls[0]["definition"] == DEFINITION
