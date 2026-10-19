"""PY-6: the format table, CSV/JSON options, inferred columns, local Parquet check.

Real files throughout (written with pyarrow), so the local schema readers are
exercised, not stubbed.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import respx
from eea_datalakehouse.dds_ingestion.client import IngestApiError, IngestClient
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds
from eea_datalakehouse.dds_ingestion.folder import FolderIngest
from eea_datalakehouse.dds_ingestion.formats import compare_columns, merge_columns
from eea_datalakehouse.dds_ingestion.models import ColumnDef, FileSpec, TableDefinition

from .conftest import BASE_URL
from .test_folder import FakeClient


def _job(folder: Path, fmt: str, client: FakeClient, **kwargs: object) -> FolderIngest:
    return FolderIngest(
        folder, "bio/uploads", data_format=fmt, show_progress=False,  # type: ignore[arg-type]
        client=client, **kwargs,  # type: ignore[arg-type]
    )


def _sent_columns(client: FakeClient) -> list[tuple[str, str]]:
    definition = client.begin_calls[0]["definition"]
    assert isinstance(definition, TableDefinition)
    return [(c.name, c.type) for c in definition.columns]


# --- inferred columns (CSV/JSON into an editable table) --------------------------


def test_csv_columns_are_inferred_from_every_file(tmp_path: Path) -> None:
    (tmp_path / "a.csv").write_text("id,price,name\n1,2,x\n")
    (tmp_path / "b.csv").write_text("id,price,name\n2,2.5,y\n")  # price widens to DOUBLE
    client = FakeClient()
    _job(tmp_path, "csv", client, intent="editable").run()
    assert _sent_columns(client) == [("id", "BIGINT"), ("price", "DOUBLE"), ("name", "VARCHAR")]


def test_inference_honours_the_csv_options(tmp_path: Path) -> None:
    (tmp_path / "a.csv").write_text("1;x\n2;y\n")
    client = FakeClient()
    options = {"delimiter": ";", "header": False}
    _job(tmp_path, "csv", client, intent="editable", csv_options=options).run()
    assert [t for _, t in _sent_columns(client)] == ["BIGINT", "VARCHAR"]
    assert client.begin_calls[0]["csv_options"] == options


def test_json_lines_columns_are_inferred(tmp_path: Path) -> None:
    (tmp_path / "a.ndjson").write_text('{"id": 1, "ok": true}\n{"id": 2, "ok": false}\n')
    client = FakeClient()
    _job(tmp_path, "json", client, intent="editable").run()
    assert _sent_columns(client) == [("id", "BIGINT"), ("ok", "BOOLEAN")]


def test_a_json_array_cannot_be_inferred_and_says_so(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    (tmp_path / "a.json").write_text(json.dumps([{"id": 1}]))
    client = FakeClient()
    with caplog.at_level(logging.WARNING, logger="eea_datalakehouse.dds_ingestion"):
        _job(tmp_path, "json", client, intent="editable", json_options={"format": "array"}).run()
    assert client.begin_calls[0]["definition"] is None
    assert "could not infer the json columns" in caplog.text


@pytest.mark.parametrize(
    "kwargs", [{"intent": "read_only"}, {"intent": "editable", "infer_schema": False}]
)
def test_no_inference_where_the_columns_are_not_used_or_it_is_off(
    tmp_path: Path, kwargs: dict[str, object]
) -> None:
    (tmp_path / "a.csv").write_text("id\n1\n")
    client = FakeClient()
    _job(tmp_path, "csv", client, **kwargs).run()
    assert client.begin_calls[0]["definition"] is None


def test_a_given_definition_is_sent_as_is(tmp_path: Path) -> None:
    (tmp_path / "a.csv").write_text("id\n1\n")
    given = TableDefinition(columns=(ColumnDef("id", "INT"),))
    client = FakeClient()
    _job(tmp_path, "csv", client, intent="editable", table_definition=given).run()
    assert client.begin_calls[0]["definition"] is given


# --- the local Parquet footer check -----------------------------------------------


def _parquet(folder: Path) -> Path:
    table = pa.table({"id": pa.array([1], pa.int64()), "name": ["a"]})
    pq.write_table(table, folder / "a.parquet")
    return folder


def test_given_parquet_columns_that_contradict_the_footer_fail_before_upload(
    tmp_path: Path,
) -> None:
    client = FakeClient()
    given = TableDefinition(columns=(ColumnDef("id", "INT"),))
    with pytest.raises(IngestApiError) as exc:
        _job(_parquet(tmp_path), "parquet", client, table_definition=given).run()
    assert exc.value.error_slug == "schema_mismatch"
    assert "id: given INT, the files have BIGINT" in exc.value.message
    assert client.begin_calls == [] and client.uploaded == []


def test_given_parquet_columns_that_match_go_through(tmp_path: Path) -> None:
    client = FakeClient()
    given = {"columns": [{"name": "ID", "type": "bigint"}, {"name": "name", "type": "VARCHAR(10)"}]}
    _job(_parquet(tmp_path), "parquet", client, table_definition=given).run()
    assert client.begin_calls[0]["definition"] == given


# --- the shared rules -----------------------------------------------------------------


def test_compare_columns_uses_the_servers_wording() -> None:
    actual = [("id", "BIGINT")]
    assert compare_columns([("id", "INT"), ("x", "VARCHAR")], actual) == [
        "id: given INT, the files have BIGINT",
        "x: not in the files",
    ]


def test_merge_columns_widens() -> None:
    assert merge_columns([[("a", "INT")], [("a", "BIGINT")], [("b", "DATE")]]) == [
        ("a", "BIGINT"),
        ("b", "DATE"),
    ]
    assert merge_columns([[("a", "BIGINT")], [("a", "VARCHAR")]]) == [("a", "VARCHAR")]


# --- the request body -----------------------------------------------------------------


@respx.mock
def test_csv_options_reach_begin_only_for_csv(creds: DremioCreds) -> None:
    route = respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "s",
                "status": "uploading",
                "s3": {"bucket": "b", "key_prefix": "p/"},
            },
        )
    )
    with IngestClient(BASE_URL, creds) as client:
        for fmt in ("csv", "parquet"):
            client.begin(
                target_catalog_path="t", intent="editable", data_format=fmt,  # type: ignore[arg-type]
                conflict_mode="fail", files=[FileSpec("a", 1)], csv_options={"delimiter": ";"},
            )
    sent = [json.loads(call.request.read()) for call in route.calls]
    assert sent[0]["csv_options"] == {"delimiter": ";"}
    assert "csv_options" not in sent[1]
