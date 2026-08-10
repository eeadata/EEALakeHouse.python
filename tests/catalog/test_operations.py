from __future__ import annotations

from pathlib import Path

import pytest

from eea_datalakehouse.catalog import operations, retry_state
from eea_datalakehouse.catalog.errors import CatalogOperationError, EngineStartingError
from .conftest import FakeCatalogRest, FakeExecutor


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Every test in this file gets its own retry-state file, never the real ~/.cache one."""
    state_path = tmp_path / "state.json"
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", state_path)
    return state_path


def test_table2view_checks_source_then_drops_then_creates(catalog_rest) -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "source"}])

    operations.table2view(
        fake, "a.view", "a.source", catalog_rest=catalog_rest, idempotency_key="k"
    )

    assert fake.statements == [
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'source'",
        'DROP TABLE IF EXISTS "a"."view"',
        'CREATE VIEW "a"."view" AS SELECT * FROM "a"."source"',
    ]


def test_table2view_raises_when_source_does_not_exist(catalog_rest) -> None:
    fake = FakeExecutor(rows=[])  # INFORMATION_SCHEMA finds nothing

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.table2view(
            fake, "a.view", "a.missing", catalog_rest=catalog_rest, idempotency_key="k"
        )

    # Fails fast — never gets to DROP/CREATE.
    assert len(fake.statements) == 1


def test_table2view_creates_missing_target_folder() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "source"}])
    catalog_rest = FakeCatalogRest(existing={"bwd"})  # "bwd.consumer" not yet there

    operations.table2view(
        fake,
        "bwd.consumer.viewname",
        "bwd.source",
        catalog_rest=catalog_rest,
        idempotency_key="k",
    )

    assert catalog_rest.created == ["bwd.consumer"]


def test_table2view_skips_folder_handling_when_disabled() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "source"}])

    # catalog_rest=None would normally be required; create_target_folder=False
    # means it's never even consulted.
    operations.table2view(
        fake, "a.view", "a.source", create_target_folder=False, idempotency_key="k"
    )

    assert fake.statements[1:] == [
        'DROP TABLE IF EXISTS "a"."view"',
        'CREATE VIEW "a"."view" AS SELECT * FROM "a"."source"',
    ]


def test_table2view_raises_when_folder_check_needed_but_no_catalog_rest_given() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "source"}])

    with pytest.raises(CatalogOperationError, match="catalog_rest"):
        operations.table2view(fake, "a.view", "a.source", idempotency_key="k")


def test_draft2version_is_a_single_ctas(executor) -> None:
    operations.draft2version(executor, "bwd.draft.bw", "bwd.versions.v1", idempotency_key="k")

    assert executor.statements == [
        'CREATE TABLE "bwd"."versions"."v1" AS SELECT * FROM "bwd"."draft"."bw"',
    ]


def test_publishversion_uses_create_or_replace_view(executor) -> None:
    operations.publishversion(executor, "bwd.consumer", "bwd.versions.v1", idempotency_key="k")

    assert executor.statements == [
        'CREATE OR REPLACE VIEW "bwd"."consumer" AS SELECT * FROM "bwd"."versions"."v1"',
    ]


def test_datacopy_create_mode_has_no_drop(executor) -> None:
    operations.datacopy(executor, "a.src", "a.dst", idempotency_key="k")

    assert executor.statements == ['CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"']


def test_datacopy_replace_mode_drops_first(executor) -> None:
    operations.datacopy(executor, "a.src", "a.dst", mode="replace", idempotency_key="k")

    assert executor.statements == [
        'DROP TABLE IF EXISTS "a"."dst"',
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
    ]


def test_datamove_table_creates_then_drops_source(executor) -> None:
    operations.datamove(executor, "a.src", "a.dst", entry_type="TABLE", idempotency_key="k")

    assert executor.statements == [
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP TABLE "a"."src"',
    ]


def test_datamove_view_uses_view_statements(executor) -> None:
    operations.datamove(executor, "a.src", "a.dst", entry_type="VIEW", idempotency_key="k")

    assert executor.statements == [
        'CREATE VIEW "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP VIEW "a"."src"',
    ]


def test_deleteview_drops_if_exists(executor) -> None:
    operations.deleteview(executor, "a.view", idempotency_key="k")

    assert executor.statements == ['DROP VIEW IF EXISTS "a"."view"']


def test_deleteview_is_registered_for_retry(executor) -> None:
    retry_state.record("k", "deleteview", "a.view", "earlier failure", params={"view_path": "a.view"})

    operations.retry_pending(executor, "k")

    assert executor.statements == ['DROP VIEW IF EXISTS "a"."view"']


def test_engine_starting_error_propagates_and_is_remembered(stalling_executor) -> None:
    with pytest.raises(EngineStartingError):
        operations.draft2version(stalling_executor, "a.draft", "a.v1", idempotency_key="stall-key")

    pending = retry_state.get("stall-key")
    assert pending is not None
    assert pending.operation == "draft2version"
    assert pending.params == {"draft_path": "a.draft", "version_path": "a.v1"}
    assert "step 1/1" in pending.last_error


def test_second_step_stalling_is_reported_with_its_step_number(executor) -> None:
    executor._fail_on = {1: EngineStartingError("stalled", idempotency_key="k")}

    with pytest.raises(EngineStartingError):
        operations.datacopy(executor, "a.src", "a.dst", mode="replace", idempotency_key="k")

    pending = retry_state.get("k")
    assert pending is not None
    assert "step 2/2" in pending.last_error


def test_success_clears_a_previously_remembered_attempt(executor) -> None:
    retry_state.record("k", "draft2version", "a.v1", "earlier failure")
    assert retry_state.get("k") is not None

    operations.draft2version(executor, "a.draft", "a.v1", idempotency_key="k")

    assert retry_state.get("k") is None


def test_retry_pending_dispatches_back_to_the_same_operation(executor) -> None:
    retry_state.record(
        "k",
        "publishversion",
        "a.consumer",
        "earlier failure",
        params={"consumer_view_path": "a.consumer", "version_path": "a.v2"},
    )

    operations.retry_pending(executor, "k")

    assert executor.statements == [
        'CREATE OR REPLACE VIEW "a"."consumer" AS SELECT * FROM "a"."v2"',
    ]


def test_retry_pending_raises_keyerror_when_nothing_pending(executor) -> None:
    with pytest.raises(KeyError):
        operations.retry_pending(executor, "missing-key")


def test_gettablesfrom_queries_information_schema_and_returns_full_paths() -> None:
    fake = FakeExecutor(
        rows=[
            {"TABLE_SCHEMA": "bwd.versions", "TABLE_NAME": "assessments"},
            {"TABLE_SCHEMA": "bwd.versions.v1", "TABLE_NAME": "sites"},
        ]
    )

    paths = operations.gettablesfrom(fake, "bwd.versions", idempotency_key="k")

    assert paths == ["bwd.versions.assessments", "bwd.versions.v1.sites"]
    assert fake.statements == [
        'SELECT "TABLE_SCHEMA", "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'bwd.versions' "
        "OR \"TABLE_SCHEMA\" LIKE 'bwd.versions.%' ESCAPE '\\'"
    ]


def test_gettablesfrom_escapes_single_quotes_in_the_schema_literal() -> None:
    fake = FakeExecutor(rows=[])

    operations.gettablesfrom(fake, "o'brien.schema", idempotency_key="k")

    assert "'o''brien.schema'" in fake.statements[0]
    assert "'o''brien.schema.%'" in fake.statements[0]


def test_gettablesfrom_escapes_underscores_so_they_are_not_like_wildcards() -> None:
    fake = FakeExecutor(rows=[])

    operations.gettablesfrom(fake, "eu_sdg_14_40", idempotency_key="k")

    assert "'eu\\_sdg\\_14\\_40.%'" in fake.statements[0]


def test_gettableitemsfrom_returns_schema_and_row_count_only() -> None:
    fake = FakeExecutor(
        rows_sequence=[
            [{"COLUMN_NAME": "id", "DATA_TYPE": "INTEGER"}, {"COLUMN_NAME": "name", "DATA_TYPE": "VARCHAR"}],
            [{"row_count": 42}],
        ]
    )

    result = operations.gettableitemsfrom(fake, "bwd.versions.v1", idempotency_key="k")

    assert result.schema == {"id": "INTEGER", "name": "VARCHAR"}
    assert result.row_count == 42
    assert not hasattr(result, "items")
    assert fake.statements == [
        'SELECT "COLUMN_NAME", "DATA_TYPE" FROM INFORMATION_SCHEMA."COLUMNS" '
        "WHERE \"TABLE_SCHEMA\" = 'bwd.versions' AND \"TABLE_NAME\" = 'v1' "
        'ORDER BY "ORDINAL_POSITION"',
        'SELECT COUNT(*) AS "row_count" FROM "bwd"."versions"."v1"',
    ]


def test_gettableitemsfrom_raises_when_table_does_not_exist() -> None:
    fake = FakeExecutor(rows=[])  # COLUMNS query finds nothing

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.gettableitemsfrom(fake, "bwd.versions.missing", idempotency_key="k")

    # Never gets to the COUNT(*) query.
    assert len(fake.statements) == 1


def test_gettablesfrom_engine_starting_is_remembered_and_retryable() -> None:
    fake = FakeExecutor(fail_on={0: EngineStartingError("stalled", idempotency_key="k")})

    with pytest.raises(EngineStartingError):
        operations.gettablesfrom(fake, "bwd.versions", idempotency_key="k")

    pending = retry_state.get("k")
    assert pending is not None
    assert pending.operation == "gettablesfrom"
    assert pending.params == {"schema_path": "bwd.versions"}

    fake._fail_on = {}
    fake._rows = [{"TABLE_SCHEMA": "bwd.versions", "TABLE_NAME": "assessments"}]
    assert operations.retry_pending(fake, "k") == ["bwd.versions.assessments"]
