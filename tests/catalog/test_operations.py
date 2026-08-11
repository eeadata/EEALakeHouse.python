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


_SOURCE_LOOKUP = (
    'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
    "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'src'"
)


def test_datacopy_create_mode_has_no_drop() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])

    operations.datacopy(fake, "a.src", "a.dst", idempotency_key="k")

    assert fake.statements == [
        _SOURCE_LOOKUP,
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
    ]


def test_datacopy_replace_mode_drops_first() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])

    operations.datacopy(fake, "a.src", "a.dst", mode="replace", idempotency_key="k")

    assert fake.statements == [
        _SOURCE_LOOKUP,
        'DROP TABLE IF EXISTS "a"."dst"',
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
    ]


def test_datacopy_raises_when_source_does_not_exist() -> None:
    fake = FakeExecutor(rows=[])

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.datacopy(fake, "a.src", "a.dst", idempotency_key="k")

    # Fails fast — never gets to CREATE.
    assert fake.statements == [_SOURCE_LOOKUP]


def test_datacopy_creates_missing_target_folder() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])
    catalog_rest = FakeCatalogRest(existing={"bwd"})  # "bwd.consumer" not yet there

    operations.datacopy(
        fake,
        "bwd.src",
        "bwd.consumer.dst",
        create_target_folder=True,
        catalog_rest=catalog_rest,
        idempotency_key="k",
    )

    assert catalog_rest.created == ["bwd.consumer"]


def test_datacopy_raises_when_folder_check_needed_but_no_catalog_rest_given() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])

    with pytest.raises(CatalogOperationError, match="catalog_rest"):
        operations.datacopy(
            fake, "bwd.src", "bwd.consumer.dst", create_target_folder=True, idempotency_key="k"
        )


def test_datamove_table_creates_then_drops_source() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])

    operations.datamove(fake, "a.src", "a.dst", entry_type="TABLE", idempotency_key="k")

    assert fake.statements == [
        _SOURCE_LOOKUP,
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP TABLE "a"."src"',
    ]


def test_datamove_view_uses_view_statements() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])

    operations.datamove(fake, "a.src", "a.dst", entry_type="VIEW", idempotency_key="k")

    assert fake.statements == [
        _SOURCE_LOOKUP,
        'CREATE VIEW "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP VIEW "a"."src"',
    ]


def test_datamove_raises_when_source_does_not_exist() -> None:
    fake = FakeExecutor(rows=[])

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.datamove(fake, "a.src", "a.dst", entry_type="TABLE", idempotency_key="k")

    # Fails fast — never gets to CREATE/DROP.
    assert fake.statements == [_SOURCE_LOOKUP]


def test_datamove_auto_detects_a_view_when_entry_type_not_given() -> None:
    fake = FakeExecutor(rows=[{"TABLE_TYPE": "VIEW"}])

    operations.datamove(fake, "a.src", "a.dst", idempotency_key="k")

    assert fake.statements == [
        'SELECT "TABLE_TYPE" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'src'",
        'CREATE VIEW "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP VIEW "a"."src"',
    ]


def test_datamove_auto_detects_a_table_when_entry_type_not_given() -> None:
    fake = FakeExecutor(rows=[{"TABLE_TYPE": "TABLE"}])

    operations.datamove(fake, "a.src", "a.dst", idempotency_key="k")

    assert fake.statements[-2:] == [
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP TABLE "a"."src"',
    ]


def test_datamove_auto_detect_raises_when_source_does_not_exist() -> None:
    fake = FakeExecutor(rows=[])

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.datamove(fake, "a.src", "a.dst", idempotency_key="k")


def test_datamove_auto_detect_survives_getting_manual_entry_type_wrong() -> None:
    # The exact real-world failure this sidesteps: caller says VIEW, source
    # is actually a TABLE — DROP VIEW on a table fails outright. Passing no
    # entry_type at all detects the real kind instead of trusting a guess.
    fake = FakeExecutor(rows=[{"TABLE_TYPE": "TABLE"}])

    operations.datamove(fake, "a.src", "a.dst", idempotency_key="k")

    assert fake.statements[-1] == 'DROP TABLE "a"."src"'


def test_datamove_retry_re_detects_entry_type() -> None:
    # entry_type=None in the remembered params (never given up front) —
    # the retry must re-run detection, not assume a type.
    retry_state.record(
        "k",
        "datamove",
        "a.dst",
        "earlier failure",
        params={
            "source_path": "a.src",
            "target_path": "a.dst",
            "entry_type": None,
            "create_target_folder": False,
        },
    )
    fake = FakeExecutor(rows=[{"TABLE_TYPE": "VIEW"}])

    operations.retry_pending(fake, "k")

    assert fake.statements == [
        'SELECT "TABLE_TYPE" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'src'",
        'CREATE VIEW "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP VIEW "a"."src"',
    ]


def test_datamove_creates_missing_target_folder() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])
    catalog_rest = FakeCatalogRest(existing={"bwd"})  # "bwd.consumer" not yet there

    operations.datamove(
        fake,
        "bwd.src",
        "bwd.consumer.dst",
        entry_type="TABLE",
        create_target_folder=True,
        catalog_rest=catalog_rest,
        idempotency_key="k",
    )

    assert catalog_rest.created == ["bwd.consumer"]


def test_datacopy_appends_source_name_when_target_is_an_existing_folder() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])
    catalog_rest = FakeCatalogRest(existing={"bwd", "bwd.folder"}, folders={"bwd.folder"})

    result = operations.datacopy(
        fake, "bwd.src", "bwd.folder", catalog_rest=catalog_rest, idempotency_key="k"
    )

    assert fake.statements[-1] == 'CREATE TABLE "bwd"."folder"."src" AS SELECT * FROM "bwd"."src"'
    assert result.job_id == "job-1"


def test_datacopy_uses_target_literally_when_it_is_not_a_folder() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])
    catalog_rest = FakeCatalogRest(existing={"bwd"})  # "bwd.dst" doesn't exist as anything

    operations.datacopy(
        fake, "bwd.src", "bwd.dst", catalog_rest=catalog_rest, idempotency_key="k"
    )

    assert fake.statements[-1] == 'CREATE TABLE "bwd"."dst" AS SELECT * FROM "bwd"."src"'


def test_datacopy_uses_target_literally_without_catalog_rest() -> None:
    # No catalog_rest at all — folder-append is skipped, not an error.
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])

    operations.datacopy(fake, "bwd.src", "bwd.folder", idempotency_key="k")

    assert fake.statements[-1] == 'CREATE TABLE "bwd"."folder" AS SELECT * FROM "bwd"."src"'


def test_datacopy_folder_append_is_remembered_for_retry() -> None:
    catalog_rest = FakeCatalogRest(existing={"bwd", "bwd.folder"}, folders={"bwd.folder"})
    fake = FakeExecutor(
        rows=[{"TABLE_NAME": "src"}],
        fail_on={1: EngineStartingError("stalled", idempotency_key="k")},
    )

    with pytest.raises(EngineStartingError):
        operations.datacopy(
            fake, "bwd.src", "bwd.folder", catalog_rest=catalog_rest, idempotency_key="k"
        )

    pending = retry_state.get("k")
    assert pending is not None
    # The resolved path (folder + source name), not the original folder path.
    assert pending.params["target_path"] == "bwd.folder.src"


def test_datamove_appends_source_name_when_target_is_an_existing_folder() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "src"}])
    catalog_rest = FakeCatalogRest(existing={"bwd", "bwd.folder"}, folders={"bwd.folder"})

    operations.datamove(
        fake,
        "bwd.src",
        "bwd.folder",
        entry_type="TABLE",
        catalog_rest=catalog_rest,
        idempotency_key="k",
    )

    assert fake.statements == [
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'bwd' AND \"TABLE_NAME\" = 'src'",
        'CREATE TABLE "bwd"."folder"."src" AS SELECT * FROM "bwd"."src"',
        'DROP TABLE "bwd"."src"',
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


def test_second_step_stalling_is_reported_with_its_step_number() -> None:
    executor = FakeExecutor(
        rows=[{"TABLE_NAME": "src"}],
        fail_on={1: EngineStartingError("stalled", idempotency_key="k")},
    )

    with pytest.raises(EngineStartingError):
        operations.datacopy(executor, "a.src", "a.dst", mode="replace", idempotency_key="k")

    pending = retry_state.get("k")
    assert pending is not None
    # actions: check_source_exists, resolve_target (no-op, no catalog_rest),
    # ensure_target_folder (no-op), drop, create
    assert "step 4/5" in pending.last_error


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


def test_getwikifrom_returns_the_wiki_text() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"}, wikis={"a.b": "# Docs"})

    text = operations.getwikifrom(fake_rest, "a.b", idempotency_key="k")

    assert text == "# Docs"


def test_getwikifrom_engine_starting_is_remembered_and_retryable(executor) -> None:
    fake_rest = FakeCatalogRest(
        raise_on_get_wiki=EngineStartingError("stalled", idempotency_key="k")
    )

    with pytest.raises(EngineStartingError):
        operations.getwikifrom(fake_rest, "a.b", idempotency_key="k")

    pending = retry_state.get("k")
    assert pending is not None
    assert pending.operation == "getwikifrom"
    assert pending.params == {"path": "a.b"}

    fake_rest._raise_on_get_wiki = None
    fake_rest.existing.add("a.b")
    fake_rest._wikis["a.b"] = "# Docs"
    # retry_pending's dispatch must not require an `executor` at all for an
    # operation that never declared one — getwikifrom's first parameter is
    # `catalog_rest`, not `executor`.
    assert operations.retry_pending(executor, "k", catalog_rest=fake_rest) == "# Docs"


def test_gettagsfrom_returns_the_tags() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"}, tags={"a.b": ["pii", "reviewed"]})

    tags = operations.gettagsfrom(fake_rest, "a.b", idempotency_key="k")

    assert tags == ["pii", "reviewed"]


def test_gettagsfrom_returns_empty_list_when_entity_has_no_tags() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"})

    assert operations.gettagsfrom(fake_rest, "a.b", idempotency_key="k") == []


def test_gettagsfrom_raises_when_path_does_not_exist() -> None:
    fake_rest = FakeCatalogRest()

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.gettagsfrom(fake_rest, "a.missing", idempotency_key="k")


def test_gettagsfrom_engine_starting_is_remembered_and_retryable(executor) -> None:
    fake_rest = FakeCatalogRest(
        raise_on_get_tags=EngineStartingError("stalled", idempotency_key="k")
    )

    with pytest.raises(EngineStartingError):
        operations.gettagsfrom(fake_rest, "a.b", idempotency_key="k")

    pending = retry_state.get("k")
    assert pending is not None
    assert pending.operation == "gettagsfrom"
    assert pending.params == {"path": "a.b"}

    fake_rest._raise_on_get_tags = None
    fake_rest.existing.add("a.b")
    fake_rest._tags["a.b"] = ["pii"]
    assert operations.retry_pending(executor, "k", catalog_rest=fake_rest) == ["pii"]


def test_assignwikito_sets_the_wiki() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"})

    operations.assignwikito(fake_rest, "a.b", "# New docs", idempotency_key="k")

    assert fake_rest._wikis["a.b"] == "# New docs"


def test_assignwikito_raises_when_path_does_not_exist() -> None:
    fake_rest = FakeCatalogRest()

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.assignwikito(fake_rest, "a.missing", "text", idempotency_key="k")


def test_assignwikito_engine_starting_is_remembered_and_retryable(executor) -> None:
    fake_rest = FakeCatalogRest(
        existing={"a.b"}, raise_on_set_wiki=EngineStartingError("stalled", idempotency_key="k")
    )

    with pytest.raises(EngineStartingError):
        operations.assignwikito(fake_rest, "a.b", "# New docs", idempotency_key="k")

    pending = retry_state.get("k")
    assert pending is not None
    assert pending.operation == "assignwikito"
    assert pending.params == {"path": "a.b", "text": "# New docs"}

    fake_rest._raise_on_set_wiki = None
    operations.retry_pending(executor, "k", catalog_rest=fake_rest)
    assert fake_rest._wikis["a.b"] == "# New docs"


def test_assigntagsto_sets_the_tags() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"})

    operations.assigntagsto(fake_rest, "a.b", ["pii", "reviewed"], idempotency_key="k")

    assert fake_rest._tags["a.b"] == ["pii", "reviewed"]


def test_assigntagsto_raises_when_path_does_not_exist() -> None:
    fake_rest = FakeCatalogRest()

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.assigntagsto(fake_rest, "a.missing", ["pii"], idempotency_key="k")


def test_assigntagsto_engine_starting_is_remembered_and_retryable(executor) -> None:
    fake_rest = FakeCatalogRest(
        existing={"a.b"}, raise_on_set_tags=EngineStartingError("stalled", idempotency_key="k")
    )

    with pytest.raises(EngineStartingError):
        operations.assigntagsto(fake_rest, "a.b", ["pii"], idempotency_key="k")

    pending = retry_state.get("k")
    assert pending is not None
    assert pending.operation == "assigntagsto"
    assert pending.params == {"path": "a.b", "tags": ["pii"]}

    fake_rest._raise_on_set_tags = None
    operations.retry_pending(executor, "k", catalog_rest=fake_rest)
    assert fake_rest._tags["a.b"] == ["pii"]


def test_deletetags_removes_only_the_specified_tags() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"}, tags={"a.b": ["pii", "reviewed", "gold"]})

    operations.deletetags(fake_rest, "a.b", ["pii", "reviewed"], idempotency_key="k")

    assert fake_rest._tags["a.b"] == ["gold"]


def test_deletetags_raises_when_path_does_not_exist() -> None:
    fake_rest = FakeCatalogRest()

    with pytest.raises(CatalogOperationError, match="does not exist"):
        operations.deletetags(fake_rest, "a.missing", ["pii"], idempotency_key="k")


def test_deletetags_engine_starting_is_remembered_and_retryable(executor) -> None:
    fake_rest = FakeCatalogRest(
        existing={"a.b"},
        tags={"a.b": ["pii", "gold"]},
        raise_on_set_tags=EngineStartingError("stalled", idempotency_key="k"),
    )

    with pytest.raises(EngineStartingError):
        operations.deletetags(fake_rest, "a.b", ["pii"], idempotency_key="k")

    pending = retry_state.get("k")
    assert pending is not None
    assert pending.operation == "deletetags"
    assert pending.params == {"path": "a.b", "tags": ["pii"]}

    fake_rest._raise_on_set_tags = None
    operations.retry_pending(executor, "k", catalog_rest=fake_rest)
    assert fake_rest._tags["a.b"] == ["gold"]
