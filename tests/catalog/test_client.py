from __future__ import annotations

from pathlib import Path

import pytest

from eea_datalakehouse.catalog import Catalog, retry_state
from eea_datalakehouse.catalog.rest import CatalogRestClient
from eea_datalakehouse.catalog.sql import RestSqlExecutor

from .conftest import FakeCatalogRest, FakeExecutor

BASE_URL = "https://dremio.example.test"


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", tmp_path / "state.json")


def test_injected_executor_is_used_directly() -> None:
    fake = FakeExecutor()
    catalog = Catalog(BASE_URL, "pat", executor=fake)

    catalog.draft2version("a.draft", "a.v1", idempotency_key="k")

    assert fake.statements == ['CREATE TABLE "a"."v1" AS SELECT * FROM "a"."draft"']


def test_without_injected_executor_resolves_via_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EEA_CATALOG_TRANSPORT", raising=False)

    catalog = Catalog(BASE_URL, "pat")

    assert isinstance(catalog._executor, RestSqlExecutor)


def test_without_injected_catalog_rest_builds_a_real_one() -> None:
    catalog = Catalog(BASE_URL, "pat")

    assert isinstance(catalog._catalog_rest, CatalogRestClient)


def test_table2view_delegates_to_operations() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "source"}])
    fake_rest = FakeCatalogRest(existing={"a"})
    catalog = Catalog(BASE_URL, "pat", executor=fake, catalog_rest=fake_rest)

    catalog.table2view("a.view", "a.source", idempotency_key="k")

    assert fake.statements == [
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'source'",
        'DROP TABLE IF EXISTS "a"."view"',
        'CREATE VIEW "a"."view" AS SELECT * FROM "a"."source"',
    ]


def test_table2view_skips_folder_check_when_disabled() -> None:
    fake = FakeExecutor(rows=[{"TABLE_NAME": "source"}])
    catalog = Catalog(BASE_URL, "pat", executor=fake)
    # create_target_folder=False means self._catalog_rest (a real, unreachable
    # -in-tests CatalogRestClient here) is never consulted at all.

    catalog.table2view("a.view", "a.source", create_target_folder=False, idempotency_key="k")

    assert fake.statements[1:] == [
        'DROP TABLE IF EXISTS "a"."view"',
        'CREATE VIEW "a"."view" AS SELECT * FROM "a"."source"',
    ]


def test_deleteview_delegates_to_operations() -> None:
    fake = FakeExecutor()
    catalog = Catalog(BASE_URL, "pat", executor=fake)

    catalog.deleteview("a.view", idempotency_key="k")

    assert fake.statements == ['DROP VIEW IF EXISTS "a"."view"']


def test_datacopy_replace_mode_delegates_correctly() -> None:
    fake = FakeExecutor()
    catalog = Catalog(BASE_URL, "pat", executor=fake)

    catalog.datacopy("a.src", "a.dst", mode="replace", idempotency_key="k")

    assert fake.statements == [
        'DROP TABLE IF EXISTS "a"."dst"',
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
    ]


def test_gettablesfrom_delegates_and_returns_full_paths() -> None:
    fake = FakeExecutor(rows=[{"TABLE_SCHEMA": "bwd.versions", "TABLE_NAME": "assessments"}])
    catalog = Catalog(BASE_URL, "pat", executor=fake)

    paths = catalog.gettablesfrom("bwd.versions", idempotency_key="k1")

    assert paths == ["bwd.versions.assessments"]


def test_gettableitemsfrom_delegates_and_returns_table_info() -> None:
    fake = FakeExecutor(
        rows_sequence=[
            [{"COLUMN_NAME": "id", "DATA_TYPE": "INTEGER"}],
            [{"row_count": 7}],
        ]
    )
    catalog = Catalog(BASE_URL, "pat", executor=fake)

    result = catalog.gettableitemsfrom("bwd.versions.v1", idempotency_key="k2")

    assert result.schema == {"id": "INTEGER"}
    assert result.row_count == 7
    assert fake.statements[-1] == 'SELECT COUNT(*) AS "row_count" FROM "bwd"."versions"."v1"'


def test_retry_pending_delegates_to_operations() -> None:
    fake = FakeExecutor()
    catalog = Catalog(BASE_URL, "pat", executor=fake)
    retry_state.record(
        "k",
        "publishversion",
        "a.consumer",
        "earlier failure",
        params={"consumer_view_path": "a.consumer", "version_path": "a.v2"},
    )

    catalog.retry_pending("k")

    assert fake.statements == [
        'CREATE OR REPLACE VIEW "a"."consumer" AS SELECT * FROM "a"."v2"',
    ]


def test_repr_does_not_expose_the_token() -> None:
    catalog = Catalog(BASE_URL, "super-secret-pat")

    rendered = repr(catalog)

    assert "super-secret-pat" not in rendered
