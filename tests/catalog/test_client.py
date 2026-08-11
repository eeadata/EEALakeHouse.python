from __future__ import annotations

from pathlib import Path

import pytest

from eea_datalakehouse.catalog import Catalog, retry_state
from eea_datalakehouse.catalog.rest import CatalogRestClient
from eea_datalakehouse.catalog.sql import FlightSqlExecutor, RestSqlExecutor

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


def test_without_injected_executor_builds_a_real_rest_executor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Fixed, not env-var-driven — even EEA_CATALOG_TRANSPORT=flight must not
    # change self._executor (only datacopy/datamove go over Flight).
    monkeypatch.setenv("EEA_CATALOG_TRANSPORT", "flight")

    catalog = Catalog(BASE_URL, "pat")

    assert isinstance(catalog._executor, RestSqlExecutor)


def test_without_injected_flight_executor_builds_a_real_flight_executor() -> None:
    catalog = Catalog(BASE_URL, "pat")

    assert isinstance(catalog._flight_executor, FlightSqlExecutor)
    assert catalog._flight_executor._username is None


def test_username_is_threaded_through_to_the_flight_executor() -> None:
    catalog = Catalog(BASE_URL, "pat", username="alice")

    assert catalog._flight_executor._username == "alice"


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


def test_datacopy_overwrite_delegates_correctly() -> None:
    # datacopy always goes over the flight executor, not the REST one.
    fake = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "src"}],
            [{"TABLE_NAME": "dst"}],
            [{"TABLE_TYPE": "TABLE"}],
        ]
    )
    catalog = Catalog(BASE_URL, "pat", flight_executor=fake, catalog_rest=FakeCatalogRest())

    catalog.datacopy("a.src", "a.dst", overwrite=True, idempotency_key="k")

    assert fake.statements == [
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'src'",
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'dst'",
        'SELECT "TABLE_TYPE" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'dst'",
        'DROP TABLE IF EXISTS "a"."dst"',
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
    ]


def test_datamove_delegates_correctly() -> None:
    # datamove always goes over the flight executor, not the REST one.
    # Three calls, different query shapes: the initial source-existence
    # check, the target-doesn't-exist-yet check, and the post-DROP
    # verification that the target now exists.
    fake = FakeExecutor(rows_sequence=[[{"TABLE_NAME": "src"}], [], [{"TABLE_NAME": "dst"}]])
    catalog = Catalog(BASE_URL, "pat", flight_executor=fake, catalog_rest=FakeCatalogRest())

    catalog.datamove("a.src", "a.dst", entry_type="TABLE", idempotency_key="k")

    assert fake.statements == [
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'src'",
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'dst'",
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP TABLE "a"."src"',
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'dst'",
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


def test_retry_pending_routes_datamove_back_through_the_flight_executor() -> None:
    rest_fake = FakeExecutor()  # must stay untouched — datamove never uses REST
    flight_fake = FakeExecutor(rows_sequence=[[{"TABLE_NAME": "src"}], [], [{"TABLE_NAME": "dst"}]])
    catalog = Catalog(
        BASE_URL,
        "pat",
        executor=rest_fake,
        flight_executor=flight_fake,
        catalog_rest=FakeCatalogRest(),
    )
    retry_state.record(
        "k",
        "datamove",
        "a.dst",
        "earlier failure",
        params={
            "source_path": "a.src",
            "target_path": "a.dst",
            "entry_type": "TABLE",
            "overwrite": False,
            "create_target_folder": False,
        },
    )

    catalog.retry_pending("k")

    assert flight_fake.statements == [
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'src'",
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'dst'",
        'CREATE TABLE "a"."dst" AS SELECT * FROM "a"."src"',
        'DROP TABLE "a"."src"',
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        "WHERE \"TABLE_SCHEMA\" = 'a' AND \"TABLE_NAME\" = 'dst'",
    ]
    assert rest_fake.statements == []


def test_retry_pending_raises_when_nothing_is_pending() -> None:
    catalog = Catalog(BASE_URL, "pat")

    with pytest.raises(KeyError):
        catalog.retry_pending("no-such-key")


def test_getwikifrom_delegates_to_operations() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"}, wikis={"a.b": "# Docs"})
    catalog = Catalog(BASE_URL, "pat", executor=FakeExecutor(), catalog_rest=fake_rest)

    text = catalog.getwikifrom("a.b", idempotency_key="k")

    assert text == "# Docs"


def test_gettagsfrom_delegates_to_operations() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"}, tags={"a.b": ["pii"]})
    catalog = Catalog(BASE_URL, "pat", executor=FakeExecutor(), catalog_rest=fake_rest)

    tags = catalog.gettagsfrom("a.b", idempotency_key="k")

    assert tags == ["pii"]


def test_assignwikito_delegates_to_operations() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"})
    catalog = Catalog(BASE_URL, "pat", executor=FakeExecutor(), catalog_rest=fake_rest)

    catalog.assignwikito("a.b", "# New docs", idempotency_key="k")

    assert fake_rest._wikis["a.b"] == "# New docs"


def test_assigntagsto_delegates_to_operations() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"})
    catalog = Catalog(BASE_URL, "pat", executor=FakeExecutor(), catalog_rest=fake_rest)

    catalog.assigntagsto("a.b", ["pii"], idempotency_key="k")

    assert fake_rest._tags["a.b"] == ["pii"]


def test_deletetags_delegates_to_operations() -> None:
    fake_rest = FakeCatalogRest(existing={"a.b"}, tags={"a.b": ["pii", "gold"]})
    catalog = Catalog(BASE_URL, "pat", executor=FakeExecutor(), catalog_rest=fake_rest)

    catalog.deletetags("a.b", ["pii"], idempotency_key="k")

    assert fake_rest._tags["a.b"] == ["gold"]


def test_repr_does_not_expose_the_token() -> None:
    catalog = Catalog(BASE_URL, "super-secret-pat")

    rendered = repr(catalog)

    assert "super-secret-pat" not in rendered


class _Closeable:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def test_close_delegates_to_executor_and_catalog_rest_and_is_idempotent() -> None:
    executor = _Closeable()
    catalog_rest = _Closeable()
    catalog = Catalog(BASE_URL, "pat", executor=executor, catalog_rest=catalog_rest)

    catalog.close()
    catalog.close()  # second call is a no-op

    assert executor.closed == 1
    assert catalog_rest.closed == 1


def test_close_ignores_objects_without_a_close_method() -> None:
    catalog = Catalog(BASE_URL, "pat", executor=FakeExecutor(), catalog_rest=FakeCatalogRest())

    catalog.close()  # must not raise, even though the fakes have no close()


def test_context_manager_closes_on_exit() -> None:
    executor = _Closeable()

    with Catalog(BASE_URL, "pat", executor=executor, catalog_rest=_Closeable()) as catalog:
        assert isinstance(catalog, Catalog)

    assert executor.closed == 1


def test_close_all_open_catalogs_closes_every_registered_instance() -> None:
    from eea_datalakehouse.catalog.client import _close_all_open_catalogs

    a_executor, b_executor = _Closeable(), _Closeable()
    a = Catalog(BASE_URL, "pat", executor=a_executor, catalog_rest=_Closeable())
    b = Catalog(BASE_URL, "pat", executor=b_executor, catalog_rest=_Closeable())

    _close_all_open_catalogs()

    assert a_executor.closed == 1
    assert b_executor.closed == 1
    assert a._closed is True
    assert b._closed is True
