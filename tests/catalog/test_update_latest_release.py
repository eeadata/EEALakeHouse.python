"""update_latest_release: a view at the dataflow's root for every table in a version."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from eea_datalakehouse.catalog import retry_state
from eea_datalakehouse.catalog.client import Catalog
from eea_datalakehouse.catalog.session import (
    CatalogCommitError,
    CatalogSession,
    CatalogSessionError,
)
from eea_datalakehouse.catalog.sql import SqlResult

from .conftest import FakeCatalogRest
from .test_createversion import MiniDremio, _unquote

DATAFLOW = "catalog.water.surface.bod_rivers"
VERSIONS = f"{DATAFLOW}.versions"
VERSION = f"{VERSIONS}.v1"

_CREATE_VIEW = re.compile(r"CREATE VIEW (?P<target>\S+) AS SELECT \* FROM (?P<source>\S+)")


class ViewDremio(MiniDremio):
    """`MiniDremio` that also applies ``CREATE VIEW``."""

    def execute(self, sql: str, *, idempotency_key: str | None = None) -> SqlResult:
        match = _CREATE_VIEW.search(sql)
        if not match:
            return super().execute(sql, idempotency_key=idempotency_key)
        self.statements.append(sql)
        target = _unquote(match["target"])
        if target in self.fail_on_create:
            raise self.fail_on_create[target]
        self.tables[target] = "VIEW"
        self.rest.existing.add(target)
        return SqlResult(row_count=1, job_id="job")


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", tmp_path / "state.json")


def _world(tables: dict[str, str] | None = None) -> tuple[FakeCatalogRest, ViewDremio]:
    folders = {
        "catalog",
        "catalog.water",
        "catalog.water.surface",
        DATAFLOW,
        VERSIONS,
        VERSION,
        f"{VERSION}.eu_sdg",
    }
    rest = FakeCatalogRest(existing=set(folders), folders=set(folders))
    dremio = ViewDremio(
        rest,
        tables
        if tables is not None
        else {
            f"{VERSION}.eu_sdg.monitoringsites": "TABLE",
            f"{VERSION}.eu_sdg.observations": "TABLE",
            f"{VERSION}.summary": "TABLE",
        },
    )
    return rest, dremio


def _session(rest: FakeCatalogRest, dremio: ViewDremio, context: str = DATAFLOW) -> CatalogSession:
    catalog = Catalog(
        "https://dremio.example.test",
        "pat",
        executor=dremio,  # type: ignore[arg-type]
        flight_executor=dremio,  # type: ignore[arg-type]
        catalog_rest=rest,  # type: ignore[arg-type]
    )
    return CatalogSession(catalog).use(context)


def _views(dremio: ViewDremio) -> set[str]:
    return {path for path, kind in dremio.tables.items() if kind == "VIEW"}


def test_creates_a_view_per_table_keeping_the_sub_folders() -> None:
    rest, dremio = _world()

    report = _session(rest, dremio).update_latest_release("v1").commit()

    assert _views(dremio) == {
        f"{DATAFLOW}.eu_sdg.monitoringsites",
        f"{DATAFLOW}.eu_sdg.observations",
        f"{DATAFLOW}.summary",
    }
    assert f"{DATAFLOW}.eu_sdg" in rest.folders
    assert any(
        'AS SELECT * FROM "catalog"."water"."surface"."bod_rivers"."versions"."v1"."summary"' in s
        for s in dremio.statements
    )
    assert report.succeeded == [
        f"create views under {DATAFLOW!r} for the tables in {VERSION!r}"
    ]
    assert report.notes == []


def test_views_in_the_version_are_skipped_and_reported() -> None:
    rest, dremio = _world({f"{VERSION}.t": "TABLE", f"{VERSION}.latest": "VIEW"})

    report = _session(rest, dremio).update_latest_release("v1").commit()

    assert _views(dremio) - {f"{VERSION}.latest"} == {f"{DATAFLOW}.t"}
    assert report.notes == [f"skipped '{VERSION}.latest': view — only tables get a view"]


@pytest.mark.parametrize(
    "context",
    ["catalog", "catalog.water.surface", VERSIONS, "catalog.water.surface.bod_rivers.versions.v1"],
)
def test_context_must_be_three_levels_below_catalog(context: str) -> None:
    rest, dremio = _world()
    session = _session(rest, dremio, context)

    with pytest.raises(CatalogSessionError, match="three levels below 'catalog'"):
        session.update_latest_release("v1")
    assert repr(session) == "CatalogSession(pending=0)"


def test_context_cleared_is_an_error() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio).set_context(None)

    with pytest.raises(CatalogSessionError, match="use\\(\\) it first"):
        session.update_latest_release("v1")


def test_missing_versions_folder_raises_at_once() -> None:
    rest, dremio = _world()
    rest.folders.discard(VERSIONS)

    with pytest.raises(CatalogSessionError, match=f"'{VERSIONS}' does not exist"):
        _session(rest, dremio).update_latest_release("v1")


def test_missing_version_folder_raises_at_once() -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogSessionError, match=f"'{VERSIONS}.v9' does not exist"):
        _session(rest, dremio).update_latest_release("v9")


@pytest.mark.parametrize(
    ("version_name", "message"),
    [("", "must not be empty"), ("  ", "must not be empty"), ("a.b", "single folder name"),
     ("a/b", "single folder name")],
)
def test_bad_version_name_raises_at_once(version_name: str, message: str) -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogSessionError, match=message):
        _session(rest, dremio).update_latest_release(version_name)


def test_an_existing_view_is_an_error_and_nothing_is_created() -> None:
    rest, dremio = _world()
    dremio.tables[f"{DATAFLOW}.summary"] = "VIEW"
    rest.existing.add(f"{DATAFLOW}.summary")

    with pytest.raises(CatalogCommitError, match="pass overwrite=True") as info:
        _session(rest, dremio).update_latest_release("v1").commit()

    assert info.value.rolled_back is True
    assert _views(dremio) == {f"{DATAFLOW}.summary"}
    assert f"{DATAFLOW}.eu_sdg" not in rest.existing


def test_overwrite_replaces_a_view_and_is_irreversible() -> None:
    rest, dremio = _world()
    dremio.tables[f"{DATAFLOW}.summary"] = "VIEW"
    rest.existing.add(f"{DATAFLOW}.summary")
    session = _session(rest, dremio)

    report = session.update_latest_release("v1", overwrite=True).commit()

    assert report.notes == [f"replaced view '{DATAFLOW}.summary'"]
    session.update_latest_release("v1", overwrite=True)
    session.set_tags("catalog.water.missing", ["x"])  # fails: path does not exist
    with pytest.raises(CatalogCommitError) as info:
        session.commit()
    assert info.value.rolled_back is False
    assert "replaced 3 existing view(s)" in info.value.unresolved[0]


def test_an_existing_table_is_never_replaced() -> None:
    rest, dremio = _world()
    dremio.tables[f"{DATAFLOW}.summary"] = "TABLE"
    rest.existing.add(f"{DATAFLOW}.summary")

    with pytest.raises(CatalogCommitError, match="is a table — a view never replaces one"):
        _session(rest, dremio).update_latest_release("v1", overwrite=True).commit()

    assert dremio.tables[f"{DATAFLOW}.summary"] == "TABLE"


def test_a_version_folder_named_draft_or_versions_is_refused() -> None:
    rest, dremio = _world({f"{VERSION}.draft.t": "TABLE"})
    rest.existing.add(f"{VERSION}.draft")
    rest.folders.add(f"{VERSION}.draft")

    with pytest.raises(CatalogCommitError, match="would put its view inside"):
        _session(rest, dremio).update_latest_release("v1").commit()


def test_an_empty_version_is_an_error() -> None:
    rest, dremio = _world({})

    with pytest.raises(CatalogCommitError, match="has no tables"):
        _session(rest, dremio).update_latest_release("v1").commit()


def test_a_failure_partway_removes_the_views_and_folders_it_created() -> None:
    rest, dremio = _world()
    dremio.fail_on_create[f"{DATAFLOW}.summary"] = RuntimeError("boom")

    with pytest.raises(CatalogCommitError, match="boom"):
        _session(rest, dremio).update_latest_release("v1").commit()

    assert _views(dremio) == set()
    assert f"{DATAFLOW}.eu_sdg" not in rest.existing


def test_rollback_drops_the_views_and_folders() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)

    session.update_latest_release("v1")
    session.set_tags("catalog.water.missing", ["x"])
    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is True
    assert _views(dremio) == set()
    assert f"{DATAFLOW}.eu_sdg" not in rest.existing
    assert f"{VERSION}.eu_sdg.monitoringsites" in dremio.tables


def test_slash_context_is_accepted() -> None:
    rest, dremio = _world()

    _session(rest, dremio, "catalog/water/surface/bod_rivers").update_latest_release("v1").commit()

    assert f"{DATAFLOW}.summary" in _views(dremio)
