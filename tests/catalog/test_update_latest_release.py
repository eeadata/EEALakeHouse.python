"""update_latest_release: a view at the dataflow's root for every table in a version."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from eea_datalakehouse.catalog import retry_state
from eea_datalakehouse.catalog.client import Catalog
from eea_datalakehouse.catalog.session import (
    PUBLIC_TAG,
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
        f"{DATAFLOW}.draft",
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


@pytest.mark.parametrize("context", ["catalog", "catalog.water", "catalog.water.surface"])
def test_a_context_above_the_area_is_refused_whatever_its_level(context: str) -> None:
    rest, dremio = _world()
    session = _session(rest, dremio, context)

    with pytest.raises(CatalogSessionError, match=f"'{context}' is not an Area folder"):
        session.update_latest_release("v1")
    assert repr(session) == "CatalogSession(pending=0)"


def test_an_area_at_any_level_is_accepted() -> None:
    area = "catalog.flow"
    rest, dremio = _world({f"{area}.versions.v1.t": "TABLE"})
    for folder in (area, f"{area}.draft", f"{area}.versions", f"{area}.versions.v1"):
        rest.existing.add(folder)
        rest.folders.add(folder)

    _session(rest, dremio, area).update_latest_release("v1").commit()

    assert f"{area}.t" in _views(dremio)


def test_a_dataflow_deeper_than_three_levels_is_accepted() -> None:
    deep = f"{DATAFLOW}.sub"
    rest, dremio = _world({f"{deep}.versions.v1.t": "TABLE"})
    for folder in (deep, f"{deep}.draft", f"{deep}.versions", f"{deep}.versions.v1"):
        rest.existing.add(folder)
        rest.folders.add(folder)

    _session(rest, dremio, deep).update_latest_release("v1").commit()

    assert f"{deep}.t" in _views(dremio)


def test_a_context_inside_the_versions_folder_has_no_versions_of_its_own() -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogSessionError, match="no 'draft' and no 'versions' folder"):
        _session(rest, dremio, VERSION).update_latest_release("v1")


def test_context_cleared_is_an_error() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio).set_context(None)

    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.update_latest_release("v1")


@pytest.mark.parametrize(
    ("removed", "message"),
    [
        (["versions"], "has no 'versions' folder just below it"),
        (["draft"], "has no 'draft' folder just below it"),
        (["draft", "versions"], "has no 'draft' and no 'versions' folder just below it"),
    ],
)
def test_the_area_needs_draft_and_versions_just_below_it(
    removed: list[str], message: str
) -> None:
    rest, dremio = _world()
    for name in removed:
        rest.folders.discard(f"{DATAFLOW}.{name}")

    with pytest.raises(CatalogSessionError, match=message) as info:
        _session(rest, dremio).update_latest_release("v1")
    assert f"'{DATAFLOW}' is not an Area folder" in str(info.value)


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


def test_without_overwrite_an_existing_view_is_an_error_and_nothing_is_created() -> None:
    rest, dremio = _world()
    dremio.tables[f"{DATAFLOW}.summary"] = "VIEW"
    rest.existing.add(f"{DATAFLOW}.summary")

    with pytest.raises(CatalogCommitError, match="pass overwrite=True") as info:
        _session(rest, dremio).update_latest_release("v1", overwrite=False).commit()

    assert info.value.rolled_back is True
    assert _views(dremio) == {f"{DATAFLOW}.summary"}
    assert f"{DATAFLOW}.eu_sdg" not in rest.existing


def test_an_existing_view_is_dropped_and_recreated_by_default() -> None:
    rest, dremio = _world()
    dremio.tables[f"{DATAFLOW}.summary"] = "VIEW"
    rest.existing.add(f"{DATAFLOW}.summary")
    session = _session(rest, dremio)

    report = session.update_latest_release("v1").commit()

    assert report.notes == [f"replaced view '{DATAFLOW}.summary'"]
    target = '"catalog"."water"."surface"."bod_rivers"."summary"'
    statements = dremio.statements
    drop_sql = f"DROP VIEW IF EXISTS {target}"
    drop = next(i for i, s in enumerate(statements) if s.startswith(drop_sql))
    create = next(i for i, s in enumerate(statements) if s.startswith(f"CREATE VIEW {target}"))
    assert drop < create
    assert _views(dremio) >= {f"{DATAFLOW}.summary", f"{DATAFLOW}.eu_sdg.observations"}


def test_a_replaced_view_makes_the_step_irreversible() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.update_latest_release("v1").commit()  # the views now exist

    session.update_latest_release("v1")
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
        _session(rest, dremio).update_latest_release("v1").commit()

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


# -- #public tag ------------------------------------------------------------------


def test_each_view_is_tagged_public() -> None:
    rest, dremio = _world()

    _session(rest, dremio).update_latest_release("v1").commit()

    for view in _views(dremio):
        assert rest.get_tags(view) == [PUBLIC_TAG]


def _with_older_version(rest: FakeCatalogRest, dremio: ViewDremio) -> str:
    older = f"{VERSIONS}.v0.summary"
    rest.existing.add(f"{VERSIONS}.v0")
    rest.folders.add(f"{VERSIONS}.v0")
    dremio.tables[older] = "TABLE"
    rest.existing.add(older)
    rest.set_tags(older, [PUBLIC_TAG, "reviewed"])
    rest.set_tags(f"{VERSION}.summary", [PUBLIC_TAG])
    return older


def test_the_tag_is_removed_from_everything_under_versions() -> None:
    rest, dremio = _world()
    older = _with_older_version(rest, dremio)

    report = _session(rest, dremio).update_latest_release("v1").commit()

    assert rest.get_tags(older) == ["reviewed"]
    assert rest.get_tags(f"{VERSION}.summary") == []
    assert f"removed '{PUBLIC_TAG}' from 2 table(s)/view(s) under '{VERSIONS}'" in report.notes


def test_rollback_puts_the_version_tags_back() -> None:
    rest, dremio = _world()
    older = _with_older_version(rest, dremio)
    session = _session(rest, dremio)

    session.update_latest_release("v1")
    session.set_tags("catalog.water.missing", ["x"])
    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is True
    assert rest.get_tags(older) == [PUBLIC_TAG, "reviewed"]
    assert rest.get_tags(f"{VERSION}.summary") == [PUBLIC_TAG]
