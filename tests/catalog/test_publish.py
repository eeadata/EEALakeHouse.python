"""publish: copy the Area's #public views into the public bucket as tables."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

from eea_datalakehouse.catalog import retry_state
from eea_datalakehouse.catalog.client import Catalog
from eea_datalakehouse.catalog.session import (
    PUBLIC_ROOT,
    PUBLIC_TAG,
    CatalogCommitError,
    CatalogSession,
    CatalogSessionError,
)
from eea_datalakehouse.catalog.sql import SqlResult

from .conftest import FakeCatalogRest
from .test_createversion import _unquote
from .test_update_latest_release import ViewDremio

AREA = "catalog.water.surface.bod_rivers"
PUBLIC = f"{PUBLIC_ROOT}.{AREA}"
SQL_PUBLIC = '"public_s3"."dh-prod-data-public"."catalog"."water"."surface"."bod_rivers"'

_PARQUET_CTAS = re.compile(
    r"CREATE TABLE (?P<target>\S+) (?:PARTITION BY \((?P<partition>[^)]*)\) )?"
    r"STORE AS \(type => 'parquet'\) AS SELECT \* FROM \S+"
)
_COUNT = re.compile(r'SELECT COUNT\(\*\) AS "row_count" FROM (?P<table>\S+)')
_COLUMNS = re.compile(
    r'SELECT "COLUMN_NAME" FROM INFORMATION_SCHEMA."COLUMNS" '
    r"WHERE \"TABLE_SCHEMA\" = '(?P<schema>[^']*)' AND \"TABLE_NAME\" = '(?P<name>[^']*)' "
    r'ORDER BY "ORDINAL_POSITION"'
)
COLUMNS = ["site_id", "Year", "country", "value"]


class PublishDremio(ViewDremio):
    """`ViewDremio` that also applies publish's Parquet ``CREATE TABLE`` and
    answers its ``COUNT(*)`` check (3 rows; `unreadable` tables raise) and
    its column lookups (`COLUMNS` for every view unless `columns` says otherwise).
    `partitions` records each created table's ``PARTITION BY`` list."""

    def __init__(self, rest: FakeCatalogRest, tables: dict[str, str]) -> None:
        super().__init__(rest, tables)
        self.unreadable: set[str] = set()
        self.columns: dict[str, list[str]] = {}
        self.partitions: dict[str, str | None] = {}

    def fetch_all(self, sql: str, *, idempotency_key: str | None = None) -> list[dict[str, Any]]:
        if match := _COLUMNS.fullmatch(sql):
            self.statements.append(sql)
            view = f"{match['schema']}.{match['name']}"
            return [{"COLUMN_NAME": c} for c in self.columns.get(view, COLUMNS)]
        match = _COUNT.fullmatch(sql)
        if not match:
            return super().fetch_all(sql, idempotency_key=idempotency_key)
        self.statements.append(sql)
        table = _unquote(match["table"])
        if table not in self.tables or table in self.unreadable:
            raise RuntimeError(f"Object '{table}' not found")
        return [{"row_count": 3}]

    def execute(self, sql: str, *, idempotency_key: str | None = None) -> SqlResult:
        match = _PARQUET_CTAS.fullmatch(sql)
        if not match:
            return super().execute(sql, idempotency_key=idempotency_key)
        self.statements.append(sql)
        target = _unquote(match["target"])
        if target in self.fail_on_create:
            raise self.fail_on_create.pop(target)  # once: a rollback's restore then succeeds
        self.tables[target] = "TABLE"
        self.rest.existing.add(target)
        self.partitions[target] = match["partition"]
        return SqlResult(row_count=1, job_id="job")


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", tmp_path / "state.json")


def _world(
    views: dict[str, list[str]] | None = None,
) -> tuple[FakeCatalogRest, PublishDremio]:
    """An Area with a version, a draft view, and `views` (path below the Area -> tags)."""
    folders = {
        "catalog",
        "catalog.water",
        "catalog.water.surface",
        AREA,
        f"{AREA}.draft",
        f"{AREA}.versions",
        f"{AREA}.versions.v1",
        f"{AREA}.reference",
        f"{AREA}.eu_sdg",
    }
    if views is None:
        views = {"eu_sdg.monitoringsites": [PUBLIC_TAG], "summary": [PUBLIC_TAG, "reviewed"]}
    rest = FakeCatalogRest(existing=set(folders), folders=set(folders))
    tables = {
        f"{AREA}.versions.v1.summary": "TABLE",
        f"{AREA}.versions.v1.latest": "VIEW",
        f"{AREA}.draft.wip": "VIEW",
        f"{AREA}.reference.codes": "VIEW",
        **{f"{AREA}.{name}": "VIEW" for name in views},
    }
    dremio = PublishDremio(rest, tables)
    rest.views = {path for path, kind in tables.items() if kind == "VIEW"}
    for name, tags in views.items():
        rest.set_tags(f"{AREA}.{name}", tags)
    for path in (f"{AREA}.versions.v1.latest", f"{AREA}.draft.wip", f"{AREA}.reference.codes"):
        rest.set_tags(path, [PUBLIC_TAG])  # must still be ignored
    return rest, dremio


def _session(rest: FakeCatalogRest, dremio: PublishDremio, context: str = AREA) -> CatalogSession:
    catalog = Catalog(
        "https://dremio.example.test",
        "pat",
        executor=dremio,  # type: ignore[arg-type]
        flight_executor=dremio,  # type: ignore[arg-type]
        catalog_rest=rest,  # type: ignore[arg-type]
    )
    return CatalogSession(catalog).use(context)


def _published(dremio: PublishDremio) -> set[str]:
    return {path for path in dremio.tables if path.startswith(f"{PUBLIC_ROOT}.")}


def test_copies_each_public_view_to_the_same_path_in_the_public_bucket() -> None:
    rest, dremio = _world()

    report = _session(rest, dremio).publish().commit()

    assert _published(dremio) == {f"{PUBLIC}.eu_sdg.monitoringsites", f"{PUBLIC}.summary"}
    assert dremio.tables[f"{PUBLIC}.summary"] == "TABLE"
    drop = f'DROP TABLE IF EXISTS {SQL_PUBLIC}."summary"'
    create = (
        f"CREATE TABLE {SQL_PUBLIC}.\"summary\" STORE AS (type => 'parquet') "
        'AS SELECT * FROM "catalog"."water"."surface"."bod_rivers"."summary"'
    )
    refresh = f'ALTER TABLE {SQL_PUBLIC}."summary" REFRESH METADATA AUTO PROMOTION'
    count = f'SELECT COUNT(*) AS "row_count" FROM {SQL_PUBLIC}."summary"'
    order = [dremio.statements.index(sql) for sql in (drop, create, refresh, count)]
    assert order == sorted(order)
    assert report.succeeded == [f"publish the {PUBLIC_TAG} views of {AREA!r} to {PUBLIC!r}"]
    assert f"copied '{PUBLIC}.summary' from '{AREA}.summary' (3 rows)" in report.notes


def test_views_without_the_tag_are_skipped_and_reported() -> None:
    rest, dremio = _world({"summary": [PUBLIC_TAG], "scratch": ["reviewed"]})

    report = _session(rest, dremio).publish().commit()

    assert _published(dremio) == {f"{PUBLIC}.summary"}
    assert f"skipped '{AREA}.scratch': no {PUBLIC_TAG} tag" in report.notes


def test_draft_versions_and_reference_are_ignored_even_when_tagged() -> None:
    rest, dremio = _world()

    report = _session(rest, dremio).publish().commit()

    assert _published(dremio) == {f"{PUBLIC}.eu_sdg.monitoringsites", f"{PUBLIC}.summary"}
    for folder in ("draft", "versions", "reference"):
        assert not any(f".{folder}." in note for note in report.notes)


def test_no_public_view_is_an_error() -> None:
    rest, dremio = _world({"summary": []})

    with pytest.raises(CatalogCommitError, match="no views tagged '#public'"):
        _session(rest, dremio).publish().commit()


def test_an_existing_table_is_dropped_and_copied_again_by_default() -> None:
    rest, dremio = _world()
    dremio.tables[f"{PUBLIC}.summary"] = "TABLE"
    session = _session(rest, dremio)

    report = session.publish().commit()

    assert f"replaced '{PUBLIC}.summary' from '{AREA}.summary' (3 rows)" in report.notes
    target = f'{SQL_PUBLIC}."summary"'
    drop = next(
        i for i, s in enumerate(dremio.statements) if s.startswith(f"DROP TABLE IF EXISTS {target}")
    )
    create = next(
        i for i, s in enumerate(dremio.statements) if s.startswith(f"CREATE TABLE {target}")
    )
    assert drop < create


def test_without_overwrite_an_existing_table_is_an_error_and_nothing_is_copied() -> None:
    rest, dremio = _world()
    dremio.tables[f"{PUBLIC}.summary"] = "TABLE"

    with pytest.raises(CatalogCommitError, match="pass overwrite=True") as info:
        _session(rest, dremio).publish(overwrite=False).commit()

    assert info.value.rolled_back is True
    assert _published(dremio) == {f"{PUBLIC}.summary"}
    assert not any(s.startswith("CREATE TABLE") for s in dremio.statements)


def test_a_replaced_table_makes_the_step_irreversible() -> None:
    rest, dremio = _world()
    dremio.tables[f"{PUBLIC}.summary"] = "TABLE"
    session = _session(rest, dremio)

    session.publish()
    session.set_tags("catalog.water.missing", ["x"])
    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is False
    assert "replaced 1 published table(s)" in info.value.unresolved[0]


def test_a_failure_partway_drops_what_was_copied() -> None:
    rest, dremio = _world()
    dremio.fail_on_create[f"{PUBLIC}.summary"] = RuntimeError("boom")

    with pytest.raises(CatalogCommitError, match="boom") as info:
        _session(rest, dremio).publish().commit()

    assert _published(dremio) == set()
    assert f"publishing '{AREA}.summary' to '{PUBLIC}.summary' failed" in str(info.value)
    assert "rolled back: every table it touched was dropped" in str(info.value)


def test_rollback_drops_the_copies() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)

    session.publish()
    session.set_tags("catalog.water.missing", ["x"])
    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is True
    assert _published(dremio) == set()


def test_the_context_must_be_a_dataflow_folder() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio, "catalog.water")

    with pytest.raises(CatalogSessionError, match="'catalog.water' is not a dataflow folder"):
        session.publish()
    assert repr(session) == "CatalogSession(pending=0)"


def _unset(rest: FakeCatalogRest, dremio: PublishDremio) -> CatalogSession:
    """A session no `use` has been run on: its context is the catalog root."""
    catalog = _session(rest, dremio)._catalog  # noqa: SLF001 — reuse the fake wiring
    return CatalogSession(catalog)


def test_without_use_or_path_is_an_error() -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogSessionError, match=r"run %catalog use\(\.\.\.\) on the dataflow"):
        _unset(rest, dremio).publish()
    with pytest.raises(CatalogSessionError, match="or pass path="):
        _unset(rest, dremio).set_context(None).publish()


def test_path_points_at_the_dataflow_without_use() -> None:
    rest, dremio = _world()

    report = _unset(rest, dremio).publish(AREA).commit()

    assert _published(dremio) == {f"{PUBLIC}.eu_sdg.monitoringsites", f"{PUBLIC}.summary"}
    assert report.succeeded == [f"publish the {PUBLIC_TAG} views of {AREA!r} to {PUBLIC!r}"]


@pytest.mark.parametrize("path", ["bod_rivers", "./bod_rivers", "catalog/water/surface/bod_rivers"])
def test_path_resolves_like_any_path(path: str) -> None:
    rest, dremio = _world()

    _session(rest, dremio, "catalog.water.surface").publish(path=path).commit()

    assert f"{PUBLIC}.summary" in _published(dremio)


def test_path_overrides_the_context() -> None:
    rest, dremio = _world()

    _session(rest, dremio, "catalog.water").publish(path=AREA).commit()

    assert f"{PUBLIC}.summary" in _published(dremio)


@pytest.mark.parametrize("path", ["", "  ", "/", 42])
def test_a_blank_path_is_refused(path: object) -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogSessionError, match="path must be a catalog folder path"):
        _session(rest, dremio).publish(path=path)  # type: ignore[arg-type]


def test_a_dataflow_without_a_reference_folder_is_refused() -> None:
    rest, dremio = _world()
    rest.folders.discard(f"{AREA}.reference")

    with pytest.raises(CatalogSessionError, match="has no 'reference' folder just below it"):
        _session(rest, dremio).publish()


def test_a_references_folder_counts_and_is_never_published() -> None:
    rest, dremio = _world()
    rest.folders.discard(f"{AREA}.reference")
    for folder in (f"{AREA}.references",):
        rest.folders.add(folder)
        rest.existing.add(folder)
    dremio.tables[f"{AREA}.references.codes"] = "VIEW"
    rest.existing.add(f"{AREA}.references.codes")
    rest.views.add(f"{AREA}.references.codes")
    rest.set_tags(f"{AREA}.references.codes", [PUBLIC_TAG])

    _session(rest, dremio).publish().commit()

    assert not any(".references." in path for path in _published(dremio))


def test_after_update_latest_release_the_new_views_are_published() -> None:
    rest, dremio = _world({})
    session = _session(rest, dremio)
    session.update_latest_release("v1").commit()
    rest.views |= {f"{AREA}.summary"}  # what Dremio's catalog now reports as a view

    session.publish().commit()

    assert _published(dremio) == {f"{PUBLIC}.summary"}


def test_a_table_that_cannot_be_read_back_fails_and_is_dropped() -> None:
    rest, dremio = _world()
    dremio.unreadable = {f"{PUBLIC}.summary"}

    with pytest.raises(CatalogCommitError, match="was not created successfully") as info:
        _session(rest, dremio).publish().commit()

    assert info.value.rolled_back is True
    assert _published(dremio) == set()


# -- partition_by ---------------------------------------------------------------


def test_without_partition_by_nothing_is_partitioned() -> None:
    rest, dremio = _world()

    _session(rest, dremio).publish().commit()

    assert set(dremio.partitions.values()) == {None}
    assert not any("COLUMNS" in sql for sql in dremio.statements)


def test_a_list_partitions_every_view_in_its_order_and_the_views_spelling() -> None:
    rest, dremio = _world()

    report = _session(rest, dremio).publish(partition_by=["country", "year"]).commit()

    assert dremio.partitions == {
        f"{PUBLIC}.eu_sdg.monitoringsites": '"country", "Year"',
        f"{PUBLIC}.summary": '"country", "Year"',
    }
    assert (
        f'CREATE TABLE {SQL_PUBLIC}."summary" PARTITION BY ("country", "Year") '
        "STORE AS (type => 'parquet') AS SELECT * FROM "
        '"catalog"."water"."surface"."bod_rivers"."summary"'
    ) in dremio.statements
    assert (
        f"copied '{PUBLIC}.summary' from '{AREA}.summary' (3 rows, partitioned by country, Year)"
        in report.notes
    )


def test_a_set_is_sorted() -> None:
    rest, dremio = _world()

    _session(rest, dremio).publish(partition_by={"Year", "country"}).commit()

    assert dremio.partitions[f"{PUBLIC}.summary"] == '"Year", "country"'


def test_a_dict_partitions_only_the_views_it_names() -> None:
    rest, dremio = _world()

    _session(rest, dremio).publish(partition_by={"eu_sdg/monitoringsites": ["site_id"]}).commit()

    assert dremio.partitions == {
        f"{PUBLIC}.eu_sdg.monitoringsites": '"site_id"',
        f"{PUBLIC}.summary": None,
    }


def test_a_missing_field_is_an_error_when_publish_is_called() -> None:
    rest, dremio = _world()
    dremio.columns[f"{AREA}.summary"] = ["site_id", "value"]
    session = _session(rest, dremio)

    with pytest.raises(
        CatalogSessionError, match=r"has no field\(s\) country to partition by"
    ) as info:
        session.publish(partition_by=["country"])

    assert "(its fields: site_id, value)" in str(info.value)
    assert repr(session) == "CatalogSession(pending=0)"
    assert not any(sql.startswith(("DROP", "CREATE")) for sql in dremio.statements)


def test_a_field_gone_by_commit_time_is_still_an_error_before_anything_is_dropped() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio).publish(partition_by=["country"])
    dremio.columns[f"{AREA}.summary"] = ["site_id", "value"]

    with pytest.raises(CatalogCommitError, match=r"has no field\(s\) country"):
        session.commit()

    assert not any(sql.startswith(("DROP", "CREATE")) for sql in dremio.statements)


def test_a_dict_key_that_names_no_published_view_is_an_error_at_once() -> None:
    rest, dremio = _world()

    with pytest.raises(
        CatalogSessionError, match=r"names view\(s\) that are not published: 'nope'"
    ):
        _session(rest, dremio).publish(partition_by={"nope": ["year"]})

    assert _published(dremio) == set()


@pytest.mark.parametrize(
    ("partition_by", "message"),
    [
        ("year", "must be a list of field names"),
        ([], "must name at least one field"),
        (["year", ""], "fields that are not names"),
        (["year", "year"], "repeats year"),
        ({"summary": "year"}, r"partition_by\['summary'\] must be a list"),
    ],
)
def test_bad_partition_by_is_refused_at_once(partition_by: Any, message: str) -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)

    with pytest.raises(CatalogSessionError, match=message):
        session.publish(partition_by=partition_by)
    assert repr(session) == "CatalogSession(pending=0)"


# -- all or nothing ---------------------------------------------------------------

BACKUP = f"{PUBLIC}__publish_backup"


def _already_published(dremio: PublishDremio) -> None:
    """Both targets exist from an earlier publish."""
    for name in ("eu_sdg.monitoringsites", "summary"):
        dremio.tables[f"{PUBLIC}.{name}"] = "TABLE"


def test_existing_tables_are_backed_up_first_and_the_backups_dropped_after() -> None:
    rest, dremio = _world()
    _already_published(dremio)

    _session(rest, dremio).publish().commit()

    backup_create = f'CREATE TABLE {SQL_PUBLIC[:-1]}__publish_backup"'
    first_backup = next(i for i, s in enumerate(dremio.statements) if s.startswith(backup_create))
    first_view_copy = next(
        i
        for i, s in enumerate(dremio.statements)
        if s.startswith("CREATE TABLE")
        and s.endswith('FROM "catalog"."water"."surface"."bod_rivers"."summary"')
    )
    assert first_backup < first_view_copy
    assert not any(path.startswith(BACKUP) for path in dremio.tables)
    assert _published(dremio) == {f"{PUBLIC}.eu_sdg.monitoringsites", f"{PUBLIC}.summary"}


def test_a_failure_restores_the_replaced_tables_from_their_backups() -> None:
    rest, dremio = _world()
    _already_published(dremio)
    dremio.fail_on_create[f"{PUBLIC}.summary"] = RuntimeError("boom")

    with pytest.raises(CatalogCommitError, match="boom") as info:
        _session(rest, dremio).publish().commit()

    assert "2 replaced table(s) restored" in str(info.value)
    assert _published(dremio) == {f"{PUBLIC}.eu_sdg.monitoringsites", f"{PUBLIC}.summary"}
    restore = (
        f"CREATE TABLE {SQL_PUBLIC}.\"summary\" STORE AS (type => 'parquet') AS SELECT * FROM "
        f'{SQL_PUBLIC[:-1]}__publish_backup"."summary"'
    )
    assert restore in dremio.statements
    assert not any(path.startswith(BACKUP) for path in dremio.tables)


def test_a_failed_backup_touches_nothing_published() -> None:
    rest, dremio = _world()
    _already_published(dremio)
    dremio.fail_on_create[f"{BACKUP}.summary"] = RuntimeError("disk full")

    with pytest.raises(CatalogCommitError, match="disk full") as info:
        _session(rest, dremio).publish().commit()

    assert f"publishing the backup of '{PUBLIC}.summary' failed" in str(info.value)
    assert not any(
        s.startswith(
            ("DROP TABLE IF EXISTS " + SQL_PUBLIC + ".", "CREATE TABLE " + SQL_PUBLIC + ".")
        )
        for s in dremio.statements
    )
    assert not any(path.startswith(BACKUP) for path in dremio.tables)


def test_a_table_that_cannot_be_restored_keeps_its_backup_and_says_so() -> None:
    rest, dremio = _world()
    _already_published(dremio)
    dremio.unreadable = {f"{PUBLIC}.summary"}  # neither the new copy nor the restore reads back

    with pytest.raises(CatalogCommitError, match="rollback incomplete") as info:
        _session(rest, dremio).publish().commit()

    assert f"'{PUBLIC}.summary' from '{BACKUP}.summary'" in str(info.value)
    assert f"{BACKUP}.summary" in dremio.tables


def test_the_target_is_the_views_full_catalog_path_under_the_public_root() -> None:
    rest, dremio = _world()

    _session(rest, dremio).publish().commit()

    assert (
        "public_s3.dh-prod-data-public.catalog.water.surface.bod_rivers.eu_sdg.monitoringsites"
        in dremio.tables
    )
    assert any(
        s.startswith(
            'CREATE TABLE "public_s3"."dh-prod-data-public"."catalog"."water"."surface".'
            '"bod_rivers"."summary" '
        )
        for s in dremio.statements
    )
