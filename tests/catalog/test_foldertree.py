"""foldertree — scanning a subtree, rewriting view SQL, copying with proof,
and only then removing the source."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from eea_datalakehouse.catalog import foldertree, retry_state
from eea_datalakehouse.catalog.errors import CatalogOperationError
from eea_datalakehouse.catalog.foldertree import (
    foldercopy,
    foldermove,
    folderscan,
    remap_view_sql,
)
from eea_datalakehouse.catalog.operations import CatalogEntry, TableInfo, ViewDefinition


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Never touch the real ~/.cache retry-state file."""
    state_path = tmp_path / "state.json"
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", state_path)
    return state_path


class FakeCatalog:
    """In-memory stand-in for `Catalog`, holding a whole little catalog.

    `tables` maps a dot-path to a row count, `views` maps one to
    ``(sql, context)``, `folders` is a set of folder paths. Everything
    `foldertree` calls is a real method here, so a test asserts against the
    catalog's *state* afterwards rather than against a list of SQL strings —
    which is what makes "did the move actually move it" checkable.
    """

    def __init__(
        self,
        tables: dict[str, int] | None = None,
        views: dict[str, tuple[str, tuple[str, ...]]] | None = None,
        folders: set[str] | None = None,
        wikis: dict[str, str] | None = None,
        tags: dict[str, list[str]] | None = None,
        fail_copy_of: set[str] | None = None,
        short_copy_of: set[str] | None = None,
    ) -> None:
        self.tables = dict(tables or {})
        self.views = dict(views or {})
        self.folders = set(folders or set())
        self.wikis = dict(wikis or {})
        self.tags = dict(tags or {})
        self.calls: list[tuple[str, str]] = []
        self.keys: list[str] = []
        self._fail_copy_of = set(fail_copy_of or set())
        self._short_copy_of = set(short_copy_of or set())

    # -- helpers ---------------------------------------------------------
    def _record(self, operation: str, path: str, idempotency_key: str) -> None:
        self.calls.append((operation, path))
        self.keys.append(idempotency_key)

    def _under(self, prefix: str, paths) -> list[str]:
        return [p for p in paths if p == prefix or p.startswith(f"{prefix}.")]

    # -- discovery -------------------------------------------------------
    def getentriesfrom(self, schema_path: str, *, idempotency_key: str) -> list[CatalogEntry]:
        self._record("getentriesfrom", schema_path, idempotency_key)
        tables = self._under(schema_path, self.tables)
        views = self._under(schema_path, self.views)
        entries = [CatalogEntry(path=p, kind="TABLE") for p in tables]
        entries += [CatalogEntry(path=p, kind="VIEW") for p in views]
        return sorted(entries, key=lambda entry: entry.path)

    def getfoldersfrom(
        self, path: str, *, recursive: bool = True, idempotency_key: str
    ) -> list[str]:
        self._record("getfoldersfrom", path, idempotency_key)
        if path not in self.folders:
            raise CatalogOperationError(f"{path!r} does not exist")
        found = [p for p in self.folders if p.startswith(f"{path}.")]
        return sorted(found, key=lambda p: (p.count("."), p))

    def entryexists(self, path: str, *, idempotency_key: str) -> bool:
        self._record("entryexists", path, idempotency_key)
        return path in self.tables or path in self.views

    def getviewsql(self, view_path: str, *, idempotency_key: str) -> ViewDefinition:
        self._record("getviewsql", view_path, idempotency_key)
        if view_path not in self.views:
            raise CatalogOperationError(f"{view_path!r} is not a view")
        sql, context = self.views[view_path]
        return ViewDefinition(sql=sql, context=context)

    def gettableitemsfrom(self, path: str, *, idempotency_key: str) -> TableInfo:
        self._record("gettableitemsfrom", path, idempotency_key)
        if path in self.tables:
            return TableInfo(schema={"c": "INTEGER"}, row_count=self.tables[path])
        if path in self.views:
            return TableInfo(schema={"c": "INTEGER"}, row_count=self._view_rows(path))
        raise CatalogOperationError(f"{path!r} does not exist")

    def _view_rows(self, view_path: str) -> int:
        """A view counts whatever the first thing its SQL names counts.

        Quotes are stripped before matching, so a rewritten
        ``"bwd"."archive"."raw"."t"`` really does resolve to ``bwd.archive.raw.t``
        — which is what makes the row-count check on a copied view mean
        something. A view naming nothing that exists counts 0, the way a broken
        rewrite would show up.
        """
        seen: set[str] = set()
        path, sql = view_path, self.views[view_path][0].replace('"', "")
        while path not in seen:
            seen.add(path)
            for table, count in self.tables.items():
                if table.lower() in sql.lower():
                    return count
            nested = next((v for v in self.views if v != path and v.lower() in sql.lower()), None)
            if nested is None:
                return 0
            path, sql = nested, self.views[nested][0].replace('"', "")
        return 0

    # -- writes ----------------------------------------------------------
    def createfolder(
        self, path: str, *, create_parents: bool = False, idempotency_key: str
    ) -> None:
        self._record("createfolder", path, idempotency_key)
        self.folders.add(path)

    def datacopy(
        self,
        source_path: str,
        target_path: str,
        *,
        overwrite: bool = False,
        create_target_folder: bool = False,
        idempotency_key: str,
    ) -> None:
        self._record("datacopy", target_path, idempotency_key)
        if source_path in self._fail_copy_of:
            raise CatalogOperationError(f"copy of {source_path!r} refused")
        rows = self.tables[source_path]
        self.tables[target_path] = rows - 1 if source_path in self._short_copy_of else rows

    def createviewas(
        self,
        view_path: str,
        sql: str,
        *,
        overwrite: bool = False,
        create_target_folder: bool = True,
        idempotency_key: str,
    ) -> None:
        self._record("createviewas", view_path, idempotency_key)
        self.views[view_path] = (sql, ())

    def deletetable(self, table_path: str, *, idempotency_key: str) -> None:
        self._record("deletetable", table_path, idempotency_key)
        self.tables.pop(table_path, None)

    def deleteview(self, view_path: str, *, idempotency_key: str) -> None:
        self._record("deleteview", view_path, idempotency_key)
        self.views.pop(view_path, None)

    def deletefolder(self, path: str, *, cascade: bool = False, idempotency_key: str) -> None:
        self._record("deletefolder", path, idempotency_key)
        still_inside = [
            p
            for p in (*self.tables, *self.views, *self.folders)
            if p.startswith(f"{path}.")
        ]
        if still_inside:
            raise CatalogOperationError(f"folder {path!r} is not empty: {sorted(still_inside)}")
        self.folders.discard(path)

    # -- metadata --------------------------------------------------------
    def getwikifrom(self, path: str, *, idempotency_key: str) -> str:
        if path not in self.wikis:
            raise CatalogOperationError(f"{path!r} has no wiki")
        return self.wikis[path]

    def setwikito(self, path: str, text: str, *, idempotency_key: str, **_: Any) -> None:
        self.wikis[path] = text

    def gettagsfrom(self, path: str, *, idempotency_key: str) -> list[str]:
        return self.tags.get(path, [])

    def settagsto(self, path: str, tags: list[str], *, idempotency_key: str) -> None:
        self.tags[path] = list(tags)


def make_catalog(**overrides: Any) -> FakeCatalog:
    """The tree every test below starts from, unless it says otherwise.

        bwd.draft                       (folder)
        bwd.draft.raw                   (folder)
        bwd.draft.raw.measurements      100 rows
        bwd.draft.raw.stations           10 rows
        bwd.draft.assessments           (view over raw.measurements)
        bwd.draft.summary               (view over the assessments view)
    """
    base: dict[str, Any] = {
        "folders": {"bwd", "bwd.draft", "bwd.draft.raw", "bwd.archive"},
        "tables": {"bwd.draft.raw.measurements": 100, "bwd.draft.raw.stations": 10},
        "views": {
            "bwd.draft.assessments": (
                'SELECT * FROM "bwd"."draft"."raw"."measurements" WHERE ok = true',
                (),
            ),
            "bwd.draft.summary": ('SELECT * FROM "bwd"."draft"."assessments"', ()),
        },
    }
    base.update(overrides)
    return FakeCatalog(**base)


# ---------------------------------------------------------------------------
# remap_view_sql
# ---------------------------------------------------------------------------


ENTRIES = {
    "bwd.draft.raw.measurements": "bwd.archive.raw.measurements",
    "bwd.draft.assessments": "bwd.archive.assessments",
}


def remap(sql: str, **kwargs: Any):
    return remap_view_sql(
        sql, source_path="bwd.draft", target_path="bwd.archive", entries=ENTRIES, **kwargs
    )


def test_rewrites_a_fully_qualified_reference_into_the_target() -> None:
    result = remap('SELECT * FROM "bwd"."draft"."raw"."measurements"')

    assert result.sql == 'SELECT * FROM "bwd"."archive"."raw"."measurements"'
    assert result.references == ("bwd.draft.raw.measurements",)


def test_rewrites_an_unquoted_reference_and_keeps_the_column_it_qualifies() -> None:
    result = remap("SELECT bwd.draft.assessments.value FROM bwd.draft.assessments")

    assert result.sql == (
        'SELECT "bwd"."archive"."assessments"."value" FROM "bwd"."archive"."assessments"'
    )


def test_matches_paths_case_insensitively() -> None:
    assert remap('SELECT * FROM "BWD"."Draft"."raw"."Measurements"').sql == (
        'SELECT * FROM "bwd"."archive"."raw"."measurements"'
    )


def test_leaves_references_outside_the_moved_subtree_alone() -> None:
    sql = 'SELECT * FROM "other"."space"."t" JOIN "bwd"."draft"."assessments" ON 1 = 1'

    assert remap(sql).sql == (
        'SELECT * FROM "other"."space"."t" JOIN "bwd"."archive"."assessments" ON 1 = 1'
    )


def test_never_rewrites_inside_a_string_literal_or_a_comment() -> None:
    sql = (
        "SELECT 'bwd.draft.raw.measurements' AS label, "
        "'it''s bwd.draft here too' AS quoted "
        "-- and bwd.draft.assessments in a comment\n"
        "/* and bwd.draft.assessments in a block */ "
        'FROM "bwd"."draft"."assessments"'
    )
    result = remap(sql)

    assert "'bwd.draft.raw.measurements'" in result.sql
    assert "'it''s bwd.draft here too'" in result.sql
    assert "-- and bwd.draft.assessments in a comment" in result.sql
    assert "/* and bwd.draft.assessments in a block */" in result.sql
    assert result.sql.endswith('FROM "bwd"."archive"."assessments"')


def test_does_not_read_a_decimal_number_as_a_path() -> None:
    assert remap("SELECT 1.5 AS x FROM t").sql == "SELECT 1.5 AS x FROM t"


def test_spells_out_a_context_relative_reference_into_the_subtree() -> None:
    # The view said `FROM assessments`, resolved against bwd.draft. After the
    # move it has no such context, so the reference has to be written in full.
    result = remap("SELECT * FROM assessments", context=("bwd", "draft"))

    assert result.sql == 'SELECT * FROM "bwd"."archive"."assessments"'
    assert result.references == ("bwd.draft.assessments",)


def test_qualifies_a_context_relative_reference_that_is_not_moving() -> None:
    probed: list[str] = []

    def exists(path: str) -> bool:
        probed.append(path)
        return path == "bwd.reference.codelist"

    result = remap(
        "SELECT * FROM reference.codelist",
        context=("bwd",),
        external_exists=exists,
    )

    assert result.sql == 'SELECT * FROM "bwd"."reference"."codelist"'
    assert probed == ["bwd.reference.codelist"]
    assert result.notes  # it says what it did


def test_keeps_the_column_on_a_context_relative_reference() -> None:
    result = remap("SELECT assessments.value FROM assessments", context=("bwd", "draft"))

    assert result.sql == (
        'SELECT "bwd"."archive"."assessments"."value" FROM "bwd"."archive"."assessments"'
    )


def test_does_not_probe_when_no_external_exists_is_given() -> None:
    assert remap("SELECT * FROM reference.codelist", context=("bwd",)).sql == (
        "SELECT * FROM reference.codelist"
    )


def test_says_so_when_the_context_is_unknown_and_a_bare_name_is_used() -> None:
    result = remap("SELECT * FROM assessments")

    assert result.sql == "SELECT * FROM assessments"
    assert any("sqlContext is unknown" in note for note in result.notes)


def test_falls_back_to_the_source_root_for_a_path_the_scan_did_not_list() -> None:
    # A table created between the scan and the rewrite is still under the
    # source root, so its prefix is swapped anyway.
    assert remap('SELECT * FROM "bwd"."draft"."raw"."brand_new"').sql == (
        'SELECT * FROM "bwd"."archive"."raw"."brand_new"'
    )


# ---------------------------------------------------------------------------
# folderscan
# ---------------------------------------------------------------------------


def test_scan_finds_folders_tables_and_views_and_maps_them_to_the_target() -> None:
    tree = folderscan(make_catalog(), "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert [entry.source_path for entry in tree.folders] == ["bwd.draft.raw"]
    assert [entry.target_path for entry in tree.folders] == ["bwd.archive.2025.raw"]
    assert [entry.source_path for entry in tree.tables] == [
        "bwd.draft.raw.measurements",
        "bwd.draft.raw.stations",
    ]
    assert [entry.target_path for entry in tree.tables] == [
        "bwd.archive.2025.raw.measurements",
        "bwd.archive.2025.raw.stations",
    ]
    assert len(tree) == 5


def test_scan_puts_a_view_after_the_view_it_selects_from() -> None:
    tree = folderscan(make_catalog(), "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    ordered = [entry.source_path for entry in tree.views]
    assert ordered.index("bwd.draft.assessments") < ordered.index("bwd.draft.summary")
    summary = next(e for e in tree.views if e.source_path == "bwd.draft.summary")
    assert summary.depends_on == ("bwd.draft.assessments",)
    assert summary.remapped_sql == 'SELECT * FROM "bwd"."archive"."2025"."assessments"'


def test_scan_derives_folders_from_entry_paths_when_rest_cannot_list_them() -> None:
    catalog = make_catalog(folders=set())  # REST answers "does not exist"
    tree = folderscan(catalog, "bwd.draft", "bwd.archive", idempotency_key="k")

    assert [entry.source_path for entry in tree.folders] == ["bwd.draft.raw"]
    assert any("rebuilt from the paths" in note for note in tree.notes)


def test_scan_refuses_a_target_inside_the_source() -> None:
    with pytest.raises(CatalogOperationError, match="inside source"):
        folderscan(make_catalog(), "bwd.draft", "bwd.draft.archive", idempotency_key="k")


def test_scan_refuses_a_source_inside_the_target() -> None:
    with pytest.raises(CatalogOperationError, match="inside target"):
        folderscan(make_catalog(), "bwd.draft.raw", "bwd.draft", idempotency_key="k")


def test_scan_refuses_the_same_path_twice() -> None:
    with pytest.raises(CatalogOperationError, match="same folder"):
        folderscan(make_catalog(), "bwd.draft", "BWD.Draft", idempotency_key="k")


def test_scan_raises_when_the_source_cannot_be_read_at_all() -> None:
    with pytest.raises(CatalogOperationError, match="could not read"):
        folderscan(FakeCatalog(), "bwd.nothing", "bwd.archive", idempotency_key="k")


# ---------------------------------------------------------------------------
# foldercopy
# ---------------------------------------------------------------------------


def test_dry_run_writes_nothing_and_counts_the_source() -> None:
    catalog = make_catalog()
    report = foldercopy(
        catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k", dry_run=True
    )

    assert [row.status for row in report.results] == ["planned"] * 5
    assert [row.source_rows for row in report.tables] == [100, 10]
    assert report.verified is False
    assert not any(op in {"datacopy", "createviewas", "createfolder"} for op, _ in catalog.calls)


def test_copy_reproduces_the_tree_and_verifies_by_row_count() -> None:
    catalog = make_catalog()
    report = foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert report.verified is True
    assert catalog.tables["bwd.archive.2025.raw.measurements"] == 100
    assert catalog.tables["bwd.archive.2025.raw.stations"] == 10
    assert [(row.source_rows, row.target_rows) for row in report.tables] == [(100, 100), (10, 10)]
    # the copied view points at the copy, not back at the original
    assert catalog.views["bwd.archive.2025.assessments"][0] == (
        'SELECT * FROM "bwd"."archive"."2025"."raw"."measurements" WHERE ok = true'
    )
    # and it resolves: both sides of every view counted the same real rows,
    # rather than both quietly counting nothing
    assert [(row.source_rows, row.target_rows) for row in report.views] == [(100, 100), (100, 100)]
    # and the source is untouched
    assert catalog.tables["bwd.draft.raw.measurements"] == 100
    assert "bwd.draft.assessments" in catalog.views


def test_copy_fails_the_entry_when_the_counts_disagree() -> None:
    catalog = make_catalog(short_copy_of={"bwd.draft.raw.stations"})
    report = foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert report.verified is False
    stations = next(r for r in report.tables if r.entry.name == "stations")
    assert (stations.source_rows, stations.target_rows) == (10, 9)
    assert stations.counts_match is False


def test_one_failed_table_does_not_stop_the_rest_of_the_sweep() -> None:
    catalog = make_catalog(fail_copy_of={"bwd.draft.raw.measurements"})
    report = foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert [row.status for row in report.tables] == ["failed", "copied"]
    assert catalog.tables["bwd.archive.2025.raw.stations"] == 10
    assert report.verified is False


def test_a_stale_view_already_at_the_target_is_caught_by_the_count() -> None:
    # Something left over from an earlier, different job sits where this view
    # should go. skip_existing leaves it alone — but counting both sides is
    # what stops that from passing for a copy.
    catalog = make_catalog()
    stale_sql = 'SELECT * FROM "bwd"."draft"."raw"."stations"'
    catalog.views["bwd.archive.2025.assessments"] = (stale_sql, ())

    report = foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    stale = next(r for r in report.views if r.entry.name == "assessments")
    assert stale.status == "failed"
    assert (stale.source_rows, stale.target_rows) == (100, 10)
    assert "not equivalent" in (stale.error or "")
    assert report.verified is False


def test_views_are_not_counted_when_verify_views_is_off() -> None:
    catalog = make_catalog()
    report = foldercopy(
        catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k", verify_views=False
    )

    assert [row.source_rows for row in report.views] == [None, None]
    assert report.verified is True


def test_re_running_skips_what_already_landed() -> None:
    catalog = make_catalog()
    foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")
    catalog.calls.clear()

    report = foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert [row.status for row in report.tables] == ["skipped", "skipped"]
    assert [row.status for row in report.views] == ["skipped", "skipped"]
    assert report.verified is True
    assert not any(op == "datacopy" for op, _ in catalog.calls)


def test_an_existing_target_with_a_different_count_fails_rather_than_being_accepted() -> None:
    catalog = make_catalog()
    catalog.tables["bwd.archive.2025.raw.measurements"] = 7

    report = foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    failed = next(r for r in report.tables if r.entry.name == "measurements")
    assert failed.status == "failed"
    assert "overwrite=True" in (failed.error or "")
    assert catalog.tables["bwd.archive.2025.raw.measurements"] == 7  # left alone


def test_overwrite_replaces_an_existing_target() -> None:
    catalog = make_catalog()
    catalog.tables["bwd.archive.2025.raw.measurements"] = 7

    report = foldercopy(
        catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k", overwrite=True
    )

    assert report.verified is True
    assert catalog.tables["bwd.archive.2025.raw.measurements"] == 100


def test_metadata_travels_with_the_entries() -> None:
    catalog = make_catalog(
        wikis={"bwd.draft.raw": "the raw landing zone"},
        tags={"bwd.draft.raw.measurements": ["bronze"]},
    )
    foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert catalog.wikis["bwd.archive.2025.raw"] == "the raw landing zone"
    assert catalog.tags["bwd.archive.2025.raw.measurements"] == ["bronze"]


def test_every_entry_gets_its_own_idempotency_key() -> None:
    catalog = make_catalog()
    foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="job-1")

    copy_keys = [
        key for (op, _), key in zip(catalog.calls, catalog.keys, strict=True) if op == "datacopy"
    ]
    assert copy_keys == [
        "job-1:datacopy:bwd.archive.2025.raw.measurements",
        "job-1:datacopy:bwd.archive.2025.raw.stations",
    ]


def test_a_prepared_tree_is_applied_without_a_second_scan() -> None:
    catalog = make_catalog()
    tree = folderscan(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")
    catalog.calls.clear()

    foldercopy(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k", tree=tree)

    assert not any(op == "getentriesfrom" for op, _ in catalog.calls)


# ---------------------------------------------------------------------------
# foldermove
# ---------------------------------------------------------------------------


def test_move_removes_the_source_only_after_a_verified_copy() -> None:
    catalog = make_catalog()
    report = foldermove(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert report.verified is True
    assert report.source_deleted is True
    assert not [p for p in catalog.tables if p.startswith("bwd.draft")]
    assert not [p for p in catalog.views if p.startswith("bwd.draft")]
    assert "bwd.draft" not in catalog.folders
    assert "bwd.draft.raw" not in catalog.folders
    # everything is where it should be on the other side
    assert catalog.tables["bwd.archive.2025.raw.measurements"] == 100
    assert "bwd.archive.2025.summary" in catalog.views


def test_move_takes_the_source_apart_in_a_deletable_order() -> None:
    catalog = make_catalog()
    foldermove(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    removals = [(op, path) for op, path in catalog.calls if op.startswith("delete")]
    order = [path for _, path in removals]
    # dependent view before the one it selects from; folders last, deepest first
    assert order.index("bwd.draft.summary") < order.index("bwd.draft.assessments")
    assert order.index("bwd.draft.raw.measurements") < order.index("bwd.draft.raw")
    assert order.index("bwd.draft.raw") < order.index("bwd.draft")
    assert order[-1] == "bwd.draft"


def test_move_leaves_the_source_alone_when_the_copy_did_not_verify() -> None:
    catalog = make_catalog(short_copy_of={"bwd.draft.raw.stations"})
    report = foldermove(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert report.verified is False
    assert report.source_deleted is False
    assert report.deleted == []
    assert catalog.tables["bwd.draft.raw.stations"] == 10
    assert "bwd.draft" in catalog.folders
    assert any("left exactly as it was" in note for note in report.notes)


def test_move_can_keep_the_source_root_folder() -> None:
    catalog = make_catalog()
    foldermove(
        catalog,
        "bwd.draft",
        "bwd.archive.2025",
        idempotency_key="k",
        delete_source_root=False,
    )

    assert "bwd.draft" in catalog.folders
    assert not [p for p in catalog.tables if p.startswith("bwd.draft.")]


def test_move_dry_run_removes_nothing() -> None:
    catalog = make_catalog()
    report = foldermove(
        catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k", dry_run=True
    )

    assert report.source_deleted is False
    assert catalog.tables["bwd.draft.raw.measurements"] == 100
    assert any("dry run" in note for note in report.notes)


def test_move_reports_a_source_folder_it_could_not_empty() -> None:
    catalog = make_catalog()
    # Something nobody told us about turns up inside the source mid-move.
    original_deletetable = catalog.deletetable

    def deletetable(table_path: str, *, idempotency_key: str) -> None:
        original_deletetable(table_path, idempotency_key=idempotency_key)
        catalog.tables.setdefault("bwd.draft.raw.surprise", 1)

    catalog.deletetable = deletetable  # type: ignore[method-assign]
    report = foldermove(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")

    assert report.source_deleted is False
    assert [path for path, _ in report.delete_errors] == ["bwd.draft.raw", "bwd.draft"]
    assert any("could not be removed" in note for note in report.notes)


def test_summary_reads_as_a_report() -> None:
    catalog = make_catalog()
    report = foldermove(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k")
    text = report.summary()

    assert "RESULT: bwd.draft -> bwd.archive.2025" in text
    assert "[100 == 100 rows]" in text
    assert "verified: True" in text
    assert "removed from the source" in text


def test_progress_is_reported_as_it_goes() -> None:
    messages: list[str] = []
    foldermove(
        make_catalog(),
        "bwd.draft",
        "bwd.archive.2025",
        idempotency_key="k",
        on_progress=messages.append,
    )

    assert any(message.startswith("scanning") for message in messages)
    assert any("copy verified" in message for message in messages)
    assert any(message.startswith("removed bwd.draft") for message in messages)


def test_module_exports_the_three_verbs() -> None:
    assert foldertree.folderscan is folderscan
    assert foldertree.foldercopy is foldercopy
    assert foldertree.foldermove is foldermove


def test_nothing_at_the_target_is_destroyed_without_overwrite() -> None:
    # skip_existing=False says "don't accept what's there", not "replace it".
    catalog = make_catalog()
    catalog.tables["bwd.archive.2025.raw.measurements"] = 100  # same count, still not ours to drop

    report = foldercopy(
        catalog, "bwd.draft", "bwd.archive.2025", idempotency_key="k", skip_existing=False
    )

    occupied = next(r for r in report.tables if r.entry.name == "measurements")
    assert occupied.status == "failed"
    assert "overwrite=True" in (occupied.error or "")
    assert not any(op == "datacopy" and path.endswith("measurements") for op, path in catalog.calls)
