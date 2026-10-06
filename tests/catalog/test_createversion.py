"""createversion: copy a list of tables from a source folder into a new version folder.

Uses `MiniDremio`, a small in-memory stand-in that answers the
INFORMATION_SCHEMA queries the operation makes and applies its CREATE/DROP
statements, kept in step with `FakeCatalogRest` — `createversion` runs several
different queries, which a fixed-rows `FakeExecutor` can't answer.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest

from eea_datalakehouse.catalog import operations, retry_state
from eea_datalakehouse.catalog.client import Catalog
from eea_datalakehouse.catalog.errors import CatalogOperationError, EngineStartingError
from eea_datalakehouse.catalog.session import (
    CatalogCommitError,
    CatalogSession,
    CatalogSessionError,
)
from eea_datalakehouse.catalog.sql import SqlResult

from .conftest import FakeCatalogRest

SOURCE = "catalog.bwd.draft"
TARGET = "catalog.bwd.versions"
VERSION = f"{TARGET}.v2025_1"

_EXACT = re.compile(
    r"\"TABLE_SCHEMA\" = '(?P<schema>[^']*)' AND \"TABLE_NAME\" = '(?P<name>[^']*)'"
)
_SUBTREE = re.compile(r"\"TABLE_SCHEMA\" = '(?P<schema>[^']*)' OR \"TABLE_SCHEMA\" LIKE")
_CTAS = re.compile(r"CREATE TABLE (?P<target>\S+) AS SELECT \* FROM (?P<source>\S+)")
_DROP = re.compile(r"DROP (?:TABLE|VIEW)(?: IF EXISTS)? (?P<path>\S+)")


def _unquote(sql_path: str) -> str:
    return ".".join(part.strip('"') for part in sql_path.split("."))


class MiniDremio:
    """SqlExecutor over an in-memory ``{path: "TABLE" | "VIEW"}``."""

    def __init__(self, rest: FakeCatalogRest, tables: dict[str, str]) -> None:
        self.rest = rest
        self.tables = dict(tables)
        self.statements: list[str] = []
        self.fail_on_create: dict[str, Exception] = {}
        for path in tables:
            rest.existing.add(path)

    def execute(self, sql: str, *, idempotency_key: str | None = None) -> SqlResult:
        self.statements.append(sql)
        if match := _CTAS.search(sql):
            target = _unquote(match["target"])
            if target in self.fail_on_create:
                raise self.fail_on_create[target]
            self.tables[target] = "TABLE"
            self.rest.existing.add(target)
        elif match := _DROP.search(sql):
            path = _unquote(match["path"])
            self.tables.pop(path, None)
            self.rest.existing.discard(path)
        return SqlResult(row_count=1, job_id="job")

    def fetch_all(self, sql: str, *, idempotency_key: str | None = None) -> list[dict[str, Any]]:
        self.statements.append(sql)
        if match := _EXACT.search(sql):
            path = f"{match['schema']}.{match['name']}"
            if path in self.tables:
                return [{"TABLE_NAME": match["name"], "TABLE_TYPE": self.tables[path]}]
            return []
        if match := _SUBTREE.search(sql):
            schema = match["schema"]
            return [
                {
                    "TABLE_SCHEMA": path.rsplit(".", 1)[0],
                    "TABLE_NAME": path.rsplit(".", 1)[1],
                    "TABLE_TYPE": kind,
                }
                for path, kind in self.tables.items()
                if path.startswith(f"{schema}.")
            ]
        raise AssertionError(f"unexpected query: {sql}")


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", tmp_path / "state.json")


def _world(
    tables: dict[str, str] | None = None,
    *,
    versions: set[str] = frozenset(),  # type: ignore[assignment]
) -> tuple[FakeCatalogRest, MiniDremio]:
    folders = {
        "catalog",
        "catalog.bwd",
        SOURCE,
        f"{SOURCE}.bw_assessment",
        f"{SOURCE}.reference",
        TARGET,
        *versions,
    }
    rest = FakeCatalogRest(existing=set(folders), folders=set(folders))
    dremio = MiniDremio(
        rest,
        tables
        if tables is not None
        else {
            f"{SOURCE}.bw_assessment.assessments": "TABLE",
            f"{SOURCE}.bw_assessment.stations": "TABLE",
            f"{SOURCE}.reference.quality_classes": "TABLE",
            f"{SOURCE}.bw_assessment.latest": "VIEW",
        },
    )
    return rest, dremio


def _create(dremio: MiniDremio, rest: FakeCatalogRest, tables: list[str], **kwargs: Any) -> Any:
    return operations.createversion(
        dremio, rest, SOURCE, "v2025_1", tables, target_path=TARGET, idempotency_key="k", **kwargs
    )


# -- happy path ---------------------------------------------------------------


def test_copies_tables_into_a_new_version_folder_keeping_sub_folders() -> None:
    rest, dremio = _world()

    result = _create(dremio, rest, ["bw_assessment.assessments", "bw_assessment.stations"])

    assert result.version_path == VERSION
    assert result.created_folder is True
    assert result.copied == [
        (f"{SOURCE}.bw_assessment.assessments", f"{VERSION}.bw_assessment.assessments"),
        (f"{SOURCE}.bw_assessment.stations", f"{VERSION}.bw_assessment.stations"),
    ]
    assert result.skipped == [] and result.replaced == []
    assert VERSION in rest.created
    assert dremio.tables[f"{VERSION}.bw_assessment.assessments"] == "TABLE"
    assert (
        'CREATE TABLE "catalog"."bwd"."versions"."v2025_1"."bw_assessment"."assessments" '
        'AS SELECT * FROM "catalog"."bwd"."draft"."bw_assessment"."assessments"'
    ) in dremio.statements


def test_tables_may_be_full_paths_as_list_returns_them() -> None:
    rest, dremio = _world()

    result = _create(dremio, rest, [f"{SOURCE}.bw_assessment.assessments"])

    assert result.copied == [
        (f"{SOURCE}.bw_assessment.assessments", f"{VERSION}.bw_assessment.assessments")
    ]


def test_reference_vocabulary_and_views_are_skipped_and_reported() -> None:
    rest, dremio = _world(
        {
            f"{SOURCE}.bw_assessment.assessments": "TABLE",
            f"{SOURCE}.reference.quality_classes": "TABLE",
            f"{SOURCE}.Vocabularies.types": "TABLE",
            f"{SOURCE}.bw_assessment.latest": "VIEW",
        }
    )

    result = _create(
        dremio,
        rest,
        [
            "bw_assessment.assessments",
            "reference.quality_classes",
            "Vocabularies.types",
            "bw_assessment.latest",
        ],
    )

    assert [source for source, _ in result.copied] == [f"{SOURCE}.bw_assessment.assessments"]
    assert result.skipped == [
        (f"{SOURCE}.reference.quality_classes", "reference table — not versioned"),
        (f"{SOURCE}.Vocabularies.types", "vocabularies table — not versioned"),
        (f"{SOURCE}.bw_assessment.latest", "view — only tables are versioned"),
    ]
    assert f"{VERSION}.reference.quality_classes" not in dremio.tables


# -- validation: nothing is written -------------------------------------------


@pytest.mark.parametrize(
    ("source", "version", "tables", "message"),
    [
        ("", "v2025_1", None, "source_path must not be empty"),
        ("   ", "v2025_1", None, "source_path must not be empty"),
        (None, "v2025_1", None, "source_path must not be empty"),
        (SOURCE, "", None, "version_name must not be empty"),
        (SOURCE, " \t ", None, "version_name must not be empty"),
        (SOURCE, "v2025.1", None, "single folder name"),
        (SOURCE, "v2025_1", "bw_assessment.assessments", "must be a list"),
        (SOURCE, "v2025_1", ["a.b", "a.b"], "listed more than once"),
        (SOURCE, "v2025_1", ["  "], "non-empty name"),
    ],
)
def test_bad_arguments_fail_before_anything_runs(
    source: Any, version: Any, tables: Any, message: str
) -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogOperationError, match=message):
        operations.createversion(
            dremio, rest, source, version, tables, target_path=TARGET, idempotency_key="k"
        )
    assert dremio.statements == [] and rest.created == []


def test_all_missing_entries_are_named_at_once() -> None:
    rest, dremio = _world()

    with pytest.raises(
        CatalogOperationError, match=r"no table or folder with tables under .*\['nope', 'x\.y'\]"
    ):
        _create(dremio, rest, ["bw_assessment.assessments", "nope", "x.y"])
    assert rest.created == []


def test_missing_source_is_an_error() -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogOperationError, match="source 'catalog.bwd.nope' does not exist"):
        operations.createversion(
            dremio, rest, "catalog.bwd.nope", "v1", ["a"], target_path=TARGET, idempotency_key="k"
        )


# -- no table list: everything under the source -------------------------------


@pytest.mark.parametrize("tables", [None, []])
def test_no_tables_copies_everything_under_the_source(tables: Any) -> None:
    rest, dremio = _world()

    result = operations.createversion(
        dremio, rest, SOURCE, "v2025_1", tables, target_path=TARGET, idempotency_key="k"
    )

    assert [target for _, target in result.copied] == [
        f"{VERSION}.bw_assessment.assessments",
        f"{VERSION}.bw_assessment.stations",
    ]
    assert result.skipped == [
        (f"{SOURCE}.bw_assessment.latest", "view — only tables are versioned"),
        (f"{SOURCE}.reference.quality_classes", "reference table — not versioned"),
    ]


def test_no_tables_from_an_empty_source_is_an_error() -> None:
    rest, dremio = _world({})

    with pytest.raises(CatalogOperationError, match="has no tables"):
        operations.createversion(
            dremio, rest, SOURCE, "v1", target_path=TARGET, idempotency_key="k"
        )
    assert rest.created == []


def test_session_with_tables_omitted_copies_everything() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.use(TARGET)

    report = session.create_version(SOURCE, "v2025_1").commit()

    assert report.succeeded == [f"create version '{VERSION}' from '{SOURCE}' (all tables)"]
    assert f"{VERSION}.bw_assessment.stations" in dremio.tables


# -- folders in the table list ------------------------------------------------


def test_a_folder_entry_stands_for_every_table_under_it() -> None:
    rest, dremio = _world()

    result = _create(dremio, rest, ["bw_assessment"])

    assert result.copied == [
        (f"{SOURCE}.bw_assessment.assessments", f"{VERSION}.bw_assessment.assessments"),
        (f"{SOURCE}.bw_assessment.stations", f"{VERSION}.bw_assessment.stations"),
    ]
    # the folder's view is still skipped, and reported
    assert result.skipped == [
        (f"{SOURCE}.bw_assessment.latest", "view — only tables are versioned")
    ]


def test_a_folder_entry_reaches_nested_folders_and_skips_reference_ones() -> None:
    rest, dremio = _world(
        {
            f"{SOURCE}.bw_assessment.assessments": "TABLE",
            f"{SOURCE}.bw_assessment.deep.more.t": "TABLE",
            f"{SOURCE}.bw_assessment.reference.codes": "TABLE",
        }
    )

    result = _create(dremio, rest, [f"{SOURCE}.bw_assessment"])  # full path works too

    assert [target for _, target in result.copied] == [
        f"{VERSION}.bw_assessment.assessments",
        f"{VERSION}.bw_assessment.deep.more.t",
    ]
    assert result.skipped == [
        (f"{SOURCE}.bw_assessment.reference.codes", "reference table — not versioned")
    ]


def test_a_table_listed_and_inside_a_listed_folder_is_copied_once() -> None:
    rest, dremio = _world()

    result = _create(dremio, rest, ["bw_assessment.assessments", "bw_assessment"])

    targets = [target for _, target in result.copied]
    assert targets.count(f"{VERSION}.bw_assessment.assessments") == 1
    assert len(targets) == 2


def test_a_folder_with_no_tables_is_reported_as_missing() -> None:
    rest, dremio = _world()
    rest.existing.add(f"{SOURCE}.empty")
    rest.folders.add(f"{SOURCE}.empty")

    with pytest.raises(CatalogOperationError, match=r"\['empty'\]"):
        _create(dremio, rest, ["empty"])


# -- missing target -----------------------------------------------------------


def test_missing_target_is_created_by_default() -> None:
    rest, dremio = _world()
    target = "catalog.bwd.releases.water"

    result = operations.createversion(
        dremio,
        rest,
        SOURCE,
        "v1",
        ["bw_assessment.assessments"],
        target_path=target,
        idempotency_key="k",
    )

    assert result.created_target_folders == ["catalog.bwd.releases", target]
    assert result.version_path == f"{target}.v1"
    assert f"{target}.v1.bw_assessment.assessments" in dremio.tables


def test_missing_target_is_an_error_when_creation_is_off() -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogOperationError, match="pass create_target_folder=True"):
        operations.createversion(
            dremio,
            rest,
            SOURCE,
            "v1",
            ["bw_assessment.assessments"],
            target_path="catalog.bwd.nope",
            create_target_folder=False,
            idempotency_key="k",
        )
    assert rest.created == []


def test_failure_also_removes_the_target_levels_it_created() -> None:
    rest, dremio = _world()
    target = "catalog.bwd.releases.water"
    dremio.fail_on_create[f"{target}.v1.bw_assessment.assessments"] = RuntimeError("disk full")

    with pytest.raises(RuntimeError):
        operations.createversion(
            dremio,
            rest,
            SOURCE,
            "v1",
            ["bw_assessment.assessments"],
            target_path=target,
            idempotency_key="k",
        )

    assert "catalog.bwd.releases" not in rest.existing
    assert TARGET in rest.existing  # was there before: untouched


def test_nothing_left_to_copy_is_an_error() -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogOperationError, match="nothing to copy"):
        _create(dremio, rest, ["reference.quality_classes", "bw_assessment.latest"])
    assert rest.created == []


# -- existing version / overwrite ---------------------------------------------


def test_existing_version_folder_is_an_error_without_overwrite() -> None:
    rest, dremio = _world(versions={VERSION})

    with pytest.raises(CatalogOperationError, match="already exists — pass overwrite=True"):
        _create(dremio, rest, ["bw_assessment.assessments"])


def test_overwrite_replaces_listed_tables_and_keeps_the_rest() -> None:
    rest, dremio = _world(versions={VERSION, f"{VERSION}.bw_assessment"})
    dremio.tables[f"{VERSION}.bw_assessment.assessments"] = "TABLE"
    dremio.tables[f"{VERSION}.bw_assessment.other"] = "TABLE"
    rest.existing |= {f"{VERSION}.bw_assessment.assessments", f"{VERSION}.bw_assessment.other"}

    result = _create(
        dremio, rest, ["bw_assessment.assessments", "bw_assessment.stations"], overwrite=True
    )

    assert result.created_folder is False
    assert result.replaced == [f"{VERSION}.bw_assessment.assessments"]
    assert f"{VERSION}.bw_assessment.other" in dremio.tables  # not listed: untouched
    assert f"{VERSION}.bw_assessment.stations" in dremio.tables


# -- failure cleanup ----------------------------------------------------------


def test_failure_deletes_the_version_folder_it_created() -> None:
    rest, dremio = _world()
    dremio.fail_on_create[f"{VERSION}.bw_assessment.stations"] = RuntimeError("disk full")

    with pytest.raises(RuntimeError, match="disk full"):
        _create(dremio, rest, ["bw_assessment.assessments", "bw_assessment.stations"])

    assert VERSION not in rest.existing
    assert f"{VERSION}.bw_assessment.assessments" not in dremio.tables


def test_failure_in_an_existing_folder_drops_only_the_new_copies() -> None:
    rest, dremio = _world(versions={VERSION})
    dremio.fail_on_create[f"{VERSION}.bw_assessment.stations"] = RuntimeError("disk full")

    with pytest.raises(RuntimeError):
        _create(
            dremio, rest, ["bw_assessment.assessments", "bw_assessment.stations"], overwrite=True
        )

    assert VERSION in rest.existing  # it existed before: kept
    assert f"{VERSION}.bw_assessment.assessments" not in dremio.tables


def test_engine_starting_is_remembered_for_retry_pending() -> None:
    rest, dremio = _world()
    dremio.fail_on_create[f"{VERSION}.bw_assessment.assessments"] = EngineStartingError(
        "warming up", idempotency_key="k"
    )

    with pytest.raises(EngineStartingError):
        _create(dremio, rest, ["bw_assessment.assessments"])

    pending = retry_state.get("k")
    assert pending is not None and pending.operation == "createversion"
    assert VERSION not in rest.existing  # cleaned up, so a retry starts fresh

    del dremio.fail_on_create[f"{VERSION}.bw_assessment.assessments"]
    result = operations.retry_pending(dremio, "k", catalog_rest=rest)
    assert result.copied[0][1] == f"{VERSION}.bw_assessment.assessments"
    assert retry_state.get("k") is None


# -- Catalog and CatalogSession -----------------------------------------------


def _session(rest: FakeCatalogRest, dremio: MiniDremio) -> CatalogSession:
    catalog = Catalog(
        "https://dremio.example.test",
        "pat",
        executor=dremio,  # type: ignore[arg-type]
        flight_executor=dremio,  # type: ignore[arg-type]
        catalog_rest=rest,  # type: ignore[arg-type]
    )
    return CatalogSession(catalog)


def test_session_target_defaults_to_the_context_and_reports_skipped_tables() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.use(TARGET)

    report = session.create_version(
        SOURCE, "v2025_1", ["bw_assessment.assessments", "reference.quality_classes"]
    ).commit()

    assert report.succeeded == [f"create version '{VERSION}' from '{SOURCE}' (2 table(s) listed)"]
    assert report.notes == [
        f"skipped '{SOURCE}.reference.quality_classes': reference table — not versioned"
    ]
    assert f"{VERSION}.bw_assessment.assessments" in dremio.tables


def test_session_relative_source_resolves_against_the_context() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.use(TARGET)

    session.create_version("../draft", "v2025_1", ["bw_assessment.assessments"]).commit()

    assert f"{VERSION}.bw_assessment.assessments" in dremio.tables


def test_session_without_target_or_context_raises_at_once() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.set_context(None)

    with pytest.raises(CatalogSessionError, match="call use\\(\\) first"):
        session.create_version(SOURCE, "v2025_1", ["bw_assessment.assessments"])


def test_session_bad_arguments_raise_at_once() -> None:
    rest, dremio = _world()

    with pytest.raises(CatalogSessionError, match="version_name must not be empty"):
        _session(rest, dremio).create_version(SOURCE, "  ", target_path=TARGET)
    with pytest.raises(CatalogSessionError, match="source_path must not be empty"):
        _session(rest, dremio).create_version(" ", "v2025_1", target_path=TARGET)


def test_session_rollback_deletes_the_new_version() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)

    session.create_version(SOURCE, "v2025_1", ["bw_assessment.assessments"], TARGET)
    session.set_tags("catalog.bwd.missing", ["x"])  # fails: path does not exist

    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is True
    assert VERSION not in rest.existing
    assert f"{VERSION}.bw_assessment.assessments" not in dremio.tables


def test_session_rollback_also_deletes_a_target_it_created() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)

    session.create_version(SOURCE, "v1", ["bw_assessment"], "catalog.bwd.releases.water")
    session.set_tags("catalog.bwd.missing", ["x"])

    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is True
    assert "catalog.bwd.releases" not in rest.existing


def test_session_overwrite_that_replaced_tables_is_irreversible() -> None:
    rest, dremio = _world(versions={VERSION, f"{VERSION}.bw_assessment"})
    dremio.tables[f"{VERSION}.bw_assessment.assessments"] = "TABLE"
    rest.existing.add(f"{VERSION}.bw_assessment.assessments")
    session = _session(rest, dremio)

    session.create_version(SOURCE, "v2025_1", ["bw_assessment.assessments"], TARGET, overwrite=True)
    session.set_tags("catalog.bwd.missing", ["x"])

    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is False
    assert "overwrote 1 existing table(s)" in info.value.unresolved[0]


# -- draft_to_version ---------------------------------------------------------


@pytest.mark.parametrize(
    "context",
    [
        "catalog.bwd",  # the folder that holds draft
        SOURCE,  # draft itself
        f"{SOURCE}.bw_assessment",  # inside draft
    ],
)
def test_draft_to_version_finds_draft_from_the_context(context: str) -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.use(context)

    report = session.draft_to_version("v2025_1", ["bw_assessment.assessments"]).commit()

    assert report.succeeded == [
        f"create version '{VERSION}' from '{SOURCE}' (1 table(s) listed)"
    ]
    assert f"{VERSION}.bw_assessment.assessments" in dremio.tables


def test_draft_to_version_without_tables_copies_all_of_draft() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.use("catalog.bwd")

    report = session.draft_to_version("v2025_1").commit()

    assert f"{VERSION}.bw_assessment.stations" in dremio.tables
    assert report.notes == [
        f"skipped '{SOURCE}.bw_assessment.latest': view — only tables are versioned",
        f"skipped '{SOURCE}.reference.quality_classes': reference table — not versioned",
    ]


def test_draft_to_version_creates_the_versions_folder_if_missing() -> None:
    rest, dremio = _world()
    rest.existing.discard(TARGET)
    rest.folders.discard(TARGET)
    session = _session(rest, dremio)
    session.use(SOURCE)

    session.draft_to_version("v1", ["bw_assessment"]).commit()

    assert TARGET in rest.existing
    assert f"{TARGET}.v1.bw_assessment.assessments" in dremio.tables


def test_draft_to_version_uses_the_nearest_draft_in_the_context() -> None:
    rest, dremio = _world()
    nested = "catalog.bwd.draft.archive.draft"
    for folder in ("catalog.bwd.draft.archive", nested):
        rest.existing.add(folder)
        rest.folders.add(folder)
    dremio.tables[f"{nested}.t"] = "TABLE"
    rest.existing.add(f"{nested}.t")
    session = _session(rest, dremio)
    session.use(nested)

    session.draft_to_version("v1", ["t"]).commit()

    assert "catalog.bwd.draft.archive.versions.v1.t" in dremio.tables


def test_draft_to_version_without_a_draft_raises_at_once() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.use(TARGET)  # versions has no draft inside, and isn't inside one

    with pytest.raises(CatalogSessionError, match="no 'draft' folder for the context"):
        session.draft_to_version("v1")
    assert repr(session) == "CatalogSession(pending=0)"


def test_draft_to_version_validates_like_create_version() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.use(SOURCE)

    with pytest.raises(CatalogSessionError, match="version_name must not be empty"):
        session.draft_to_version("  ")


def test_draft_to_version_rollback_removes_the_version() -> None:
    rest, dremio = _world()
    session = _session(rest, dremio)
    session.use(SOURCE)

    session.draft_to_version("v2025_1", ["bw_assessment"])
    session.set_tags("catalog.bwd.missing", ["x"])

    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is True
    assert VERSION not in rest.existing
