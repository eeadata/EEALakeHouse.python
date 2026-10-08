"""CatalogSession's read-only queries (get_wiki, get_tags, list, schema) and
delete verbs (delete_view, delete_table).

See src/eea_datalakehouse/catalog/session.py — the read-only queries are
answered immediately (never queued); delete_view/delete_table are queued
like every other mutating verb, but — unlike the idempotent
`Catalog.deleteview`/`deletetable` they wrap — require `path` to already
exist.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from eea_datalakehouse.catalog import retry_state
from eea_datalakehouse.catalog.client import Catalog
from eea_datalakehouse.catalog.errors import CatalogOperationError
from eea_datalakehouse.catalog.operations import TableInfo
from eea_datalakehouse.catalog.session import (
    CatalogCommitError,
    CatalogSession,
    CatalogSessionError,
)

from .conftest import FakeCatalogRest, FakeExecutor

BASE_URL = "https://dremio.example.test"


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", tmp_path / "state.json")


def _catalog(rest: FakeCatalogRest, executor: FakeExecutor | None = None) -> Catalog:
    executor = executor or FakeExecutor()
    return Catalog(BASE_URL, "pat", executor=executor, flight_executor=executor, catalog_rest=rest)


def _session(rest: FakeCatalogRest, executor: FakeExecutor | None = None) -> CatalogSession:
    """A `CatalogSession` with context cleared. This file's fixture paths
    are short absolute stand-ins (e.g. `"bwd.table1"`) that predate
    `CatalogSession`'s own default context (the catalog root, per
    `_ROOT_SOURCE`) — clearing it keeps them absolute rather than
    silently getting `"catalog."` prepended."""
    session = CatalogSession(_catalog(rest, executor))
    session.set_context(None)
    return session


# -- get_wiki -----------------------------------------------------------------


def test_get_wiki_returns_the_wiki_text() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"}, wikis={"bwd.table1": "hello"})
    session = _session(rest)

    assert session.get_wiki("bwd.table1") == "hello"


def test_get_wiki_resolves_a_relative_path() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference", "bwd.reference.table1"},
        wikis={"bwd.reference.table1": "hi"},
    )
    session = _session(rest)
    session.use("bwd.reference")

    assert session.get_wiki(".table1") == "hi"


def test_get_wiki_raises_when_path_does_not_exist() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)

    with pytest.raises(CatalogOperationError, match="does not exist"):
        session.get_wiki("bwd.missing")


def test_get_wiki_does_not_queue_anything() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"}, wikis={"bwd.table1": "hi"})
    session = _session(rest)

    session.get_wiki("bwd.table1")

    assert repr(session) == "CatalogSession(pending=0)"


# -- get_tags -------------------------------------------------------------------


def test_get_tags_returns_the_tags() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"}, tags={"bwd.table1": ["reviewed"]})
    session = _session(rest)

    assert session.get_tags("bwd.table1") == ["reviewed"]


def test_get_tags_raises_when_path_does_not_exist() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)

    with pytest.raises(CatalogOperationError, match="does not exist"):
        session.get_tags("bwd.missing")


def test_get_tags_raises_on_a_folder() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.folder1"}, folders={"bwd.folder1"})
    session = _session(rest)

    with pytest.raises(CatalogOperationError, match="only works on tables/views"):
        session.get_tags("bwd.folder1")


# -- list -----------------------------------------------------------------------
# list() walks the catalog tree through the REST catalog API (no SQL) — see
# CatalogRestClient.list_datasets; FakeCatalogRest.list_datasets is its stand-in.


def _tree() -> FakeCatalogRest:
    folders = {"bwd", "bwd.reference", "bwd.reference.sub", "bwd.reference.sub.deeper"}
    tables = {
        "bwd.reference.water_temperature",
        "bwd.reference.sub.stations",
        "bwd.reference.sub.deeper.codes",
    }
    return FakeCatalogRest(existing={"a", *folders, *tables}, folders=folders)


def test_list_walks_every_child_folder_and_returns_full_paths() -> None:
    executor = FakeExecutor()
    session = _session(_tree(), executor)

    assert session.list("bwd.reference") == [
        "bwd.reference.sub.deeper.codes",
        "bwd.reference.sub.stations",
        "bwd.reference.water_temperature",
    ]
    assert executor.statements == []  # navigation only — no SQL at all


def test_list_resolves_a_relative_path() -> None:
    session = _session(_tree())
    session.use("bwd")

    assert "bwd.reference.water_temperature" in session.list(".reference")


def test_list_resolves_a_bare_relative_path_too() -> None:
    # list() (and every other single-path verb) treats a bare path with no
    # leading '.' as relative once a context exists — the dot is optional
    # sugar there, not the marker for relative vs absolute (use() is the
    # one exception — see test_session_context.py).
    session = _session(_tree())
    session.use("bwd")

    assert "bwd.reference.sub.stations" in session.list("reference")


def test_list_with_no_path_lists_the_context_itself() -> None:
    session = _session(_tree())
    session.use("bwd.reference.sub")

    assert session.list() == ["bwd.reference.sub.deeper.codes", "bwd.reference.sub.stations"]


def test_list_with_empty_path_lists_the_context_itself() -> None:
    session = _session(_tree())
    session.use("bwd.reference.sub.deeper")

    assert session.list("") == ["bwd.reference.sub.deeper.codes"]


def test_list_with_no_path_and_no_context_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)

    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.list()


def test_list_absolute_path_still_works_with_no_context() -> None:
    session = _session(_tree())

    assert session.list("bwd.reference.sub.deeper") == ["bwd.reference.sub.deeper.codes"]


def test_list_raises_when_path_does_not_exist() -> None:
    session = _session(_tree())

    with pytest.raises(CatalogOperationError, match="does not exist"):
        session.list("bwd.missing")


def test_list_accepts_the_slash_form_of_a_path() -> None:
    session = _session(_tree())

    assert session.list("bwd/reference/sub/deeper") == ["bwd.reference.sub.deeper.codes"]


def test_a_slash_context_from_the_catalog_tree_is_stored_with_dots() -> None:
    session = _session(_tree())

    session.set_context("bwd/reference/sub")  # what the JupyterLab tree pushes

    assert session.get_context() == "bwd.reference.sub"
    assert session.list() == ["bwd.reference.sub.deeper.codes", "bwd.reference.sub.stations"]


def test_use_accepts_the_slash_form_too() -> None:
    session = _session(_tree())

    session.use("bwd/reference")

    assert session.get_context() == "bwd.reference"


# -- schema ---------------------------------------------------------------------


def test_schema_returns_table_info() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    executor = FakeExecutor(
        rows_sequence=[[{"COLUMN_NAME": "id", "DATA_TYPE": "INTEGER"}], [{"row_count": 5}]]
    )
    session = _session(rest, executor)

    assert session.schema("bwd.table1") == TableInfo(schema={"id": "INTEGER"}, row_count=5)


def test_schema_raises_on_a_folder() -> None:
    # The explicit requirement: schema() only works on a table or view.
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.folder1"}, folders={"bwd.folder1"})
    session = _session(rest)

    with pytest.raises(CatalogOperationError, match="only works on tables/views"):
        session.schema("bwd.folder1")


def test_schema_raises_when_path_does_not_exist() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)

    with pytest.raises(CatalogOperationError, match="does not exist"):
        session.schema("bwd.missing")


# -- delete_view -------------------------------------------------------------------


def test_delete_view_drops_the_view() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.view1"})
    executor = FakeExecutor()
    session = _session(rest, executor)

    session.delete_view("bwd.view1")
    report = session.commit()

    assert report.succeeded == ["delete view 'bwd.view1'"]
    assert executor.statements == ['DROP VIEW IF EXISTS "bwd"."view1"']


def test_delete_view_resolves_a_relative_path() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.reference.view1"})
    executor = FakeExecutor()
    session = _session(rest, executor)
    session.use("bwd.reference")

    session.delete_view(".view1")
    report = session.commit()

    assert report.succeeded == ["delete view 'bwd.reference.view1'"]


def test_delete_view_raises_when_path_does_not_exist() -> None:
    # Unlike the raw Catalog.deleteview (DROP VIEW IF EXISTS — a no-op on a
    # missing path), CatalogSession.delete_view requires it to be real.
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)
    session.delete_view("bwd.missing")

    with pytest.raises(CatalogCommitError, match="does not exist"):
        session.commit()


def test_delete_view_is_never_reversible() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.view1"})
    session = _session(rest)

    session.delete_view("bwd.view1")  # succeeds, but not reversible
    session.set_tags("bwd.missing", ["x"])  # fails: path does not exist

    with pytest.raises(CatalogCommitError) as exc_info:
        session.commit()

    error = exc_info.value
    assert error.rolled_back is False
    assert len(error.unresolved) == 1
    assert "cannot be recreated" in error.unresolved[0]


# -- delete_table ------------------------------------------------------------------


def test_delete_table_drops_the_table() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    executor = FakeExecutor()
    session = _session(rest, executor)

    session.delete_table("bwd.table1")
    report = session.commit()

    assert report.succeeded == ["delete table 'bwd.table1'"]
    assert executor.statements == ['DROP TABLE IF EXISTS "bwd"."table1"']


def test_delete_table_raises_when_path_does_not_exist() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)
    session.delete_table("bwd.missing")

    with pytest.raises(CatalogCommitError, match="does not exist"):
        session.commit()


def test_delete_table_is_never_reversible() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    session = _session(rest)

    session.delete_table("bwd.table1")  # succeeds, but not reversible
    session.set_tags("bwd.missing", ["x"])  # fails: path does not exist

    with pytest.raises(CatalogCommitError) as exc_info:
        session.commit()

    error = exc_info.value
    assert error.rolled_back is False
    assert len(error.unresolved) == 1
    assert "cannot be recreated" in error.unresolved[0]
