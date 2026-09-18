"""CatalogSession's "current path" context: relative paths (a leading `.`),
set only via `use()`/`set_context()` — no other verb ever changes it, even
though most resolve their own `path` against it.

See src/eea_datalakehouse/catalog/session.py's `use`/`set_context`/
`_resolve_path` and docs/notebook-facade-for-data-scientists.md
("Pre-filling catalog context").
"""

from __future__ import annotations

from pathlib import Path

import pytest

from eea_datalakehouse.catalog import retry_state
from eea_datalakehouse.catalog.client import Catalog
from eea_datalakehouse.catalog.session import CatalogSession, CatalogSessionError

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
    silently getting `"catalog."` prepended. Tests that exercise the
    "no context set yet" error paths, or the new default itself, construct
    a plain `CatalogSession(_catalog(rest))` directly instead."""
    session = CatalogSession(_catalog(rest, executor))
    session.set_context(None)
    return session


def test_new_session_defaults_context_to_the_catalog_root() -> None:
    # A freshly built session already has something to resolve a relative
    # path against — the catalog root — rather than None; use()/
    # set_context(None) are still the only way to clear it explicitly.
    rest = FakeCatalogRest(existing={"a"})
    session = CatalogSession(_catalog(rest))

    assert session.get_context() == "catalog"


def test_relative_path_without_any_context_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    session = _session(rest)

    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.set_tags(".table1", ["reviewed"])


def test_ordinary_verbs_never_change_or_infer_the_context() -> None:
    # Only use()/set_context() may change context — set_tags below fully
    # resolves an absolute path, but that never becomes the new context
    # (unlike the old "ordinary use keeps context current" behaviour), so
    # a later relative call still has nothing to resolve against.
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference.water_temperature", "bwd.reference.stations"}
    )
    session = _session(rest)

    session.set_tags("bwd.reference.water_temperature", ["reviewed"])

    assert session.get_context() is None
    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.set_tags(".stations", ["reviewed"])


def test_bare_path_starting_with_root_source_stays_absolute_even_with_context_set() -> None:
    # "catalog" is this deployment's one real top-level source (_ROOT_SOURCE) — a
    # bare path starting with it is unambiguous, so it's never appended to an
    # existing context the way an ordinary bare name ("stations" above) is.
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.reference",
            "catalog",
            "catalog.other_root",
            "catalog.other_root.table1",
        }
    )
    session = CatalogSession(_catalog(rest))

    session.set_context("bwd.reference")
    session.set_tags("catalog.other_root.table1", ["reviewed"])

    report = session.commit()

    assert report.succeeded == ["set tags ['reviewed'] on 'catalog.other_root.table1'"]


def test_bare_path_equal_to_root_source_stays_absolute_even_with_context_set() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "catalog"})
    session = CatalogSession(_catalog(rest))

    session.set_context("bwd.reference")
    session.set_tags("catalog", ["reviewed"])

    report = session.commit()

    assert report.succeeded == ["set tags ['reviewed'] on 'catalog'"]


def test_set_context_seeds_it_explicitly_before_any_path_is_used() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference.water_temperature"})
    session = CatalogSession(_catalog(rest))

    session.set_context("bwd.reference")
    session.set_tags(".water_temperature", ["reviewed"])

    report = session.commit()

    assert report.succeeded == ["set tags ['reviewed'] on 'bwd.reference.water_temperature'"]


def test_use_with_a_whole_path_sets_context_like_set_context() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference", "bwd.reference.water_temperature"}
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.set_tags(".water_temperature", ["reviewed"])

    report = session.commit()

    assert report.succeeded == ["set tags ['reviewed'] on 'bwd.reference.water_temperature'"]


def test_use_never_resolves_a_leading_dot_relatively() -> None:
    # use() takes path literally as a whole, absolute path — unlike every
    # other verb, a leading '.' means nothing special here; it's just part
    # of a literal string that (in this case) doesn't exist in the catalog.
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference", "bwd.reference.2027"},
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    with pytest.raises(CatalogSessionError, match="does not exist in the catalog"):
        session.use(".2027")  # NOT resolved against 'bwd.reference' — taken literally

    assert session.get_context() == "bwd.reference"  # unchanged after the failed check


def test_use_always_takes_a_bare_path_literally() -> None:
    # Whether or not a context already exists, a bare path passed to use()
    # is always the whole, absolute path — never relative to it.
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.other"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd")
    assert session.get_context() == "bwd"

    session.use("bwd.other")  # NOT relative to 'bwd' — still a literal, whole path
    assert session.get_context() == "bwd.other"


def test_use_none_or_empty_resets_context_to_the_catalog_root() -> None:
    # Unlike set_context(None) (still "no context at all"), use(None)/
    # use("") reset to _ROOT_SOURCE — the same starting point a freshly
    # built session already has (see test_new_session_defaults_context_to_the_catalog_root).
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.other"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.use(None)

    assert session.get_context() == "catalog"

    session.use("bwd.other")
    session.use("")

    assert session.get_context() == "catalog"


def test_set_context_none_still_clears_context_entirely() -> None:
    # set_context (the lower-level primitive, not normally called by a data
    # custodian directly) keeps the old "no context at all" behaviour —
    # only use()'s own None/"" handling changed.
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.set_context(None)

    assert session.get_context() is None


def test_get_context_is_unaffected_by_other_verbs() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference.water_temperature"})
    session = _session(rest)

    assert session.get_context() is None

    session.set_tags("bwd.reference.water_temperature", ["reviewed"])

    assert session.get_context() is None


def test_create_folder_does_not_change_the_context() -> None:
    # The exact case reported: create_folder used to leave its own path as
    # the new context (a deliberate design choice at the time), which then
    # surprised a custodian who only ever wanted that to come from use().
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)

    session.create_folder("bwd.reference.2027", create_parents=True)

    assert session.get_context() is None

    session.use("bwd")
    session.create_folder(".reference")  # relative to the context use() set

    assert session.get_context() == "bwd"  # still what use() set — create_folder didn't touch it


def test_create_folder_and_delete_folder_resolve_a_bare_relative_path() -> None:
    # Same dot-optional resolution as every other single-path verb: a bare
    # path with no leading '.' still appends to the context once one exists.
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.create_folder("2027")  # bare, no dot — still bwd.reference.2027
    session.delete_folder("2027")

    report = session.commit()

    assert report.succeeded == [
        "create folder 'bwd.reference.2027'",
        "delete folder 'bwd.reference.2027'",
    ]


def test_create_folder_and_delete_folder_keep_a_root_source_path_absolute() -> None:
    # "catalog" is this deployment's one real top-level source (_ROOT_SOURCE)
    # — a bare path starting with it is unambiguous, so it's never appended
    # to an existing context (see test_bare_path_starting_with_root_source_
    # stays_absolute_even_with_context_set for the general rule).
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "catalog", "catalog.other_root"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.create_folder("catalog.other_root.new_folder")
    session.delete_folder("catalog.other_root.new_folder")

    report = session.commit()

    assert report.succeeded == [
        "create folder 'catalog.other_root.new_folder'",
        "delete folder 'catalog.other_root.new_folder'",
    ]


def test_queued_write_verbs_resolve_relative_and_root_source_absolute_paths() -> None:
    # set_wiki/delete_wiki/set_tags/delete_tags/delete_view/delete_table all
    # share the exact same _resolve_path call as every other verb — a bare
    # relative name appends to context, a catalog.-prefixed path stays
    # absolute even with a context set.
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.reference",
            "bwd.reference.table1",
            "bwd.reference.view1",
            "catalog",
            "catalog.other_root",
            "catalog.other_root.table1",
            "catalog.other_root.view1",
        }
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.set_wiki("table1", "hello")  # relative
    session.set_wiki("catalog.other_root.table1", "hello, absolute")  # absolute
    session.delete_wiki("table1")
    session.delete_wiki("catalog.other_root.table1")
    session.set_tags("table1", ["x"])
    session.set_tags("catalog.other_root.table1", ["x-abs"])
    session.delete_tags("table1", ["x"])
    session.delete_tags("catalog.other_root.table1", ["x-abs"])
    session.delete_view("view1")
    session.delete_view("catalog.other_root.view1")
    session.delete_table("table1")
    session.delete_table("catalog.other_root.table1")

    report = session.commit()

    assert report.succeeded == [
        "set wiki on 'bwd.reference.table1'",
        "set wiki on 'catalog.other_root.table1'",
        "delete wiki on 'bwd.reference.table1'",
        "delete wiki on 'catalog.other_root.table1'",
        "set tags ['x'] on 'bwd.reference.table1'",
        "set tags ['x-abs'] on 'catalog.other_root.table1'",
        "delete tags ['x'] from 'bwd.reference.table1'",
        "delete tags ['x-abs'] from 'catalog.other_root.table1'",
        "delete view 'bwd.reference.view1'",
        "delete view 'catalog.other_root.view1'",
        "delete table 'bwd.reference.table1'",
        "delete table 'catalog.other_root.table1'",
    ]


def test_read_only_verbs_resolve_relative_and_root_source_absolute_paths() -> None:
    # get_wiki/get_tags aren't queued, but resolve their own path the same
    # dot-optional, root-source-aware way as every write verb.
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.reference",
            "bwd.reference.table1",
            "catalog",
            "catalog.other_root",
            "catalog.other_root.table1",
        },
        wikis={"bwd.reference.table1": "hi", "catalog.other_root.table1": "hi, absolute"},
        tags={"bwd.reference.table1": ["x"], "catalog.other_root.table1": ["x-abs"]},
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")

    assert session.get_wiki("table1") == "hi"  # relative
    assert session.get_wiki("catalog.other_root.table1") == "hi, absolute"  # absolute
    assert session.get_tags("table1") == ["x"]  # relative
    assert session.get_tags("catalog.other_root.table1") == ["x-abs"]  # absolute


def test_data_copy_resolves_both_paths_against_the_current_context() -> None:
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.draft.raw_2026",
            "catalog",
            "catalog.other_root",
            "catalog.other_root.water_temperature",
        }
    )
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "raw_2026"}],  # source exists
            [],  # target does not exist yet
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.set_context("bwd.draft")
    session.data_copy(".raw_2026", "catalog.other_root.water_temperature")  # NOT under bwd.draft

    report = session.commit()

    assert report.succeeded == [
        "data_copy 'bwd.draft.raw_2026' -> 'catalog.other_root.water_temperature'",
    ]
    assert session.get_context() == "bwd.draft"  # data_copy never changes it


def test_data_copy_resolves_a_bare_relative_path_for_both_paths() -> None:
    # data_copy/data_move/create_view are no longer an exception to the
    # dot-optional rule — a bare path resolves relative to context here
    # too, same as every other verb, since a genuinely absolute path in
    # this deployment always starts with _ROOT_SOURCE and so is never
    # ambiguous with a deliberately relative one in the same call.
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.reference.raw_2026"})
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "raw_2026"}],  # source exists
            [],  # target does not exist yet
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.use("bwd.reference")
    session.data_copy("raw_2026", "archive")  # bare, no dot — both resolve under bwd.reference

    report = session.commit()

    assert report.succeeded == [
        "data_copy 'bwd.reference.raw_2026' -> 'bwd.reference.archive'",
    ]


def test_data_move_resolves_both_paths_against_the_current_context() -> None:
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.draft.raw_2026",
            "catalog",
            "catalog.other_root",
            "catalog.other_root.water_temperature",
        }
    )
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "raw_2026"}],  # source exists
            [],  # target does not exist yet
            [{"TABLE_NAME": "water_temperature"}],  # verify target exists after the move
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.set_context("bwd.draft")
    session.data_move(".raw_2026", "catalog.other_root.water_temperature")  # NOT under bwd.draft

    report = session.commit()

    assert report.succeeded == [
        "data_move 'bwd.draft.raw_2026' -> 'catalog.other_root.water_temperature'",
    ]
    assert session.get_context() == "bwd.draft"  # data_move never changes it


def test_data_move_resolves_a_bare_relative_path_for_both_paths() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.reference.raw_2026"})
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "raw_2026"}],  # source exists
            [],  # target does not exist yet
            [{"TABLE_NAME": "archive"}],  # verify target exists after the move
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.use("bwd.reference")
    session.data_move("raw_2026", "archive")  # bare, no dot — both resolve under bwd.reference

    report = session.commit()

    assert report.succeeded == [
        "data_move 'bwd.reference.raw_2026' -> 'bwd.reference.archive'",
    ]


def test_create_view_resolves_both_paths_against_the_current_context() -> None:
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.draft.raw_2026",
            "catalog",
            "catalog.other_root",
            "catalog.other_root.water_temperature",
        }
    )
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "raw_2026"}],  # source exists
            [],  # target does not exist yet
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.set_context("bwd.draft")
    session.create_view(".raw_2026", "catalog.other_root.water_temperature")  # target NOT bwd.draft

    report = session.commit()

    assert report.succeeded == [
        "create view 'bwd.draft.raw_2026' -> 'catalog.other_root.water_temperature'",
    ]
    assert session.get_context() == "bwd.draft"  # create_view never changes it


def test_create_view_resolves_a_bare_relative_path_for_both_paths() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.reference.raw_2026"})
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "raw_2026"}],  # source exists
            [],  # target does not exist yet
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.use("bwd.reference")
    session.create_view("raw_2026", "archive")  # bare, no dot — both resolve under bwd.reference

    report = session.commit()

    assert report.succeeded == [
        "create view 'bwd.reference.raw_2026' -> 'bwd.reference.archive'",
    ]


def test_bare_dot_resolves_to_the_context_itself() -> None:
    # "." alone used to append a stray trailing "." to the context instead
    # of meaning the context itself — get_wiki (a read-only verb, so the
    # resolved path is directly observable rather than via a step
    # description) proves the fix rather than just _resolve_path directly.
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference"}, wikis={"bwd.reference": "hi"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")

    assert session.get_wiki(".") == "hi"
    assert session.get_wiki("./") == "hi"


def test_bare_dot_with_no_context_yet_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)

    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.get_wiki(".")


def test_dot_slash_name_means_the_same_as_dot_name() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference", "bwd.reference.water_temperature"},
        wikis={"bwd.reference.water_temperature": "hi"},
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")

    assert session.get_wiki("./water_temperature") == "hi"


def test_bare_dotdot_resolves_to_the_parent_of_the_context() -> None:
    # use() no longer accepts '..' at all (see test_use_never_resolves_a_
    # leading_dot_relatively) — create_folder exercises the same
    # _resolve_path/_resolve_parent_path machinery every other verb shares.
    # It never changes context though, so the resolution is checked via
    # the committed step's own description, not get_context().
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.reference.2027"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference.2027")
    session.create_folder("..")

    report = session.commit()

    assert report.succeeded == ["create folder 'bwd.reference'"]
    assert session.get_context() == "bwd.reference.2027"  # unchanged — still what use() set


def test_dotdot_slash_name_resolves_to_a_sibling_of_the_context() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference.2027", "bwd.reference.2027.stations"}
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference.2027.stations")
    session.create_folder("../water_temp")  # sibling of the current context

    report = session.commit()

    assert report.succeeded == ["create folder 'bwd.reference.2027.water_temp'"]
    assert session.get_context() == "bwd.reference.2027.stations"  # unchanged


def test_chained_dotdot_walks_up_multiple_levels() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference", "bwd.reference.2027.stations", "bwd.archive"}
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference.2027.stations")
    session.create_folder("../../../archive")  # up three levels, then into "archive"
    session.create_folder("../..")  # up two levels, no name after it — still off the same context

    report = session.commit()

    assert report.succeeded == [
        "create folder 'bwd.archive'",
        "create folder 'bwd.reference'",
    ]
    assert session.get_context() == "bwd.reference.2027.stations"  # unchanged throughout


def test_dotdot_past_the_top_of_the_context_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd")

    with pytest.raises(CatalogSessionError, match="goes above the top"):
        session.create_folder("..")


def test_dotdot_with_no_context_yet_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = _session(rest)

    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.create_folder("../reference")


def test_dotdot_works_for_ordinary_verbs() -> None:
    # ../ is a general path-resolution feature (_resolve_path), available
    # to every verb except use(), which only takes whole, absolute paths.
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.reference",
            "bwd.reference.2027",
            "bwd.reference.water_temperature",
        }
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference.2027")
    session.set_tags("../water_temperature", ["reviewed"])  # ../ from a plain verb's path arg

    report = session.commit()

    assert report.succeeded == ["set tags ['reviewed'] on 'bwd.reference.water_temperature'"]
    assert session.get_context() == "bwd.reference.2027"  # set_tags never changes it


def test_use_raises_when_the_resolved_path_does_not_exist() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    with pytest.raises(CatalogSessionError, match="does not exist in the catalog"):
        session.use("bwd.nonexistent")


def test_use_leaves_context_unchanged_after_a_failed_check() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    with pytest.raises(CatalogSessionError, match="does not exist"):
        session.use("bwd.nonexistent")

    assert session.get_context() == "bwd.reference"  # still the last good value


def test_use_accepts_a_folder_not_just_a_table_or_view() -> None:
    # exists() (CatalogRestClient) sees any entity type — unlike the
    # INFORMATION_SCHEMA-based checks other verbs use, which only see
    # tables/views. use() should work against a bare folder.
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference"}, folders={"bwd.reference"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")

    assert session.get_context() == "bwd.reference"


def test_use_wraps_a_stalled_existence_check_as_a_catalog_session_error() -> None:
    from eea_datalakehouse.catalog.errors import EngineStartingError

    rest = FakeCatalogRest(
        existing={"a", "bwd"},
        raise_on_exists=EngineStartingError("engine starting", idempotency_key="k"),
    )
    session = CatalogSession(_catalog(rest))

    # Not a raw EngineStartingError escaping — %catalog only catches
    # CatalogSessionError (see notebook/magics.py's _dispatch), so this must
    # come back wrapped or it would surface as an ugly traceback instead of
    # a friendly printed message.
    with pytest.raises(CatalogSessionError, match="could not check whether"):
        session.use("bwd.reference")
