"""CatalogSession's "current path" context: relative paths (a leading `.`),
auto-inferred from usage, `set_context()` as an explicit seed.

See src/eea_datalakehouse/catalog/session.py's `_resolve`/`_resolve_path`
and docs/notebook-facade-for-data-scientists.md ("Pre-filling catalog
context").
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


def test_relative_path_without_any_context_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    session = CatalogSession(_catalog(rest))

    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.set_tags(".table1", ["reviewed"])


def test_touching_a_leaf_infers_context_for_a_later_relative_call() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference.water_temperature", "bwd.reference.stations"}
    )
    session = CatalogSession(_catalog(rest))

    session.set_tags("bwd.reference.water_temperature", ["reviewed"])  # -> bwd.reference
    session.set_tags(".stations", ["reviewed"])  # resolves to bwd.reference.stations

    report = session.commit()

    assert report.succeeded == [
        "set tags ['reviewed'] on 'bwd.reference.water_temperature'",
        "set tags ['reviewed'] on 'bwd.reference.stations'",
    ]
    assert rest.get_tags("bwd.reference.stations") == ["reviewed"]


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


def test_use_with_a_relative_path_resolves_against_the_existing_context() -> None:
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.reference",
            "bwd.reference.2027",
            "bwd.reference.2027.water_temperature",
        }
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.use(".2027")  # narrows bwd.reference -> bwd.reference.2027
    session.set_tags(".water_temperature", ["reviewed"])

    report = session.commit()

    assert report.succeeded == ["set tags ['reviewed'] on 'bwd.reference.2027.water_temperature'"]


def test_use_with_a_relative_path_and_no_context_yet_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.use(".reference")


def test_use_bare_path_resolves_relative_once_context_exists() -> None:
    # No leading '.' needed once inside a context — "2027" and ".2027" mean
    # the same thing here (unlike every other verb's path arguments, which
    # always require the dot to mean relative).
    rest = FakeCatalogRest(
        existing={
            "a",
            "bwd",
            "bwd.reference",
            "bwd.reference.2027",
            "bwd.reference.2027.water_temperature",
        }
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.use("2027")  # bare, no dot — still narrows to bwd.reference.2027
    session.set_tags(".water_temperature", ["reviewed"])

    report = session.commit()

    assert report.succeeded == ["set tags ['reviewed'] on 'bwd.reference.2027.water_temperature'"]


def test_use_bare_path_with_no_context_yet_is_absolute() -> None:
    # The very first use() has nothing to be relative to, so a bare path is
    # unambiguous: it must be the whole path.
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd")

    assert session.get_context() == "bwd"


def test_use_none_clears_context_like_set_context() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.other"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference")
    session.use(None)

    assert session.get_context() is None
    # Cleared, so a bare path is absolute again rather than relative to
    # whatever used to be there.
    session.use("bwd.other")
    assert session.get_context() == "bwd.other"


def test_get_context_reflects_state_set_by_other_verbs() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference.water_temperature"})
    session = CatalogSession(_catalog(rest))

    assert session.get_context() is None

    session.set_tags("bwd.reference.water_temperature", ["reviewed"])  # auto-updates context

    assert session.get_context() == "bwd.reference"


def test_create_folder_context_becomes_the_folder_itself_not_its_parent() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference.water_temperature"})
    session = CatalogSession(_catalog(rest))

    session.create_folder("bwd.reference.2027", create_parents=True)  # context -> that folder
    session.set_context("bwd.reference")  # re-seed for the rest of this test
    session.set_tags(".water_temperature", ["reviewed"])

    report = session.commit()

    assert report.succeeded == [
        "create folder 'bwd.reference.2027'",
        "set tags ['reviewed'] on 'bwd.reference.water_temperature'",
    ]


def test_data_copy_resolves_both_paths_against_the_same_starting_context() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.draft.raw_2026", "bwd.reference", "bwd.reference.stations"}
    )
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "raw_2026"}],  # source exists
            [],  # target does not exist yet
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.set_context("bwd.draft")
    session.data_copy(".raw_2026", "bwd.reference.water_temperature")  # target NOT under bwd.draft
    session.set_tags(".stations", ["reviewed"])  # resolves against the target's parent

    report = session.commit()

    assert report.succeeded == [
        "data_copy 'bwd.draft.raw_2026' -> 'bwd.reference.water_temperature'",
        "set tags ['reviewed'] on 'bwd.reference.stations'",
    ]


def test_data_move_resolves_both_paths_against_the_same_starting_context() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.draft.raw_2026", "bwd.reference", "bwd.reference.stations"}
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
    session.data_move(".raw_2026", "bwd.reference.water_temperature")  # target NOT under bwd.draft
    session.set_tags(".stations", ["reviewed"])  # resolves against the target's parent

    report = session.commit()

    assert report.succeeded == [
        "data_move 'bwd.draft.raw_2026' -> 'bwd.reference.water_temperature'",
        "set tags ['reviewed'] on 'bwd.reference.stations'",
    ]


def test_create_view_resolves_both_paths_against_the_same_starting_context() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.draft.raw_2026", "bwd.reference", "bwd.reference.stations"}
    )
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "raw_2026"}],  # source exists
            [],  # target does not exist yet
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.set_context("bwd.draft")
    session.create_view(".raw_2026", "bwd.reference.water_temperature")  # target NOT bwd.draft
    session.set_tags(".stations", ["reviewed"])  # resolves against the target's parent

    report = session.commit()

    assert report.succeeded == [
        "create view 'bwd.draft.raw_2026' -> 'bwd.reference.water_temperature'",
        "set tags ['reviewed'] on 'bwd.reference.stations'",
    ]


def test_bare_dotdot_resolves_to_the_parent_of_the_context() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.reference.2027"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference.2027")
    session.use("..")

    assert session.get_context() == "bwd.reference"


def test_dotdot_slash_name_resolves_to_a_sibling_of_the_context() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference.2027.stations", "bwd.reference.2027.water_temp"}
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference.2027.stations")
    session.use("../water_temp")  # sibling of the current context

    assert session.get_context() == "bwd.reference.2027.water_temp"


def test_chained_dotdot_walks_up_multiple_levels() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference", "bwd.reference.2027.stations", "bwd.archive"}
    )
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference.2027.stations")
    session.use("../../../archive")  # up three levels, then into "archive"

    assert session.get_context() == "bwd.archive"

    # A bare absolute path is only absolute again once the context is
    # cleared — see use()'s own docstring on this tradeoff.
    session.use(None)
    session.use("bwd.reference.2027.stations")
    session.use("../..")  # up two levels, no name after it

    assert session.get_context() == "bwd.reference"


def test_dotdot_past_the_top_of_the_context_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd")

    with pytest.raises(CatalogSessionError, match="goes above the top"):
        session.use("..")


def test_dotdot_with_no_context_yet_raises() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    with pytest.raises(CatalogSessionError, match="no context is set"):
        session.use("../reference")


def test_dotdot_works_for_verbs_other_than_use_too() -> None:
    # ../ is a general path-resolution feature (_resolve_path), not
    # something special-cased inside use() alone.
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


def test_get_context_after_dotdot_is_the_full_resolved_path() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference", "bwd.reference.2027"})
    session = CatalogSession(_catalog(rest))

    session.use("bwd.reference.2027")
    session.use("..")

    context = session.get_context()
    assert context == "bwd.reference"
    assert not context.startswith(".")  # never a raw relative fragment


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
        session.use(".nonexistent")

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
