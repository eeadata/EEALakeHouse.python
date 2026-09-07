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
        session.tag(".table1", ["reviewed"])


def test_touching_a_leaf_infers_context_for_a_later_relative_call() -> None:
    rest = FakeCatalogRest(
        existing={"a", "bwd", "bwd.reference.water_temperature", "bwd.reference.stations"}
    )
    session = CatalogSession(_catalog(rest))

    session.tag("bwd.reference.water_temperature", ["reviewed"])  # sets context to bwd.reference
    session.tag(".stations", ["reviewed"])  # resolves to bwd.reference.stations

    report = session.commit()

    assert report.succeeded == [
        "tag 'bwd.reference.water_temperature' with ['reviewed']",
        "tag 'bwd.reference.stations' with ['reviewed']",
    ]
    assert rest.get_tags("bwd.reference.stations") == ["reviewed"]


def test_set_context_seeds_it_explicitly_before_any_path_is_used() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference.water_temperature"})
    session = CatalogSession(_catalog(rest))

    session.set_context("bwd.reference")
    session.tag(".water_temperature", ["reviewed"])

    report = session.commit()

    assert report.succeeded == ["tag 'bwd.reference.water_temperature' with ['reviewed']"]


def test_create_folder_context_becomes_the_folder_itself_not_its_parent() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.reference.water_temperature"})
    session = CatalogSession(_catalog(rest))

    session.create_folder("bwd.reference.2027", create_parents=True)  # context -> that folder
    session.set_context("bwd.reference")  # re-seed for the rest of this test
    session.tag(".water_temperature", ["reviewed"])

    report = session.commit()

    assert report.succeeded == [
        "create folder 'bwd.reference.2027'",
        "tag 'bwd.reference.water_temperature' with ['reviewed']",
    ]


def test_copy_resolves_both_paths_against_the_same_starting_context() -> None:
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
    session.copy(".raw_2026", "bwd.reference.water_temperature")  # target does NOT use bwd.draft
    session.tag(".stations", ["reviewed"])  # resolves against bwd.reference (the target's parent)

    report = session.commit()

    assert report.succeeded == [
        "copy 'bwd.draft.raw_2026' -> 'bwd.reference.water_temperature'",
        "tag 'bwd.reference.stations' with ['reviewed']",
    ]
