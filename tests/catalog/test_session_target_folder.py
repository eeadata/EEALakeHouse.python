"""data_copy/data_move: a target ending in '/' is a folder, created first if missing."""

from __future__ import annotations

from pathlib import Path

import pytest

from eea_datalakehouse.catalog import retry_state
from eea_datalakehouse.catalog.client import Catalog
from eea_datalakehouse.catalog.session import (
    CatalogCommitError,
    CatalogSession,
    CatalogSessionError,
)

from .conftest import FakeCatalogRest
from .test_createversion import MiniDremio

AREA = "catalog.water.bwd"
SOURCE = f"{AREA}.draft.stations"


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", tmp_path / "state.json")


def _session() -> tuple[CatalogSession, FakeCatalogRest, MiniDremio]:
    folders = {"catalog", "catalog.water", AREA, f"{AREA}.draft"}
    rest = FakeCatalogRest(existing=set(folders), folders=set(folders))
    dremio = MiniDremio(rest, {SOURCE: "TABLE"})
    catalog = Catalog(
        "https://dremio.example.test",
        "pat",
        executor=dremio,  # type: ignore[arg-type]
        flight_executor=dremio,  # type: ignore[arg-type]
        catalog_rest=rest,  # type: ignore[arg-type]
    )
    return CatalogSession(catalog).use(AREA), rest, dremio


@pytest.mark.parametrize("verb", ["data_copy", "data_move"])
def test_a_trailing_slash_creates_the_folder_and_lands_inside_it(verb: str) -> None:
    session, rest, dremio = _session()

    report = getattr(session, verb)(SOURCE, "archive/2026/").commit()

    assert rest.created == [f"{AREA}.archive", f"{AREA}.archive.2026"]
    assert dremio.tables[f"{AREA}.archive.2026.stations"] == "TABLE"
    assert (SOURCE in dremio.tables) is (verb == "data_copy")
    assert report.succeeded == [f"{verb} {SOURCE!r} -> '{AREA}.archive.2026.stations'"]


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("./archive/", f"{AREA}.archive.stations"),
        (".archive/", f"{AREA}.archive.stations"),
        ("../archive/", "catalog.water.archive.stations"),
        ("catalog/water/bwd/archive/", f"{AREA}.archive.stations"),
        ("catalog.water.bwd.archive/", f"{AREA}.archive.stations"),
        ("./", f"{AREA}.stations"),
    ],
)
def test_the_folder_resolves_like_any_path(target: str, expected: str) -> None:
    session, _, dremio = _session()

    session.data_copy(SOURCE, target).commit()

    assert expected in dremio.tables


def test_an_existing_folder_is_used_as_is() -> None:
    session, rest, dremio = _session()
    rest.existing.add(f"{AREA}.archive")
    rest.folders.add(f"{AREA}.archive")

    session.data_copy(SOURCE, "archive/").commit()

    assert rest.created == []
    assert f"{AREA}.archive.stations" in dremio.tables


def test_without_a_slash_the_target_is_the_table_itself() -> None:
    session, rest, dremio = _session()

    session.data_copy(SOURCE, "stations_copy").commit()

    assert f"{AREA}.stations_copy" in dremio.tables
    assert rest.created == []


def test_a_bare_slash_is_refused() -> None:
    session, _, _ = _session()

    with pytest.raises(CatalogSessionError, match="names no folder"):
        session.data_copy(SOURCE, "/")


def test_rollback_drops_the_copy_and_the_folders_it_created() -> None:
    session, rest, dremio = _session()

    session.data_copy(SOURCE, "archive/2026/")
    session.set_tags("catalog.water.missing", ["x"])
    with pytest.raises(CatalogCommitError) as info:
        session.commit()

    assert info.value.rolled_back is True
    assert f"{AREA}.archive.2026.stations" not in dremio.tables
    assert f"{AREA}.archive" not in rest.existing


def test_rollback_of_a_move_moves_it_back_and_removes_the_folder() -> None:
    session, rest, dremio = _session()

    session.data_move(SOURCE, "archive/")
    session.set_tags("catalog.water.missing", ["x"])
    with pytest.raises(CatalogCommitError):
        session.commit()

    assert SOURCE in dremio.tables
    assert f"{AREA}.archive" not in rest.existing


def test_a_failed_copy_removes_the_folders_it_created() -> None:
    session, rest, dremio = _session()
    dremio.fail_on_create[f"{AREA}.archive.stations"] = RuntimeError("boom")

    with pytest.raises(CatalogCommitError, match="boom"):
        session.data_copy(SOURCE, "archive/").commit()

    assert f"{AREA}.archive" not in rest.existing
