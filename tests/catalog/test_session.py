"""CatalogSession: queue catalog verbs, commit as one all-or-nothing batch.

See src/eea_datalakehouse/catalog/session.py and
docs/notebook-facade-for-data-scientists.md ("Queue calls, then commit").
"""

from __future__ import annotations

from pathlib import Path

import pytest

from eea_datalakehouse.catalog import retry_state
from eea_datalakehouse.catalog.client import Catalog
from eea_datalakehouse.catalog.errors import EngineStartingError
from eea_datalakehouse.catalog.session import CatalogCommitError, CatalogSession

from .conftest import FakeCatalogRest, FakeExecutor

BASE_URL = "https://dremio.example.test"


@pytest.fixture(autouse=True)
def _isolated_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(retry_state, "DEFAULT_STATE_PATH", tmp_path / "state.json")


def _catalog(rest: FakeCatalogRest, executor: FakeExecutor | None = None) -> Catalog:
    executor = executor or FakeExecutor()
    # data_copy/data_move always go over flight_executor — share one FakeExecutor
    # so a test can reason about one single call log regardless of which verb
    # made the call.
    return Catalog(BASE_URL, "pat", executor=executor, flight_executor=executor, catalog_rest=rest)


def test_nothing_runs_until_commit() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    session.create_folder("bwd.new")

    assert rest.created == []
    assert repr(session) == "CatalogSession(pending=1)"


def test_commit_runs_queued_steps_in_order_and_clears_the_queue() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    session = CatalogSession(_catalog(rest))

    session.create_folder("bwd.newfolder").set_tags("bwd.table1", ["reviewed"])
    report = session.commit()

    assert report.succeeded == [
        "create folder 'bwd.newfolder'",
        "set tags ['reviewed'] on 'bwd.table1'",
    ]
    assert rest.created == ["bwd.newfolder"]
    assert rest.get_tags("bwd.table1") == ["reviewed"]
    assert repr(session) == "CatalogSession(pending=0)"  # queue cleared


def test_commit_rolls_back_a_cleanly_reversible_batch_on_failure() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    executor = FakeExecutor(rows=[{"TABLE_NAME": "table1", "TABLE_TYPE": "TABLE"}])
    session = CatalogSession(_catalog(rest, executor))

    # step 1 succeeds and is reversible; step 2 fails because the target
    # already exists and overwrite defaults to False.
    session.create_folder("bwd.newfolder")
    session.data_copy("bwd.table1", "bwd.table1")

    with pytest.raises(CatalogCommitError) as exc_info:
        session.commit()

    error = exc_info.value
    assert error.rolled_back is True
    assert error.unresolved == []
    assert error.failed_step == "data_copy 'bwd.table1' -> 'bwd.table1'"
    assert rest.created == ["bwd.newfolder"]
    assert rest.deleted == ["bwd.newfolder"]  # step 1's undo ran
    assert repr(session) == "CatalogSession(pending=0)"  # a failed commit still clears the queue


def test_data_move_rollback_moves_the_table_back() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    # Six fetch_all calls in order: the forward move's source-exists,
    # target-empty, and verify-target-exists checks, then the same three
    # again for the undo's data_move back from table2 to table1.
    executor = FakeExecutor(
        rows_sequence=[
            [{"TABLE_NAME": "table1"}],
            [],
            [{"TABLE_NAME": "table2"}],
            [{"TABLE_NAME": "table2"}],
            [],
            [{"TABLE_NAME": "table1"}],
        ]
    )
    session = CatalogSession(_catalog(rest, executor))

    session.data_move("bwd.table1", "bwd.table2")  # reversible
    session.set_tags("bwd.missing", ["x"])  # fails: path does not exist

    with pytest.raises(CatalogCommitError) as exc_info:
        session.commit()

    error = exc_info.value
    assert error.rolled_back is True
    assert error.failed_step == "set tags ['x'] on 'bwd.missing'"  # step 2 is what actually failed
    # step 1 (the move) succeeded, then got undone: a data_move back from
    # table2 to table1, always as a TABLE — no re-detection needed, since
    # move can no longer create a VIEW either.
    assert 'CREATE TABLE "bwd"."table1" AS SELECT * FROM "bwd"."table2"' in executor.statements
    assert 'DROP TABLE "bwd"."table2"' in executor.statements


def test_create_view_rollback_drops_the_view() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    executor = FakeExecutor(
        rows_sequence=[[{"TABLE_NAME": "table1"}], []],  # source-exists, target-empty
        rows=[{"TABLE_TYPE": "VIEW"}],  # answers the undo's kind-detection lookup
    )
    session = CatalogSession(_catalog(rest, executor))

    session.create_view("bwd.table1", "bwd.view1")  # reversible — source is untouched either way
    session.set_tags("bwd.missing", ["x"])  # fails: path does not exist

    with pytest.raises(CatalogCommitError) as exc_info:
        session.commit()

    error = exc_info.value
    assert error.rolled_back is True
    assert error.failed_step == "set tags ['x'] on 'bwd.missing'"  # step 2 is what actually failed
    assert 'DROP VIEW IF EXISTS "bwd"."view1"' in executor.statements  # undo: drop what it created
    assert "bwd.table1" in rest.existing  # source was never touched


def test_commit_reports_what_it_could_not_undo() -> None:
    rest = FakeCatalogRest(existing={"a", "bwd", "bwd.table1"})
    executor = FakeExecutor(rows=[{"TABLE_NAME": "table1", "TABLE_TYPE": "TABLE"}])
    session = CatalogSession(_catalog(rest, executor))

    session.create_folder("bwd.newfolder")  # reversible
    session.data_copy("bwd.table1", "bwd.table1", overwrite=True)  # succeeds, but NOT reversible
    session.set_tags("bwd.missing", ["x"])  # fails: path does not exist

    with pytest.raises(CatalogCommitError) as exc_info:
        session.commit()

    error = exc_info.value
    assert error.rolled_back is False
    assert len(error.unresolved) == 1
    assert "overwrote an existing target" in error.unresolved[0]
    # the reversible step before the irreversible one is still cleaned up
    assert rest.deleted == ["bwd.newfolder"]


def test_retry_reruns_the_same_step_after_engine_starting_error() -> None:
    class _FlakyRest(FakeCatalogRest):
        def __init__(self, *args: object, **kwargs: object) -> None:
            super().__init__(*args, **kwargs)  # type: ignore[arg-type]
            self._create_folder_attempts = 0

        def create_folder(self, path: str) -> bool:
            self._create_folder_attempts += 1
            if self._create_folder_attempts == 1:
                raise EngineStartingError("engine starting", idempotency_key="k")
            return super().create_folder(path)

    rest = _FlakyRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    session.create_folder("bwd.new")
    report = session.commit(retry=True, retry_delay=0)

    assert report.succeeded == ["create folder 'bwd.new'"]
    assert rest.created == ["bwd.new"]
    assert rest._create_folder_attempts == 2


def test_engine_starting_error_without_retry_rolls_back_immediately() -> None:
    class _AlwaysStartingRest(FakeCatalogRest):
        def create_folder(self, path: str) -> bool:
            raise EngineStartingError("engine starting", idempotency_key="k")

    rest = _AlwaysStartingRest(existing={"a", "bwd"})
    session = CatalogSession(_catalog(rest))

    session.create_folder("bwd.new")

    with pytest.raises(CatalogCommitError) as exc_info:
        session.commit()  # retry defaults to False

    assert isinstance(exc_info.value.original_error, EngineStartingError)
    assert exc_info.value.rolled_back is True
