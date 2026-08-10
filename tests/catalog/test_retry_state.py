from __future__ import annotations

from pathlib import Path

from eea_datalakehouse.catalog import retry_state


def test_record_then_get_round_trips(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"

    pending = retry_state.record(
        "key-1", "draft2version", "versions.v1", "timed out", params={"a": "b"}, state_path=state_path
    )

    assert pending.attempts == 1
    fetched = retry_state.get("key-1", state_path=state_path)
    assert fetched is not None
    assert fetched.operation == "draft2version"
    assert fetched.params == {"a": "b"}


def test_record_twice_increments_attempts_and_keeps_first_timestamp(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"

    first = retry_state.record("key-1", "datacopy", "t", "err1", state_path=state_path)
    second = retry_state.record("key-1", "datacopy", "t", "err2", state_path=state_path)

    assert second.attempts == 2
    assert second.first_attempted_at == first.first_attempted_at
    assert second.last_error == "err2"


def test_get_missing_key_returns_none(tmp_path: Path) -> None:
    assert retry_state.get("nope", state_path=tmp_path / "state.json") is None


def test_clear_removes_entry(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    retry_state.record("key-1", "datamove", "t", "err", state_path=state_path)

    retry_state.clear("key-1", state_path=state_path)

    assert retry_state.get("key-1", state_path=state_path) is None


def test_clear_missing_key_is_a_no_op(tmp_path: Path) -> None:
    retry_state.clear("nope", state_path=tmp_path / "state.json")  # must not raise


def test_list_pending_returns_every_recorded_key(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    retry_state.record("key-1", "datamove", "t1", "err", state_path=state_path)
    retry_state.record("key-2", "datacopy", "t2", "err", state_path=state_path)

    pending = {p.idempotency_key for p in retry_state.list_pending(state_path=state_path)}

    assert pending == {"key-1", "key-2"}


def test_corrupt_state_file_is_treated_as_empty(tmp_path: Path) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text("not json{{{")

    assert retry_state.get("key-1", state_path=state_path) is None
    retry_state.record("key-1", "datamove", "t", "err", state_path=state_path)  # must not raise
    assert retry_state.get("key-1", state_path=state_path) is not None
