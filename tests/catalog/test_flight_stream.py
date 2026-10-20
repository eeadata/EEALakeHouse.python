"""`FlightSqlExecutor.stream_batches` / `fork` and `Catalog.stream_rows`,
against a fake Flight client (no Dremio)."""

from __future__ import annotations

import threading
import time
from typing import Any

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from eea_datalakehouse.catalog.errors import CatalogOperationError, EngineStartingError
from eea_datalakehouse.catalog.sql import FlightSqlExecutor


class _Chunk:
    def __init__(self, data: pa.RecordBatch | None) -> None:
        self.data = data


class _Reader:
    def __init__(self, batches: list[pa.RecordBatch], fail: Exception | None, delay: float) -> None:
        self._batches = batches
        self._fail = fail
        self._delay = delay
        self.cancelled = False

    def __iter__(self) -> Any:
        yield _Chunk(None)  # an empty chunk is skipped
        for batch in self._batches:
            time.sleep(self._delay)
            yield _Chunk(batch)
        if self._fail is not None:
            raise self._fail

    def cancel(self) -> None:
        self.cancelled = True


class _Endpoint:
    def __init__(self, ticket: str) -> None:
        self.ticket = ticket


class _FakeFlight:
    """One query's result split over ``endpoints`` (ticket -> batches)."""

    def __init__(
        self,
        endpoints: dict[str, list[pa.RecordBatch]],
        *,
        fail: dict[str, Exception] | None = None,
        info_error: Exception | None = None,
        delay: float = 0.0,
    ) -> None:
        self.endpoints = endpoints
        self.fail = fail or {}
        self.info_error = info_error
        self.delay = delay
        self.readers: list[_Reader] = []
        self.get_options: list[Any] = []
        self.authenticate_calls = 0
        self.threads: set[str] = set()
        self._lock = threading.Lock()

    def authenticate_basic_token(self, username: str, password: str, options: Any = None) -> Any:
        with self._lock:
            self.authenticate_calls += 1
        time.sleep(0.01)  # widen the window a racing second login would need
        return (b"authorization", b"Bearer t")

    def get_flight_info(self, descriptor: Any, options: Any) -> Any:
        if self.info_error is not None:
            raise self.info_error

        class Info:
            endpoints = [_Endpoint(t) for t in self.endpoints]

        return Info()

    def do_get(self, ticket: str, options: Any) -> _Reader:
        with self._lock:
            self.threads.add(threading.current_thread().name)
            self.get_options.append(options)
            reader = _Reader(self.endpoints[ticket], self.fail.get(ticket), self.delay)
            self.readers.append(reader)
        return reader

    def close(self) -> None:
        pass


def _batch(*values: int) -> pa.RecordBatch:
    return pa.record_batch({"v": list(values)})


def _executor(client: _FakeFlight) -> FlightSqlExecutor:
    return FlightSqlExecutor("grpc://x:32010", "alice", "pat", flight_client=client)


def _values(batches: Any) -> list[int]:
    return [v for batch in batches for v in batch.column("v").to_pylist()]


def test_one_endpoint_streams_its_batches_in_order() -> None:
    client = _FakeFlight({"t": [_batch(1, 2), _batch(3)]})

    assert _values(_executor(client).stream_batches("SELECT 1")) == [1, 2, 3]
    assert client.readers[0].cancelled is False  # read to the end


def test_reading_has_no_overall_deadline_but_keeps_the_login() -> None:
    client = _FakeFlight({"t": [_batch(1)]})

    list(_executor(client).stream_batches("SELECT 1"))

    (options,) = client.get_options
    assert options.timeout is None or options.timeout < 0  # pyarrow shows "none" as -1
    assert options.headers == [(b"authorization", b"Bearer t")]


def test_several_endpoints_are_read_at_once() -> None:
    client = _FakeFlight(
        {"a": [_batch(1), _batch(2)], "b": [_batch(10), _batch(20)], "c": [_batch(100)]},
        delay=0.02,
    )

    values = _values(_executor(client).stream_batches("SELECT 1"))

    assert sorted(values) == [1, 2, 10, 20, 100]
    assert len(client.threads) == 3


def test_stopping_early_cancels_the_open_streams() -> None:
    client = _FakeFlight({"t": [_batch(i) for i in range(10)]})
    stream = _executor(client).stream_batches("SELECT 1")

    next(stream)
    stream.close()

    assert client.readers[0].cancelled is True


def test_stopping_early_with_several_endpoints_stops_their_threads() -> None:
    client = _FakeFlight({t: [_batch(i) for i in range(50)] for t in "abc"}, delay=0.005)
    stream = _executor(client).stream_batches("SELECT 1", max_queued=1)

    next(stream)
    stream.close()

    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and not all(r.cancelled for r in client.readers):
        time.sleep(0.01)
    assert all(r.cancelled for r in client.readers)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (flight.FlightTimedOutError("slow"), EngineStartingError),
        (flight.FlightServerError("bad"), CatalogOperationError),
    ],
)
def test_a_failure_while_reading_is_mapped(error: Exception, expected: type[Exception]) -> None:
    for endpoints in ({"t": [_batch(1)]}, {"a": [_batch(1)], "b": [_batch(2)]}):
        client = _FakeFlight(endpoints, fail={"t": error, "b": error})
        with pytest.raises(expected):
            list(_executor(client).stream_batches("SELECT 1"))


def test_a_timeout_planning_the_query_is_engine_starting() -> None:
    client = _FakeFlight({}, info_error=flight.FlightTimedOutError("cold"))

    with pytest.raises(EngineStartingError):
        list(_executor(client).stream_batches("SELECT 1"))


def test_fork_opens_a_new_channel_and_reuses_the_login(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[str] = []

    class _Client(_FakeFlight):
        def __init__(self, location: str) -> None:
            super().__init__({})
            created.append(location)

    monkeypatch.setattr(flight, "FlightClient", _Client)
    original = _executor(_FakeFlight({}))
    original._call_options()  # log in once

    twin = original.fork()
    options = twin._call_options()
    assert created == []  # the channel opens on first use
    twin._get_client()

    assert created == ["grpc://x:32010"]
    assert twin._client is not original._client
    assert options.headers == [(b"authorization", b"Bearer t")]
    assert twin._client.authenticate_calls == 0  # no second handshake
    twin.close()


def test_logging_in_from_several_threads_happens_once() -> None:
    client = _FakeFlight({})
    executor = _executor(client)

    threads = [threading.Thread(target=executor._call_options) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert client.authenticate_calls == 1


def test_catalog_stream_rows_and_open_flight_channel() -> None:
    from eea_datalakehouse.catalog.client import Catalog

    client = _FakeFlight({"t": [_batch(7)]})
    catalog = Catalog.__new__(Catalog)
    catalog._flight_executor = _executor(client)

    assert _values(catalog.stream_rows("SELECT 1", idempotency_key="k")) == [7]
    channel = catalog.open_flight_channel()
    assert isinstance(channel, FlightSqlExecutor) and channel is not catalog._flight_executor
