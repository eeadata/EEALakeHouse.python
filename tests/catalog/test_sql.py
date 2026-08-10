from __future__ import annotations

import httpx
import pytest
import respx

from eea_datalakehouse.catalog.errors import CatalogOperationError, EngineStartingError
from eea_datalakehouse.catalog.sql import (
    FLIGHT_LOCATION_ENV_VAR,
    TRANSPORT_ENV_VAR,
    FlightSqlExecutor,
    RestSqlExecutor,
    resolve_executor,
)

BASE_URL = "https://dremio.example.test"


@respx.mock
def test_execute_polls_until_completed_and_returns_row_count() -> None:
    respx.post(f"{BASE_URL}/api/v3/sql").mock(
        return_value=httpx.Response(200, json={"id": "job-1"})
    )
    respx.get(f"{BASE_URL}/api/v3/job/job-1").mock(
        side_effect=[
            httpx.Response(200, json={"jobState": "RUNNING"}),
            httpx.Response(200, json={"jobState": "COMPLETED", "rowCount": 5}),
        ]
    )

    executor = RestSqlExecutor(BASE_URL, "pat", poll_interval=0.01)
    result = executor.execute("SELECT 1")

    assert result.job_id == "job-1"
    assert result.row_count == 5


@respx.mock
def test_execute_raises_catalog_error_on_submit_rejection() -> None:
    respx.post(f"{BASE_URL}/api/v3/sql").mock(
        return_value=httpx.Response(400, text="bad SQL")
    )
    executor = RestSqlExecutor(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError):
        executor.execute("NOT SQL")


@respx.mock
def test_execute_raises_catalog_error_on_failed_job() -> None:
    respx.post(f"{BASE_URL}/api/v3/sql").mock(
        return_value=httpx.Response(200, json={"id": "job-1"})
    )
    respx.get(f"{BASE_URL}/api/v3/job/job-1").mock(
        return_value=httpx.Response(200, json={"jobState": "FAILED", "errorMessage": "nope"})
    )
    executor = RestSqlExecutor(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="nope"):
        executor.execute("SELECT 1")


@respx.mock
def test_execute_raises_engine_starting_on_submit_timeout() -> None:
    respx.post(f"{BASE_URL}/api/v3/sql").mock(side_effect=httpx.TimeoutException("timed out"))
    executor = RestSqlExecutor(BASE_URL, "pat")

    with pytest.raises(EngineStartingError):
        executor.execute("SELECT 1")


@respx.mock
def test_execute_raises_engine_starting_when_poll_deadline_exceeded() -> None:
    respx.post(f"{BASE_URL}/api/v3/sql").mock(
        return_value=httpx.Response(200, json={"id": "job-1"})
    )
    # Always non-terminal — the executor's own timeout budget must be what
    # eventually gives up, not any single HTTP call timing out.
    respx.get(f"{BASE_URL}/api/v3/job/job-1").mock(
        return_value=httpx.Response(200, json={"jobState": "RUNNING"})
    )
    executor = RestSqlExecutor(BASE_URL, "pat", timeout=0.05, poll_interval=0.01)

    with pytest.raises(EngineStartingError):
        executor.execute("SELECT 1")


def test_repr_does_not_expose_the_bearer_token() -> None:
    executor = RestSqlExecutor(BASE_URL, "super-secret-pat")

    rendered = repr(executor)

    assert "super-secret-pat" not in rendered
    assert "Bearer" not in rendered


def test_resolve_executor_defaults_to_rest_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(TRANSPORT_ENV_VAR, raising=False)

    executor = resolve_executor(BASE_URL, "pat")

    assert isinstance(executor, RestSqlExecutor)


@pytest.mark.parametrize("value", ["rest", "REST", "", "something-else"])
def test_resolve_executor_treats_anything_but_flight_as_rest(
    value: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(TRANSPORT_ENV_VAR, value)

    executor = resolve_executor(BASE_URL, "pat")

    assert isinstance(executor, RestSqlExecutor)


def test_resolve_executor_picks_flight_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(TRANSPORT_ENV_VAR, "flight")
    monkeypatch.delenv(FLIGHT_LOCATION_ENV_VAR, raising=False)

    executor = resolve_executor(BASE_URL, "pat")

    assert isinstance(executor, FlightSqlExecutor)
    assert executor._location == "grpc://dremio.example.test:32010"


def test_resolve_executor_flight_location_env_var_overrides_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TRANSPORT_ENV_VAR, "flight")
    monkeypatch.setenv(FLIGHT_LOCATION_ENV_VAR, "grpc://custom-host:1234")

    executor = resolve_executor(BASE_URL, "pat")

    assert executor._location == "grpc://custom-host:1234"


def test_resolve_executor_explicit_flight_location_wins_over_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(TRANSPORT_ENV_VAR, "flight")
    monkeypatch.setenv(FLIGHT_LOCATION_ENV_VAR, "grpc://from-env:1234")

    executor = resolve_executor(BASE_URL, "pat", flight_location="grpc://explicit:5678")

    assert executor._location == "grpc://explicit:5678"
