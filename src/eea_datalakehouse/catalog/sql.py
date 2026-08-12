"""Run SQL against Dremio, via either its REST Jobs API or Arrow Flight SQL.

Both executors implement the same tiny :class:`SqlExecutor` protocol, so a
caller in operations.py picks whichever transport it wants without the
operation itself caring. Both treat a stalled/timed-out call as *probably* a
Dremio engine cold-starting rather than a real failure — the same problem
dds_ingestion's folder.py documents on the ingest side (a cold engine can
take minutes to answer) — and raise :class:`EngineStartingError` instead of
blocking indefinitely, so the caller can remember the attempt
(catalog.retry_state) and retry later.

Neither executor knows about idempotency keys or retry bookkeeping — that
lives in operations.py, which is what actually decides whether/how to retry.

REST is the default transport. Arrow Flight is opt-in via the
``EEA_CATALOG_TRANSPORT=flight`` environment variable (see .env) — call
:func:`resolve_executor` rather than constructing an executor directly if you
want that choice honoured without an ``if`` in the caller.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx

from .errors import CatalogOperationError, EngineStartingError

DEFAULT_TIMEOUT = 900.0  # seconds — matches dds_ingestion's DDS_TIMEOUT reasoning
DEFAULT_POLL_INTERVAL = 2.0
DEFAULT_RESULTS_PAGE_SIZE = 500

# Env var read by resolve_executor(). Any value other than "flight"
# (including unset) means REST — REST is the default transport.
TRANSPORT_ENV_VAR = "EEA_CATALOG_TRANSPORT"
# Optional override for the Flight SQL location, since it's a different
# host/port/scheme (grpc://host:32010) than the REST base_url. Only
# consulted when the transport is actually "flight".
FLIGHT_LOCATION_ENV_VAR = "DREMIO_FLIGHT_LOCATION"
# Dremio's documented default Arrow Flight port — used only as a fallback
# when neither an explicit flight_location nor DREMIO_FLIGHT_LOCATION is
# given; unverified against this project's actual deployment.
DEFAULT_FLIGHT_PORT = 32010

# Dremio job states that mean "done, one way or another" (api/v3/job/{id}).
_TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELED"}


@dataclass(frozen=True, slots=True)
class SqlResult:
    """Outcome of a successful SQL statement (DDL/DML — no result rows)."""

    row_count: int | None
    job_id: str | None = None


class SqlExecutor(Protocol):
    def execute(self, sql: str, *, idempotency_key: str | None = None) -> SqlResult: ...

    def fetch_all(
        self, sql: str, *, idempotency_key: str | None = None
    ) -> list[dict[str, Any]]: ...


def _default_flight_location(base_url: str) -> str:
    """Best-effort Flight location derived from a REST base_url's host and
    scheme.

    Dremio's REST API and Flight endpoint are different ports on the same
    host, not the same URL — this borrows the host from base_url, and its
    TLS-ness too (an https base_url gets grpc+tls, not plain grpc — a
    Dremio deployment terminating REST in TLS almost always requires Flight
    over TLS as well; connecting with plain grpc to a TLS-only endpoint
    fails with a misleading "Socket closed" rather than a clear protocol
    error). The port is DEFAULT_FLIGHT_PORT, which is Dremio's documented
    default and has not been verified against this project's actual
    deployment. Prefer passing `flight_location` explicitly, or set
    DREMIO_FLIGHT_LOCATION, if either guess is wrong for you.
    """
    parsed = urlsplit(base_url)
    host = parsed.hostname or base_url
    scheme = "grpc+tls" if parsed.scheme == "https" else "grpc"
    return f"{scheme}://{host}:{DEFAULT_FLIGHT_PORT}"


def resolve_flight_location(base_url: str, flight_location: str | None = None) -> str:
    """The Flight location to actually connect to: explicit `flight_location`,
    else DREMIO_FLIGHT_LOCATION, else a best-effort default derived from
    `base_url`'s host (see `_default_flight_location`)."""
    return (
        flight_location or os.environ.get(FLIGHT_LOCATION_ENV_VAR) or _default_flight_location(base_url)
    )


def resolve_executor(
    base_url: str,
    token: str,
    *,
    username: str | None = None,
    flight_location: str | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> SqlExecutor:
    """REST by default; Arrow Flight when EEA_CATALOG_TRANSPORT=flight.

    `flight_location` (or DREMIO_FLIGHT_LOCATION if that's not given) and
    `username` are only consulted when Flight is actually selected — REST
    callers never need to know or care about either (REST authenticates
    with `token` alone).
    """
    transport = os.environ.get(TRANSPORT_ENV_VAR, "").strip().lower()
    if transport != "flight":
        return RestSqlExecutor(base_url, token, timeout=timeout)

    return FlightSqlExecutor(
        resolve_flight_location(base_url, flight_location), username, token, timeout=timeout
    )


class RestSqlExecutor:
    """Runs SQL through Dremio's REST Jobs API (``POST /api/v3/sql``).

    Submits, then polls ``GET /api/v3/job/{id}`` until a terminal state.
    `timeout` bounds the whole submit-and-poll cycle, not any single HTTP
    call — a cold engine shows up as many fast polls returning a
    non-terminal state, not one slow call, so the budget has to span the
    loop.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"}
        self._timeout = timeout
        self._poll_interval = poll_interval
        self._owns_client = http_client is None
        # Deliberately a short per-call timeout: the *poll loop* is what
        # waits out a cold engine, not any single request.
        self._http = http_client or httpx.Client(timeout=30.0)

    def __repr__(self) -> str:
        # No Authorization header here, same reasoning as IngestClient.
        return f"RestSqlExecutor(base_url={self._base_url!r})"

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def _submit_and_wait(
        self, sql: str, idempotency_key: str | None
    ) -> tuple[str, dict[str, Any]]:
        """Submit `sql` and poll until terminal. Returns (job_id, final payload)."""
        try:
            resp = self._http.post(
                f"{self._base_url}/api/v3/sql", json={"sql": sql}, headers=self._headers
            )
        except httpx.TimeoutException as exc:
            raise EngineStartingError(
                f"submitting SQL timed out (likely a starting engine): {sql!r}",
                idempotency_key=idempotency_key,
            ) from exc
        if resp.status_code >= 400:
            raise CatalogOperationError(
                f"SQL submission rejected ({resp.status_code}): {resp.text[:300]}"
            )
        job_id = resp.json()["id"]

        deadline = time.monotonic() + self._timeout
        payload: dict[str, Any] = {}
        state = ""
        while True:
            try:
                job_resp = self._http.get(
                    f"{self._base_url}/api/v3/job/{job_id}", headers=self._headers
                )
            except httpx.TimeoutException as exc:
                raise EngineStartingError(
                    f"polling job {job_id} timed out", idempotency_key=idempotency_key
                ) from exc
            if job_resp.status_code >= 400:
                raise CatalogOperationError(
                    f"could not poll job {job_id} ({job_resp.status_code}): {job_resp.text[:300]}"
                )
            payload = job_resp.json()
            state = str(payload.get("jobState", "")).upper()
            if state in _TERMINAL_STATES:
                break
            if time.monotonic() >= deadline:
                raise EngineStartingError(
                    f"job {job_id} still {state!r} after {self._timeout:.0f}s — "
                    "likely an engine starting; safe to retry later",
                    idempotency_key=idempotency_key,
                )
            time.sleep(min(self._poll_interval, max(0.0, deadline - time.monotonic())))

        if state == "FAILED":
            raise CatalogOperationError(
                f"job {job_id} failed: {payload.get('errorMessage', payload)}"
            )
        if state == "CANCELED":
            raise CatalogOperationError(f"job {job_id} was canceled")
        return job_id, payload

    def execute(self, sql: str, *, idempotency_key: str | None = None) -> SqlResult:
        job_id, payload = self._submit_and_wait(sql, idempotency_key)
        return SqlResult(job_id=job_id, row_count=payload.get("rowCount"))

    def fetch_all(
        self, sql: str, *, idempotency_key: str | None = None
    ) -> list[dict[str, Any]]:
        """Run a SELECT and return every row as a dict keyed by column name."""
        job_id, payload = self._submit_and_wait(sql, idempotency_key)
        total = payload.get("rowCount") or 0

        rows: list[dict[str, Any]] = []
        offset = 0
        while offset < total:
            try:
                resp = self._http.get(
                    f"{self._base_url}/api/v3/job/{job_id}/results",
                    params={"offset": offset, "limit": DEFAULT_RESULTS_PAGE_SIZE},
                    headers=self._headers,
                )
            except httpx.TimeoutException as exc:
                raise EngineStartingError(
                    f"fetching results for job {job_id} timed out",
                    idempotency_key=idempotency_key,
                ) from exc
            if resp.status_code >= 400:
                raise CatalogOperationError(
                    f"could not fetch results for job {job_id} "
                    f"({resp.status_code}): {resp.text[:300]}"
                )
            page = resp.json().get("rows", [])
            if not page:
                break
            rows.extend(page)
            offset += len(page)
        return rows


class FlightSqlExecutor:
    """Runs SQL through Dremio's Arrow Flight SQL endpoint.

    Authenticates with `username`/`token` (the Dremio PAT used as the
    password) via Flight's own HTTP-basic handshake
    (`FlightClient.authenticate_basic_token`), the same username/password
    auth `DREMIO_USER`/`DREMIO_TOKEN` already provide elsewhere in this
    project — rather than sending the raw PAT as a bearer header directly
    (some Dremio deployments reject that on the Flight endpoint even though
    REST accepts it). The handshake runs once and its resulting session
    token is cached for the life of this executor, not repeated per call.

    Holds one `FlightClient` (a persistent gRPC channel) for the executor's
    whole lifetime instead of opening a new one per statement — data
    operations like `datacopy`/`datamove` issue several statements through
    the same executor (existence check, folder creation, the CTAS/DROP
    itself), and reconnecting for each would be pure overhead. Call
    `close()` when done, same as `RestSqlExecutor`.
    """

    def __init__(
        self,
        location: str,
        username: str | None,
        token: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        flight_client: Any | None = None,
    ) -> None:
        self._location = location
        self._username = username
        self._token = token
        self._timeout = timeout
        self._owns_client = flight_client is None
        self._client = flight_client
        self._auth_header: tuple[bytes, bytes] | None = None

    def __repr__(self) -> str:
        return f"FlightSqlExecutor(location={self._location!r}, username={self._username!r})"

    def close(self) -> None:
        if self._owns_client and self._client is not None:
            self._client.close()
            self._client = None
        self._auth_header = None

    def _get_client(self):
        import pyarrow.flight as flight

        if self._client is None:
            self._client = flight.FlightClient(self._location)
        return self._client

    def _authenticate(self, idempotency_key: str | None):
        """The basic-auth handshake's bearer-token header, done once and
        cached — reused by every call for this executor's whole lifetime,
        same reasoning as `_get_client()` reusing one `FlightClient`."""
        import pyarrow.flight as flight

        if self._auth_header is None:
            if not self._username:
                raise CatalogOperationError(
                    "FlightSqlExecutor needs a username to authenticate — "
                    "pass Catalog(..., username=...) (e.g. DREMIO_USER)"
                )
            client = self._get_client()
            try:
                self._auth_header = client.authenticate_basic_token(
                    self._username, self._token, flight.FlightCallOptions(timeout=self._timeout)
                )
            except flight.FlightTimedOutError as exc:
                raise EngineStartingError(
                    "Flight authentication timed out (likely a starting engine)",
                    idempotency_key=idempotency_key,
                ) from exc
            except flight.FlightServerError as exc:
                raise CatalogOperationError(f"Flight authentication failed: {exc}") from exc
        return self._auth_header

    def _call_options(self, idempotency_key: str | None = None):
        import pyarrow.flight as flight

        return flight.FlightCallOptions(
            timeout=self._timeout, headers=[self._authenticate(idempotency_key)]
        )

    def _get_flight_info(self, sql: str, idempotency_key: str | None):
        import pyarrow.flight as flight

        client = self._get_client()
        options = self._call_options(idempotency_key)
        descriptor = flight.FlightDescriptor.for_command(sql.encode())
        try:
            info = client.get_flight_info(descriptor, options)
        except flight.FlightTimedOutError as exc:
            raise EngineStartingError(
                f"Flight SQL call timed out (likely a starting engine): {sql!r}",
                idempotency_key=idempotency_key,
            ) from exc
        except flight.FlightServerError as exc:
            raise CatalogOperationError(f"Flight SQL error: {exc}") from exc
        return client, options, info

    def execute(self, sql: str, *, idempotency_key: str | None = None) -> SqlResult:
        import pyarrow.flight as flight

        client, options, info = self._get_flight_info(sql, idempotency_key)
        row_count = 0
        for endpoint in info.endpoints:
            try:
                reader = client.do_get(endpoint.ticket, options)
                for chunk in reader:
                    row_count += chunk.data.num_rows
            except flight.FlightTimedOutError as exc:
                raise EngineStartingError(
                    f"Flight SQL fetch timed out (likely a starting engine): {sql!r}",
                    idempotency_key=idempotency_key,
                ) from exc
        return SqlResult(job_id=None, row_count=row_count or None)

    def fetch_all(
        self, sql: str, *, idempotency_key: str | None = None
    ) -> list[dict[str, Any]]:
        """Run a SELECT and return every row as a dict keyed by column name."""
        import pyarrow.flight as flight

        client, options, info = self._get_flight_info(sql, idempotency_key)
        rows: list[dict[str, Any]] = []
        for endpoint in info.endpoints:
            try:
                reader = client.do_get(endpoint.ticket, options)
                for chunk in reader:
                    rows.extend(chunk.data.to_pylist())
            except flight.FlightTimedOutError as exc:
                raise EngineStartingError(
                    f"Flight SQL fetch timed out (likely a starting engine): {sql!r}",
                    idempotency_key=idempotency_key,
                ) from exc
        return rows
