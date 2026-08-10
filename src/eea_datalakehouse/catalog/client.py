"""Catalog — the same shape as dds_ingestion's IngestClient/FolderIngest:
one object wrapping the resolved connection, with the operations as methods
instead of free functions that each take an executor.

    catalog = Catalog(base_url, token)          # REST by default
    catalog.draft2version("bwd.draft.bw", "bwd.versions.v1", idempotency_key="bwd-v2025_1")

Which transport `catalog` actually uses is resolved once, at construction,
by :func:`resolve_executor` (REST unless EEA_CATALOG_TRANSPORT=flight) — the
same env-var-driven choice as the module-level functions in operations.py,
which is what every method here delegates to.
"""

from __future__ import annotations

from typing import Any, Literal

from . import operations
from .operations import TableInfo
from .rest import CatalogRestClient
from .sql import DEFAULT_TIMEOUT, SqlExecutor, SqlResult, resolve_executor


class Catalog:
    """Dremio catalog operations, bound to one resolved connection.

    Pass `executor=` to inject one directly (e.g. a test's fake, or a
    specific `RestSqlExecutor`/`FlightSqlExecutor`) and skip
    `resolve_executor`'s env-var-driven choice entirely.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        flight_location: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        executor: SqlExecutor | None = None,
        catalog_rest: CatalogRestClient | None = None,
    ) -> None:
        self._executor = executor or resolve_executor(
            base_url, token, flight_location=flight_location, timeout=timeout
        )
        # Folder existence/creation has no SQL or Flight equivalent, so it
        # always goes through Dremio's own REST catalog API directly,
        # independent of whichever transport self._executor ended up using.
        self._catalog_rest = catalog_rest or CatalogRestClient(base_url, token)

    def __repr__(self) -> str:
        # No token here, same reasoning as IngestClient's own __repr__.
        return f"Catalog(executor={self._executor!r})"

    def table2view(
        self,
        view_path: str,
        source_path: str,
        *,
        create_target_folder: bool = True,
        idempotency_key: str,
    ) -> SqlResult:
        return operations.table2view(
            self._executor,
            view_path,
            source_path,
            create_target_folder=create_target_folder,
            catalog_rest=self._catalog_rest,
            idempotency_key=idempotency_key,
        )

    def draft2version(
        self, draft_path: str, version_path: str, *, idempotency_key: str
    ) -> SqlResult:
        return operations.draft2version(
            self._executor, draft_path, version_path, idempotency_key=idempotency_key
        )

    def publishversion(
        self, consumer_view_path: str, version_path: str, *, idempotency_key: str
    ) -> SqlResult:
        return operations.publishversion(
            self._executor, consumer_view_path, version_path, idempotency_key=idempotency_key
        )

    def datacopy(
        self,
        source_path: str,
        target_path: str,
        *,
        mode: Literal["create", "replace"] = "create",
        idempotency_key: str,
    ) -> SqlResult:
        return operations.datacopy(
            self._executor, source_path, target_path, mode=mode, idempotency_key=idempotency_key
        )

    def datamove(
        self,
        source_path: str,
        target_path: str,
        *,
        entry_type: Literal["TABLE", "VIEW"],
        idempotency_key: str,
    ) -> SqlResult:
        return operations.datamove(
            self._executor,
            source_path,
            target_path,
            entry_type=entry_type,
            idempotency_key=idempotency_key,
        )

    def deleteview(self, view_path: str, *, idempotency_key: str) -> SqlResult:
        return operations.deleteview(self._executor, view_path, idempotency_key=idempotency_key)

    def gettablesfrom(self, schema_path: str, *, idempotency_key: str) -> list[str]:
        return operations.gettablesfrom(
            self._executor, schema_path, idempotency_key=idempotency_key
        )

    def gettableitemsfrom(self, table_path: str, *, idempotency_key: str) -> TableInfo:
        return operations.gettableitemsfrom(
            self._executor, table_path, idempotency_key=idempotency_key
        )

    def retry_pending(self, idempotency_key: str) -> SqlResult | list[Any]:
        return operations.retry_pending(
            self._executor, idempotency_key, catalog_rest=self._catalog_rest
        )
