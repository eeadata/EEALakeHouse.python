"""Dremio catalog operations, via a `Catalog` object (same shape as
dds_ingestion's IngestClient/FolderIngest):

    catalog = Catalog(base_url, token)  # REST by default
    catalog.draft2version("bwd.draft.bw", "bwd.versions.v1", idempotency_key="bwd-v2025_1")

table2view, draft2version, publishversion, datacopy, datamove, deleteview,
gettablesfrom, gettableitemsfrom — over REST (the default) or Arrow Flight
(opt in via EEA_CATALOG_TRANSPORT=flight in .env), with retry-later error
handling for a Dremio engine that's still starting up.

The module-level functions in `operations` (table2view(executor, ...) etc.)
are what `Catalog`'s methods delegate to — call them directly if you'd
rather manage the executor yourself.
"""

from __future__ import annotations

from .client import Catalog
from .errors import CatalogOperationError, EngineStartingError
from .operations import (
    TableInfo,
    datacopy,
    datamove,
    deleteview,
    draft2version,
    gettableitemsfrom,
    gettablesfrom,
    publishversion,
    retry_pending,
    table2view,
)
from .rest import CatalogRestClient
from .sql import (
    FLIGHT_LOCATION_ENV_VAR,
    TRANSPORT_ENV_VAR,
    FlightSqlExecutor,
    RestSqlExecutor,
    SqlExecutor,
    SqlResult,
    resolve_executor,
)

__all__ = [
    "FLIGHT_LOCATION_ENV_VAR",
    "TRANSPORT_ENV_VAR",
    "Catalog",
    "CatalogOperationError",
    "CatalogRestClient",
    "EngineStartingError",
    "FlightSqlExecutor",
    "RestSqlExecutor",
    "SqlExecutor",
    "SqlResult",
    "TableInfo",
    "datacopy",
    "datamove",
    "deleteview",
    "draft2version",
    "gettableitemsfrom",
    "gettablesfrom",
    "publishversion",
    "resolve_executor",
    "retry_pending",
    "table2view",
]
