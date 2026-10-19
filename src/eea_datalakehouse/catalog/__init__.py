"""Dremio catalog operations, via a `Catalog` object (same shape as
dds_ingestion's IngestClient/FolderIngest):

    catalog = Catalog(base_url, token)  # REST by default
    catalog.draft2version("bwd.draft.bw", "bwd.versions.v1", idempotency_key="bwd-v2025_1")

table2view, draft2version, publishversion, datacopy, datamove, deleteview,
deletetable, createviewas, gettablesfrom, getentriesfrom, gettableitemsfrom,
entryexists, getviewsql, getfoldersfrom, getwikifrom, gettagsfrom, setwikito,
deletewiki, setmeta2wiki, getmetafromwiki, settagsto, deletetags,
createfolder, deletefolder — over REST (the default) or Arrow Flight (opt
in via EEA_CATALOG_TRANSPORT=flight in .env), with retry-later error
handling for a Dremio engine that's still starting up.

A whole folder at a time — sub-folders, tables and views together — lives in
`foldertree`: `folderscan` plans it, `foldercopy` reproduces it and proves
the copy by row count, `foldermove` does that and then removes the source,
but only once the copy verified. Views are rewritten to point at the copies
rather than back at the originals (`remap_view_sql`).

The module-level functions in `operations` (table2view(executor, ...) etc.)
are what `Catalog`'s methods delegate to — call them directly if you'd
rather manage the executor yourself.
"""

from __future__ import annotations

from .client import Catalog
from .errors import CatalogAuthError, CatalogOperationError, EngineStartingError
from .foldertree import (
    EntryResult,
    FolderTransferReport,
    FolderTree,
    RemapResult,
    TreeEntry,
    foldercopy,
    foldermove,
    folderscan,
    remap_view_sql,
)
from .operations import (
    CatalogEntry,
    TableInfo,
    VersionResult,
    ViewDefinition,
    createfolder,
    createversion,
    createviewas,
    datacopy,
    datamove,
    deletefolder,
    deletetable,
    deletetags,
    deleteview,
    deletewiki,
    draft2version,
    entryexists,
    getentriesfrom,
    getfoldersfrom,
    getmetafromwiki,
    gettableitemsfrom,
    gettablesfrom,
    gettagsfrom,
    getviewsql,
    getwikifrom,
    publishversion,
    retry_pending,
    setmeta2wiki,
    settagsto,
    setwikito,
    table2view,
)
from .rest import CatalogRestClient
from .session import (
    CatalogCommitError,
    CatalogListing,
    CatalogSession,
    CatalogSessionError,
    CommitReport,
)
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
    "CatalogAuthError",
    "CatalogCommitError",
    "CatalogEntry",
    "CatalogListing",
    "CatalogOperationError",
    "CatalogRestClient",
    "CatalogSession",
    "CatalogSessionError",
    "CommitReport",
    "EngineStartingError",
    "EntryResult",
    "FlightSqlExecutor",
    "FolderTransferReport",
    "FolderTree",
    "RemapResult",
    "RestSqlExecutor",
    "SqlExecutor",
    "SqlResult",
    "TableInfo",
    "TreeEntry",
    "VersionResult",
    "ViewDefinition",
    "createfolder",
    "createversion",
    "createviewas",
    "datacopy",
    "datamove",
    "deletefolder",
    "deletetable",
    "deletetags",
    "deleteview",
    "deletewiki",
    "draft2version",
    "entryexists",
    "foldercopy",
    "foldermove",
    "folderscan",
    "getentriesfrom",
    "getfoldersfrom",
    "gettableitemsfrom",
    "gettablesfrom",
    "gettagsfrom",
    "getviewsql",
    "getwikifrom",
    "publishversion",
    "getmetafromwiki",
    "remap_view_sql",
    "resolve_executor",
    "retry_pending",
    "setmeta2wiki",
    "settagsto",
    "setwikito",
    "table2view",
]
