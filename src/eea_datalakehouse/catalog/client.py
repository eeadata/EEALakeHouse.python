"""Catalog — the same shape as dds_ingestion's IngestClient/FolderIngest:
one object wrapping the resolved connection, with the operations as methods
instead of free functions that each take an executor.

    catalog = Catalog(base_url, token)
    catalog.draft2version("bwd.draft.bw", "bwd.versions.v1", idempotency_key="bwd-v2025_1")

`catalog` actually holds two SQL executors, chosen per operation rather than
once for the whole instance: `datacopy`/`datamove` — the operations that
actually move data, not just metadata — always go over Arrow Flight SQL;
every other executor-based operation (table2view, draft2version,
publishversion, deleteview, gettablesfrom, gettableitemsfrom) always goes
over Dremio's REST Jobs API. This is fixed, not env-var-driven — pass
`executor=`/`flight_executor=` to override either one directly (e.g. a
test's fake). `resolve_executor`/EEA_CATALOG_TRANSPORT (see sql.py) still
exist for a caller using the module-level `operations.*` functions with
their own executor, but `Catalog` itself no longer consults that env var.

The Flight side holds one persistent gRPC session for its whole lifetime
(see FlightSqlExecutor's own docstring) — it needs to be disposed, not just
dropped, or that channel leaks. `Catalog.close()` does that; `with
Catalog(...) as catalog:` calls it automatically when the caller is done.
For the case a caller doesn't (a debug script that just exits, a container
receiving SIGTERM), every `Catalog` also registers itself for best-effort
cleanup at interpreter exit and on SIGTERM/SIGINT — see
`_close_all_open_catalogs` below.
"""

from __future__ import annotations

import atexit
import signal
import threading
import weakref
from collections.abc import Iterator
from typing import Any, Literal

import pyarrow as pa

from . import foldertree, operations, retry_state
from .foldertree import FolderTransferReport, FolderTree, ProgressCallback
from .operations import CatalogEntry, TableInfo, VersionResult, ViewDefinition
from .rest import CatalogRestClient
from .sql import (
    DEFAULT_TIMEOUT,
    FlightSqlExecutor,
    RestSqlExecutor,
    SqlExecutor,
    SqlResult,
    resolve_flight_location,
)

# Which operations' retries must go back over Flight rather than REST — kept
# in sync with which methods below pass self._flight_executor.
_FLIGHT_OPERATIONS = frozenset({"datacopy", "datamove", "createversion"})

_open_catalogs: weakref.WeakSet[Catalog] = weakref.WeakSet()
_signal_lock = threading.Lock()
_signal_handlers_installed = False


def _close_all_open_catalogs() -> None:
    """Best-effort `close()` of every still-open `Catalog` — one failure
    must not stop the rest from being disposed too."""
    for catalog in list(_open_catalogs):
        try:
            catalog.close()
        except Exception:
            pass


atexit.register(_close_all_open_catalogs)


def _install_signal_handlers() -> None:
    """Close every open `Catalog` on SIGTERM/SIGINT before chaining to
    whatever handler was previously installed (e.g. Jupyter's own SIGINT
    handling) — covers a container orchestrator stopping the process, which
    doesn't run atexit hooks unless SIGTERM actually triggers interpreter
    shutdown.

    `signal.signal()` only works on the main thread; installed once
    globally (not per-`Catalog`) so multiple instances don't stack
    handlers. Silently skipped off the main thread — atexit still covers a
    normal interpreter shutdown from there.
    """
    global _signal_handlers_installed
    with _signal_lock:
        if _signal_handlers_installed or threading.current_thread() is not threading.main_thread():
            return
        previous_handlers: dict[int, Any] = {
            int(sig): signal.getsignal(sig) for sig in (signal.SIGTERM, signal.SIGINT)
        }

        def _handler(signum: int, frame: Any) -> None:
            _close_all_open_catalogs()
            previous = previous_handlers.get(signum)
            if callable(previous):
                previous(signum, frame)
            else:
                raise SystemExit(128 + signum)

        for sig in previous_handlers:
            signal.signal(sig, _handler)
        _signal_handlers_installed = True


class Catalog:
    """Dremio catalog operations, bound to two fixed connections.

    `self._executor` (REST) backs every operation except `datacopy` and
    `datamove`, which always go through `self._flight_executor` instead —
    see the module docstring for why. Pass `executor=`/`flight_executor=`
    to inject either one directly (e.g. a test's fake) and skip building a
    real `RestSqlExecutor`/`FlightSqlExecutor`.

    `username` (e.g. DREMIO_USER) is only needed for the Flight side — REST
    authenticates with `token` alone, but Flight authenticates via a
    basic-auth handshake (username + `token` as the password), not a raw
    bearer header (see `FlightSqlExecutor`'s docstring) — so it's required
    to actually call `datacopy`/`datamove` (a clear `CatalogOperationError`
    otherwise), optional if you never do.

    Call `close()` (or use `with Catalog(...) as catalog:`) once done —
    see the module docstring for why, and for the atexit/signal fallback
    that covers a caller who doesn't.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        username: str | None = None,
        flight_location: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        executor: SqlExecutor | None = None,
        flight_executor: SqlExecutor | None = None,
        catalog_rest: CatalogRestClient | None = None,
    ) -> None:
        self._executor = executor or RestSqlExecutor(base_url, token, timeout=timeout)
        # Constructing this doesn't connect or authenticate anything —
        # FlightSqlExecutor only opens its gRPC channel and does the
        # basic-auth handshake lazily, on first actual use — so building
        # one here costs nothing for a Catalog that never calls
        # datacopy/datamove.
        self._flight_executor = flight_executor or FlightSqlExecutor(
            resolve_flight_location(base_url, flight_location), username, token, timeout=timeout
        )
        # Folder existence/creation has no SQL or Flight equivalent, so it
        # always goes through Dremio's own REST catalog API directly,
        # independent of self._executor/self._flight_executor.
        self._catalog_rest = catalog_rest or CatalogRestClient(base_url, token)
        self._closed = False
        _open_catalogs.add(self)
        _install_signal_handlers()

    def __repr__(self) -> str:
        # No token here, same reasoning as IngestClient's own __repr__.
        return f"Catalog(executor={self._executor!r}, flight_executor={self._flight_executor!r})"

    def __enter__(self) -> Catalog:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Dispose the underlying Flight/REST session(s). Idempotent."""
        if self._closed:
            return
        self._closed = True
        for target in (self._executor, self._flight_executor, self._catalog_rest):
            close = getattr(target, "close", None)
            if callable(close):
                close()

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
        overwrite: bool = False,
        create_target_folder: bool = False,
        idempotency_key: str,
    ) -> SqlResult:
        # Arrow Flight, not REST — see the module docstring.
        return operations.datacopy(
            self._flight_executor,
            source_path,
            target_path,
            overwrite=overwrite,
            create_target_folder=create_target_folder,
            catalog_rest=self._catalog_rest,
            idempotency_key=idempotency_key,
        )

    def createversion(
        self,
        source_path: str,
        version_name: str,
        tables: list[str] | None = None,
        *,
        target_path: str,
        overwrite: bool = False,
        create_target_folder: bool = True,
        include_views: bool = False,
        idempotency_key: str,
    ) -> VersionResult:
        # Arrow Flight, not REST: it is a series of datacopy calls.
        return operations.createversion(
            self._flight_executor,
            self._catalog_rest,
            source_path,
            version_name,
            tables,
            target_path=target_path,
            overwrite=overwrite,
            create_target_folder=create_target_folder,
            include_views=include_views,
            idempotency_key=idempotency_key,
        )

    def datamove(
        self,
        source_path: str,
        target_path: str,
        *,
        entry_type: Literal["TABLE", "VIEW"] | None = None,
        overwrite: bool = False,
        create_target_folder: bool = False,
        idempotency_key: str,
    ) -> SqlResult:
        # Arrow Flight, not REST — see the module docstring.
        return operations.datamove(
            self._flight_executor,
            source_path,
            target_path,
            entry_type=entry_type,
            overwrite=overwrite,
            create_target_folder=create_target_folder,
            catalog_rest=self._catalog_rest,
            idempotency_key=idempotency_key,
        )

    def createview(
        self,
        source_path: str,
        target_path: str,
        *,
        overwrite: bool = False,
        create_target_folder: bool = False,
        idempotency_key: str,
    ) -> SqlResult:
        # Metadata-only (no data moves) — REST, not Flight, like table2view.
        return operations.createview(
            self._executor,
            source_path,
            target_path,
            overwrite=overwrite,
            create_target_folder=create_target_folder,
            catalog_rest=self._catalog_rest,
            idempotency_key=idempotency_key,
        )

    def deleteview(self, view_path: str, *, idempotency_key: str) -> SqlResult:
        return operations.deleteview(self._executor, view_path, idempotency_key=idempotency_key)

    def deletetable(self, table_path: str, *, idempotency_key: str) -> SqlResult:
        return operations.deletetable(self._executor, table_path, idempotency_key=idempotency_key)

    def gettablesfrom(self, schema_path: str, *, idempotency_key: str) -> list[str]:
        return operations.gettablesfrom(
            self._executor, schema_path, idempotency_key=idempotency_key
        )

    def gettableitemsfrom(self, table_path: str, *, idempotency_key: str) -> TableInfo:
        return operations.gettableitemsfrom(
            self._executor, table_path, idempotency_key=idempotency_key
        )

    def stream_rows(self, sql: str, *, idempotency_key: str) -> Iterator[pa.RecordBatch]:
        """Run a SELECT and yield its rows as Arrow `RecordBatch` chunks as they
        arrive, never the whole result at once (see `FlightSqlExecutor.
        stream_batches`). Always Arrow Flight: like `datacopy`/`datamove`,
        this moves data, and REST's poll-then-paginate shape doesn't scale to
        a bulk read."""
        return self._flight_executor.stream_batches(sql, idempotency_key=idempotency_key)

    def open_flight_channel(self) -> FlightSqlExecutor:
        """A Flight executor on its own gRPC channel, logged in as this
        catalog — for reading several large results at once, one channel
        each (see `FlightSqlExecutor.fork`). The caller closes it."""
        return self._flight_executor.fork()

    def getentriesfrom(self, schema_path: str, *, idempotency_key: str) -> list[CatalogEntry]:
        return operations.getentriesfrom(
            self._executor, schema_path, idempotency_key=idempotency_key
        )

    def entryexists(self, path: str, *, idempotency_key: str) -> bool:
        return operations.entryexists(self._executor, path, idempotency_key=idempotency_key)

    def getviewsql(self, view_path: str, *, idempotency_key: str) -> ViewDefinition:
        return operations.getviewsql(
            self._executor,
            view_path,
            catalog_rest=self._catalog_rest,
            idempotency_key=idempotency_key,
        )

    def createviewas(
        self,
        view_path: str,
        sql: str,
        *,
        overwrite: bool = False,
        create_target_folder: bool = True,
        idempotency_key: str,
    ) -> SqlResult:
        return operations.createviewas(
            self._executor,
            view_path,
            sql,
            overwrite=overwrite,
            create_target_folder=create_target_folder,
            catalog_rest=self._catalog_rest,
            idempotency_key=idempotency_key,
        )

    def getfoldersfrom(
        self, path: str, *, recursive: bool = True, idempotency_key: str
    ) -> list[str]:
        return operations.getfoldersfrom(
            self._catalog_rest, path, recursive=recursive, idempotency_key=idempotency_key
        )

    def getwikifrom(self, path: str, *, idempotency_key: str) -> str:
        return operations.getwikifrom(self._catalog_rest, path, idempotency_key=idempotency_key)

    def gettagsfrom(self, path: str, *, idempotency_key: str) -> list[str]:
        return operations.gettagsfrom(self._catalog_rest, path, idempotency_key=idempotency_key)

    def setwikito(
        self,
        path: str,
        text: str,
        *,
        tags: list[dict[str, str]] | None = None,
        idempotency_key: str,
    ) -> None:
        return operations.setwikito(
            self._catalog_rest, path, text, tags=tags, idempotency_key=idempotency_key
        )

    def deletewiki(self, path: str, *, idempotency_key: str) -> None:
        return operations.deletewiki(self._catalog_rest, path, idempotency_key=idempotency_key)

    def setmeta2wiki(
        self,
        path: str,
        *,
        tags: list[dict[str, str]] | None = None,
        overwrite: bool = True,
        idempotency_key: str,
    ) -> None:
        return operations.setmeta2wiki(
            self._catalog_rest, path, tags=tags, overwrite=overwrite, idempotency_key=idempotency_key
        )

    def getmetafromwiki(
        self,
        path: str,
        tag_name: str | None = None,
        field: Literal["tag_value", "tag_title"] | None = None,
        *,
        idempotency_key: str,
    ) -> list[dict[str, str]] | dict[str, str]:
        return operations.getmetafromwiki(
            self._catalog_rest, path, tag_name, field, idempotency_key=idempotency_key
        )

    def settagsto(self, path: str, tags: list[str], *, idempotency_key: str) -> None:
        return operations.settagsto(
            self._catalog_rest, path, tags, idempotency_key=idempotency_key
        )

    def deletetags(self, path: str, tags: list[str], *, idempotency_key: str) -> None:
        return operations.deletetags(
            self._catalog_rest, path, tags, idempotency_key=idempotency_key
        )

    def createfolder(
        self, path: str, *, create_parents: bool = False, idempotency_key: str
    ) -> None:
        return operations.createfolder(
            self._catalog_rest, path, create_parents=create_parents, idempotency_key=idempotency_key
        )

    def deletefolder(self, path: str, *, cascade: bool = False, idempotency_key: str) -> None:
        return operations.deletefolder(
            self._executor,
            self._catalog_rest,
            path,
            cascade=cascade,
            idempotency_key=idempotency_key,
        )

    # --- whole folders: scan / copy / move -------------------------------
    # These are orchestration over the operations above rather than one SQL
    # statement each, so they live in `foldertree` and are exposed here for
    # a caller who already has a Catalog. See that module's docstring for
    # what a move actually does, and in what order.

    def folderscan(
        self,
        source_path: str,
        target_path: str,
        *,
        idempotency_key: str,
        qualify_external: bool = True,
        on_progress: ProgressCallback | None = None,
    ) -> FolderTree:
        return foldertree.folderscan(
            self,
            source_path,
            target_path,
            idempotency_key=idempotency_key,
            qualify_external=qualify_external,
            on_progress=on_progress,
        )

    def foldercopy(
        self,
        source_path: str,
        target_path: str,
        *,
        idempotency_key: str,
        dry_run: bool = False,
        overwrite: bool = False,
        skip_existing: bool = True,
        verify_views: bool = True,
        copy_metadata: bool = True,
        qualify_external: bool = True,
        tree: FolderTree | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> FolderTransferReport:
        return foldertree.foldercopy(
            self,
            source_path,
            target_path,
            idempotency_key=idempotency_key,
            dry_run=dry_run,
            overwrite=overwrite,
            skip_existing=skip_existing,
            verify_views=verify_views,
            copy_metadata=copy_metadata,
            qualify_external=qualify_external,
            tree=tree,
            on_progress=on_progress,
        )

    def foldermove(
        self,
        source_path: str,
        target_path: str,
        *,
        idempotency_key: str,
        dry_run: bool = False,
        overwrite: bool = False,
        skip_existing: bool = True,
        verify_views: bool = True,
        copy_metadata: bool = True,
        qualify_external: bool = True,
        delete_source_root: bool = True,
        tree: FolderTree | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> FolderTransferReport:
        return foldertree.foldermove(
            self,
            source_path,
            target_path,
            idempotency_key=idempotency_key,
            dry_run=dry_run,
            overwrite=overwrite,
            skip_existing=skip_existing,
            verify_views=verify_views,
            copy_metadata=copy_metadata,
            qualify_external=qualify_external,
            delete_source_root=delete_source_root,
            tree=tree,
            on_progress=on_progress,
        )

    def retry_pending(self, idempotency_key: str) -> SqlResult | list[Any]:
        pending = retry_state.get(idempotency_key)
        if pending is None:
            raise KeyError(f"no pending operation remembered for {idempotency_key!r}")
        executor = self._flight_executor if pending.operation in _FLIGHT_OPERATIONS else self._executor
        return operations.retry_pending(executor, idempotency_key, catalog_rest=self._catalog_rest)
