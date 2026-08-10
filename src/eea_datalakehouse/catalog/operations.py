"""The catalog operations: table2view, draft2version, publishversion,
datacopy, datamove, deleteview, gettablesfrom, gettableitemsfrom.

Each takes a :class:`~eea_datalakehouse.catalog.sql.SqlExecutor` (REST or
Flight — the operation doesn't care which) and an `idempotency_key`. On
:class:`EngineStartingError` the attempt is remembered via
:mod:`eea_datalakehouse.catalog.retry_state` and re-raised; call
:func:`retry_pending` later (a fresh cell, a fresh process) to pick it back
up using the same key.

Every operation submits one SQL statement per `executor.execute()` call —
Dremio's SQL job API and Flight SQL endpoint both expect a single statement,
so a multi-step operation (table2view, datamove) runs as a short sequence of
calls rather than one semicolon-joined string.

Assumptions worth checking against your actual Dremio deployment before
relying on this:

* ``table2view`` is the one-time DROP TABLE + CREATE VIEW transition for a
  location that started as a physical table (e.g. straight off ingest) and
  is moving to being a view over some other table. ``publishversion`` is the
  ongoing, idempotent repoint once a location is *already* a view.
* ``datamove`` is implemented as copy-then-drop (CREATE ... AS SELECT / DROP)
  rather than a native Dremio catalog rename — this repo's Dremio is
  Community Edition, which folder.py's own comments say does not support
  the same promotion/catalog flow as Enterprise, so a SQL-only
  implementation was chosen over assuming a specific REST rename endpoint.
* Multi-step operations are safe to retry *from the start* only because
  every statement here is either idempotent (``IF EXISTS``) or fails loudly
  rather than silently duplicating. The one real gap: if ``datamove``'s
  CREATE succeeds but the following DROP then stalls on an engine starting,
  a blind retry's CREATE will fail with "already exists" — the error
  message says which step it got to, but resolving that specific case is
  left to the caller (check the catalog, then call retry_pending or finish
  the DROP by hand).
* ``table2view``'s ``create_target_folder=True`` (the default) needs a
  ``catalog_rest=`` (a :class:`~eea_datalakehouse.catalog.rest.CatalogRestClient`)
  to check/create the view's containing folder — folder creation has no SQL
  or Flight equivalent. Pass ``create_target_folder=False`` if you don't
  have one and know the folder already exists.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from . import retry_state
from .errors import CatalogOperationError, EngineStartingError
from .sql import SqlExecutor, SqlResult

if TYPE_CHECKING:
    from .rest import CatalogRestClient


def _quote_path(path: str) -> str:
    """Dot-separated Dremio SQL identifier, each segment double-quoted.

    ``"a.b.c"`` -> ``'"a"."b"."c"'`` — safe even when a segment collides with
    a reserved word or contains characters SQL would otherwise choke on.
    """
    return ".".join(f'"{segment}"' for segment in path.split("."))


def _quote_literal(value: str) -> str:
    """SQL string literal, with embedded single quotes doubled per the standard."""
    return "'" + value.replace("'", "''") + "'"


def _run_steps(
    executor: SqlExecutor,
    statements: list[str],
    *,
    operation: str,
    target: str,
    idempotency_key: str,
    params: dict[str, str],
) -> SqlResult:
    """Run `statements` one executor.execute() call at a time, in order.

    Returns the last statement's result. On EngineStartingError, records
    which step stalled (so the message says how far it got) and re-raises;
    on full success, clears any previously remembered attempt.
    """
    result: SqlResult | None = None
    for step, sql in enumerate(statements, start=1):
        try:
            result = executor.execute(sql, idempotency_key=idempotency_key)
        except EngineStartingError as exc:
            retry_state.record(
                idempotency_key,
                operation,
                target,
                f"step {step}/{len(statements)}: {exc}",
                params=params,
            )
            raise
    retry_state.clear(idempotency_key)
    assert result is not None
    return result


def _fetch_step(
    executor: SqlExecutor,
    sql: str,
    *,
    operation: str,
    target: str,
    idempotency_key: str,
    params: dict[str, str],
) -> list[dict[str, Any]]:
    """Like `_run_steps`, but for a single read query via `executor.fetch_all`."""
    try:
        rows = executor.fetch_all(sql, idempotency_key=idempotency_key)
    except EngineStartingError as exc:
        retry_state.record(idempotency_key, operation, target, f"step 1/1: {exc}", params=params)
        raise
    retry_state.clear(idempotency_key)
    return rows


def _run_actions(
    actions: list[Any],
    *,
    operation: str,
    target: str,
    idempotency_key: str,
    params: dict[str, Any],
) -> Any:
    """Like `_run_steps`, but for arbitrary zero-arg callables, not just SQL.

    Used where a single operation mixes SQL calls with something else that
    has no SQL equivalent (table2view's folder check/creation, via
    CatalogRestClient) — every step still shares one idempotency_key and one
    step-numbered retry_state record.
    """
    result: Any = None
    for step, action in enumerate(actions, start=1):
        try:
            result = action()
        except EngineStartingError as exc:
            retry_state.record(
                idempotency_key, operation, target, f"step {step}/{len(actions)}: {exc}", params=params
            )
            raise
    retry_state.clear(idempotency_key)
    return result


def _parent_path(path: str) -> str | None:
    """Everything before the last segment of `path`, or None if there isn't one."""
    parent, _, _ = path.rpartition(".")
    return parent or None


def _entry_exists(executor: SqlExecutor, path: str, *, idempotency_key: str) -> bool:
    """Whether `path` is a known table or view, via INFORMATION_SCHEMA.

    Transport-agnostic (REST or Flight, whichever `executor` is) — unlike
    the folder check in CatalogRestClient, which has no SQL equivalent.
    """
    schema_path = _parent_path(path)
    if schema_path is None:
        raise ValueError(f"{path!r} has no containing schema")
    leaf = path.rsplit(".", 1)[-1]
    sql = (
        'SELECT "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        f"WHERE \"TABLE_SCHEMA\" = {_quote_literal(schema_path)} "
        f"AND \"TABLE_NAME\" = {_quote_literal(leaf)}"
    )
    rows = executor.fetch_all(sql, idempotency_key=idempotency_key)
    return len(rows) > 0


def table2view(
    executor: SqlExecutor,
    view_path: str,
    source_path: str,
    *,
    create_target_folder: bool = True,
    catalog_rest: CatalogRestClient | None = None,
    idempotency_key: str,
) -> SqlResult:
    """Replace the table at `view_path` with a view selecting from `source_path`.

    One-time transition (see module docstring); safe to re-run from the
    start with the same `idempotency_key` — the DROP is ``IF EXISTS``.

    Checks `source_path` actually exists (as a table or view) before doing
    anything else, so a typo fails with a clear message instead of a
    confusing "CREATE VIEW" error. When `create_target_folder` is true (the
    default), also ensures `view_path`'s containing folder exists first,
    creating any missing levels — this requires `catalog_rest`; pass
    `create_target_folder=False` if you don't have one and know the folder
    is already there.
    """

    def check_source_exists() -> None:
        if not _entry_exists(executor, source_path, idempotency_key=idempotency_key):
            raise CatalogOperationError(f"source {source_path!r} does not exist")

    def ensure_target_folder() -> None:
        if not create_target_folder:
            return
        parent = _parent_path(view_path)
        if parent is None:
            return
        if catalog_rest is None:
            raise CatalogOperationError(
                "table2view(create_target_folder=True) needs catalog_rest= "
                "to check/create the target folder"
            )
        catalog_rest.ensure_folder_path(parent)

    def drop_existing() -> SqlResult:
        return executor.execute(
            f"DROP TABLE IF EXISTS {_quote_path(view_path)}", idempotency_key=idempotency_key
        )

    def create_view() -> SqlResult:
        return executor.execute(
            f"CREATE VIEW {_quote_path(view_path)} AS SELECT * FROM {_quote_path(source_path)}",
            idempotency_key=idempotency_key,
        )

    return _run_actions(
        [check_source_exists, ensure_target_folder, drop_existing, create_view],
        operation="table2view",
        target=view_path,
        idempotency_key=idempotency_key,
        params={
            "view_path": view_path,
            "source_path": source_path,
            "create_target_folder": create_target_folder,
        },
    )


def draft2version(
    executor: SqlExecutor,
    draft_path: str,
    version_path: str,
    *,
    idempotency_key: str,
) -> SqlResult:
    """Promote the draft table at `draft_path` into a permanent `version_path`.

    CTAS, not a move — the draft stays exactly where ingest left it. Not
    idempotent by itself (a retry after a real partial failure would hit
    "table already exists"); pass a fresh `version_path` per release, as the
    ``versions.v2025_1``-style convention already does.
    """
    statements = [
        f"CREATE TABLE {_quote_path(version_path)} AS SELECT * FROM {_quote_path(draft_path)}",
    ]
    return _run_steps(
        executor,
        statements,
        operation="draft2version",
        target=version_path,
        idempotency_key=idempotency_key,
        params={"draft_path": draft_path, "version_path": version_path},
    )


def publishversion(
    executor: SqlExecutor,
    consumer_view_path: str,
    version_path: str,
    *,
    idempotency_key: str,
) -> SqlResult:
    """Repoint the consumer-facing view at `version_path`.

    ``CREATE OR REPLACE VIEW`` — idempotent, safe to retry or re-run.
    `consumer_view_path` must already be a view (see ``table2view`` for the
    one-time conversion from a table).
    """
    statements = [
        f"CREATE OR REPLACE VIEW {_quote_path(consumer_view_path)} AS "
        f"SELECT * FROM {_quote_path(version_path)}",
    ]
    return _run_steps(
        executor,
        statements,
        operation="publishversion",
        target=consumer_view_path,
        idempotency_key=idempotency_key,
        params={"consumer_view_path": consumer_view_path, "version_path": version_path},
    )


def datacopy(
    executor: SqlExecutor,
    source_path: str,
    target_path: str,
    *,
    mode: Literal["create", "replace"] = "create",
    idempotency_key: str,
) -> SqlResult:
    """Copy data from `source_path` into `target_path`.

    ``mode="create"`` (default) is a plain CTAS — fails loudly if
    `target_path` already exists, so you don't silently overwrite something.
    ``mode="replace"`` drops `target_path` first (``IF EXISTS``, idempotent),
    for a caller that has already decided overwriting is correct.
    """
    statements = []
    if mode == "replace":
        statements.append(f"DROP TABLE IF EXISTS {_quote_path(target_path)}")
    statements.append(
        f"CREATE TABLE {_quote_path(target_path)} AS SELECT * FROM {_quote_path(source_path)}"
    )
    return _run_steps(
        executor,
        statements,
        operation="datacopy",
        target=target_path,
        idempotency_key=idempotency_key,
        params={"source_path": source_path, "target_path": target_path, "mode": mode},
    )


def datamove(
    executor: SqlExecutor,
    source_path: str,
    target_path: str,
    *,
    entry_type: Literal["TABLE", "VIEW"],
    idempotency_key: str,
) -> SqlResult:
    """Move a table or view from `source_path` to `target_path` (copy, then drop).

    `entry_type` must be given explicitly — this does not query
    ``INFORMATION_SCHEMA`` to detect it, so a retry never risks a second
    (possibly stale) round trip guessing what `source_path` is before doing
    the real work. See the module docstring for the one partial-failure case
    this doesn't fully cover (CREATE succeeds, DROP then stalls).
    """
    create_kind = "VIEW" if entry_type == "VIEW" else "TABLE"
    statements = [
        f"CREATE {create_kind} {_quote_path(target_path)} AS SELECT * FROM {_quote_path(source_path)}",
        f"DROP {create_kind} {_quote_path(source_path)}",
    ]
    return _run_steps(
        executor,
        statements,
        operation="datamove",
        target=target_path,
        idempotency_key=idempotency_key,
        params={
            "source_path": source_path,
            "target_path": target_path,
            "entry_type": entry_type,
        },
    )


def deleteview(
    executor: SqlExecutor,
    view_path: str,
    *,
    idempotency_key: str,
) -> SqlResult:
    """Delete the view at `view_path`.

    ``DROP VIEW IF EXISTS`` — idempotent, safe to retry or re-run even if the
    view is already gone. If `view_path` is actually a table rather than a
    view, this fails loudly rather than silently doing nothing or dropping
    the wrong kind of entry.
    """
    statements = [f"DROP VIEW IF EXISTS {_quote_path(view_path)}"]
    return _run_steps(
        executor,
        statements,
        operation="deleteview",
        target=view_path,
        idempotency_key=idempotency_key,
        params={"view_path": view_path},
    )


def _like_escape(value: str) -> str:
    """Escape a LIKE pattern's own wildcards so a literal `_`/`%` in a path
    segment (real folder/table names routinely contain underscores) isn't
    misread as "match any character"/"match anything"."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def gettablesfrom(
    executor: SqlExecutor,
    schema_path: str,
    *,
    idempotency_key: str,
) -> list[str]:
    """Full paths of every table and view under `schema_path`, at any depth.

    One query covers the whole subtree — no per-folder recursion needed:
    matches `TABLE_SCHEMA` equal to `schema_path` itself (direct children) or
    starting with `schema_path.` (anything nested under it, however deep,
    since SQL LIKE's `%` matches across dots too).

    Returns full dot-paths (schema + name), not bare names — two different
    subfolders can each have a table called the same thing, so a bare name
    would be ambiguous once subfolders are in play.

    Queries ``INFORMATION_SCHEMA."TABLES"``, which — per ANSI convention —
    covers both tables and views (``TABLE_TYPE`` distinguishes them, not
    selected here since this only returns paths); unverified against this
    project's actual Dremio version.
    """
    like_pattern = f"{_like_escape(schema_path)}.%"
    sql = (
        'SELECT "TABLE_SCHEMA", "TABLE_NAME" FROM INFORMATION_SCHEMA."TABLES" '
        f"WHERE \"TABLE_SCHEMA\" = {_quote_literal(schema_path)} "
        f"OR \"TABLE_SCHEMA\" LIKE {_quote_literal(like_pattern)} ESCAPE '\\'"
    )
    rows = _fetch_step(
        executor,
        sql,
        operation="gettablesfrom",
        target=schema_path,
        idempotency_key=idempotency_key,
        params={"schema_path": schema_path},
    )
    return [f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}" for row in rows]


@dataclass(frozen=True, slots=True)
class TableInfo:
    """Result of `gettableitemsfrom`: a table's schema and row count — no row data."""

    schema: dict[str, str]  # column name -> Dremio DATA_TYPE
    row_count: int


def gettableitemsfrom(
    executor: SqlExecutor,
    table_path: str,
    *,
    idempotency_key: str,
) -> TableInfo:
    """The schema and row count of `table_path`. Never fetches the actual rows.

    Raises `CatalogOperationError` if `table_path` doesn't exist — checked
    via `INFORMATION_SCHEMA."COLUMNS"`, which also supplies the schema, so
    this is one existence check that pays for itself rather than a second
    query on top of the real work. `row_count` comes from `SELECT COUNT(*)`
    — the table's true count, not bounded by anything fetched.
    """
    schema_path = _parent_path(table_path)
    if schema_path is None:
        raise ValueError(f"{table_path!r} has no containing schema")
    leaf = table_path.rsplit(".", 1)[-1]

    columns_sql = (
        'SELECT "COLUMN_NAME", "DATA_TYPE" FROM INFORMATION_SCHEMA."COLUMNS" '
        f"WHERE \"TABLE_SCHEMA\" = {_quote_literal(schema_path)} "
        f"AND \"TABLE_NAME\" = {_quote_literal(leaf)} "
        'ORDER BY "ORDINAL_POSITION"'
    )
    column_rows = _fetch_step(
        executor,
        columns_sql,
        operation="gettableitemsfrom",
        target=table_path,
        idempotency_key=idempotency_key,
        params={"table_path": table_path},
    )
    if not column_rows:
        raise CatalogOperationError(f"{table_path!r} does not exist")
    schema = {row["COLUMN_NAME"]: row["DATA_TYPE"] for row in column_rows}

    count_sql = f'SELECT COUNT(*) AS "row_count" FROM {_quote_path(table_path)}'
    count_rows = _fetch_step(
        executor,
        count_sql,
        operation="gettableitemsfrom",
        target=table_path,
        idempotency_key=idempotency_key,
        params={"table_path": table_path},
    )
    row_count = count_rows[0]["row_count"] if count_rows else 0

    return TableInfo(schema=schema, row_count=row_count)


_OPERATIONS = {
    "table2view": table2view,
    "draft2version": draft2version,
    "publishversion": publishversion,
    "datacopy": datacopy,
    "datamove": datamove,
    "deleteview": deleteview,
    "gettablesfrom": gettablesfrom,
    "gettableitemsfrom": gettableitemsfrom,
}


def retry_pending(executor: SqlExecutor, idempotency_key: str, **extra: Any) -> SqlResult | list[Any]:
    """Re-attempt whatever `idempotency_key` last stalled on.

    Looks up the remembered operation and params (retry_state.get) and
    dispatches back to the same function that recorded it. Raises
    ``KeyError`` if nothing is pending under that key.

    `**extra` (e.g. `catalog_rest=...`) is only forwarded to whichever
    operation actually declares that parameter — table2view's retry needs
    `catalog_rest`, draft2version's doesn't, and neither has to know about
    the other's dependencies. Nothing here is persisted to retry_state:
    credentials/clients are never JSON-serializable and never should be
    written to that file.
    """
    pending = retry_state.get(idempotency_key)
    if pending is None:
        raise KeyError(f"no pending operation remembered for {idempotency_key!r}")
    operation = _OPERATIONS[pending.operation]
    accepted = set(inspect.signature(operation).parameters)
    kwargs = {key: value for key, value in extra.items() if key in accepted}
    return operation(executor, **pending.params, idempotency_key=idempotency_key, **kwargs)
