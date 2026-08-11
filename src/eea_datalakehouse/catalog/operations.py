"""The catalog operations: table2view, draft2version, publishversion,
datacopy, datamove, deleteview, gettablesfrom, gettableitemsfrom,
getwikifrom, gettagsfrom, assignwikito, assigntagsto, deletetags.

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
* ``table2view``, ``datacopy``, and ``datamove`` all check `source_path`
  actually exists (as a table or view) before doing anything else, so a
  typo fails with a clear message instead of a confusing CTAS/CREATE VIEW
  error. Each also takes ``create_target_folder=`` — ``True`` by default
  for ``table2view``, ``False`` by default for ``datacopy``/``datamove`` —
  which, when true, needs a ``catalog_rest=`` (a
  :class:`~eea_datalakehouse.catalog.rest.CatalogRestClient`) to check/create
  the target's containing folder, since folder creation has no SQL or
  Flight equivalent.
* When Flight is the selected transport, ``FlightSqlExecutor`` holds one
  ``FlightClient`` (a persistent gRPC channel) for its whole lifetime
  instead of reconnecting per statement — data operations like
  ``datacopy``/``datamove`` run several statements (existence check,
  folder creation, the CTAS/DROP itself) through the same executor, so this
  session reuse is what keeps them from paying a fresh handshake per step.
* ``getwikifrom`` / ``gettagsfrom`` / ``assignwikito`` / ``assigntagsto`` are
  REST-only for the same reason: Dremio wikis/tags have no SQL or Flight
  equivalent, so they take a ``catalog_rest`` directly rather than a
  ``SqlExecutor``. ``getwikifrom`` raises if the entity has no wiki at all;
  ``gettagsfrom`` doesn't — zero tags is normal, only a missing path raises.
* ``assignwikito``/``assigntagsto`` create the wiki/tags if the entity has
  none yet, or overwrite them if it already has some — Dremio's
  collaboration API is versioned for optimistic concurrency, and the exact
  semantics of that version field are unverified against a real deployment
  (see rest.py's docstring). ``assigntagsto`` *replaces* the tag set, it
  doesn't merge with the existing tags.
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


def _entry_kind(executor: SqlExecutor, path: str, *, idempotency_key: str) -> Literal["TABLE", "VIEW"]:
    """Whether `path` is a Dremio TABLE or VIEW, via INFORMATION_SCHEMA's
    own ``TABLE_TYPE`` column — doubles as an existence check.

    Raises `CatalogOperationError` if `path` doesn't exist at all. Used by
    `datamove` to auto-detect `entry_type` rather than trusting a caller
    to have gotten it right — a ``DROP VIEW`` on an actual table (or vice
    versa) fails outright with "is not a VIEW"/"is not a TABLE", which is
    exactly the failure this sidesteps.
    """
    schema_path = _parent_path(path)
    if schema_path is None:
        raise ValueError(f"{path!r} has no containing schema")
    leaf = path.rsplit(".", 1)[-1]
    sql = (
        'SELECT "TABLE_TYPE" FROM INFORMATION_SCHEMA."TABLES" '
        f"WHERE \"TABLE_SCHEMA\" = {_quote_literal(schema_path)} "
        f"AND \"TABLE_NAME\" = {_quote_literal(leaf)}"
    )
    rows = executor.fetch_all(sql, idempotency_key=idempotency_key)
    if not rows:
        raise CatalogOperationError(f"{path!r} does not exist")
    return "VIEW" if "VIEW" in str(rows[0].get("TABLE_TYPE", "")).upper() else "TABLE"


def _resolve_target_path(
    catalog_rest: CatalogRestClient | None, target_path: str, source_path: str
) -> str:
    """`cp source dest/` semantics: if `target_path` is an existing folder
    rather than a specific table/view path, land the copy/move *inside* it
    under the source's own name, instead of literally trying to name the
    folder itself the target.

    Best-effort — with no `catalog_rest` to ask, `target_path` is used
    exactly as given (the pre-existing behavior).
    """
    if catalog_rest is not None and catalog_rest.is_folder(target_path):
        leaf = source_path.rsplit(".", 1)[-1]
        return f"{target_path}.{leaf}"
    return target_path


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
    overwrite: bool = False,
    create_target_folder: bool = False,
    catalog_rest: CatalogRestClient | None = None,
    idempotency_key: str,
) -> SqlResult:
    """Copy data from `source_path` into `target_path`.

    Checks `source_path` actually exists (as a table or view) before doing
    anything else, so a typo fails with a clear message instead of a
    confusing CTAS error. If `target_path` is an existing folder rather
    than a specific table/view path, the source's own name is appended to
    it — `cp source dest/` semantics, landing the copy inside that folder
    under the same name (needs `catalog_rest`; skipped, using `target_path`
    exactly as given, without one).

    `overwrite=False` (the default) checks the (possibly folder-adjusted)
    target explicitly and raises `CatalogOperationError` if it already
    exists, rather than letting a bare CTAS fail with Dremio's own less
    specific error — so you don't silently overwrite something.
    `overwrite=True` drops it first (``IF EXISTS``, idempotent), for a
    caller that has already decided overwriting is correct. When
    `create_target_folder` is true, also ensures its containing folder
    exists first, creating any missing levels — this requires
    `catalog_rest`.
    """
    params: dict[str, Any] = {
        "source_path": source_path,
        "target_path": target_path,
        "overwrite": overwrite,
        "create_target_folder": create_target_folder,
    }

    def check_source_exists() -> None:
        if not _entry_exists(executor, source_path, idempotency_key=idempotency_key):
            raise CatalogOperationError(f"source {source_path!r} does not exist")

    def resolve_target() -> None:
        nonlocal target_path
        target_path = _resolve_target_path(catalog_rest, target_path, source_path)
        params["target_path"] = target_path

    def ensure_target_folder() -> None:
        if not create_target_folder:
            return
        parent = _parent_path(target_path)
        if parent is None:
            return
        if catalog_rest is None:
            raise CatalogOperationError(
                "datacopy(create_target_folder=True) needs catalog_rest= "
                "to check/create the target folder"
            )
        catalog_rest.ensure_folder_path(parent)

    def check_or_drop_target() -> None:
        if not _entry_exists(executor, target_path, idempotency_key=idempotency_key):
            return  # nothing there — nothing to check or drop
        if not overwrite:
            raise CatalogOperationError(
                f"target {target_path!r} already exists — pass overwrite=True to replace it"
            )
        # Drop whatever it actually is (it might be a view, not a table,
        # from an earlier different operation) — DROP TABLE on a view (or
        # vice versa) fails outright, the same class of bug datamove's
        # entry_type auto-detection exists to avoid.
        existing_kind = _entry_kind(executor, target_path, idempotency_key=idempotency_key)
        executor.execute(
            f"DROP {existing_kind} IF EXISTS {_quote_path(target_path)}",
            idempotency_key=idempotency_key,
        )

    def copy_data() -> SqlResult:
        return executor.execute(
            f"CREATE TABLE {_quote_path(target_path)} AS SELECT * FROM {_quote_path(source_path)}",
            idempotency_key=idempotency_key,
        )

    return _run_actions(
        [check_source_exists, resolve_target, ensure_target_folder, check_or_drop_target, copy_data],
        operation="datacopy",
        target=target_path,
        idempotency_key=idempotency_key,
        params=params,
    )


def datamove(
    executor: SqlExecutor,
    source_path: str,
    target_path: str,
    *,
    entry_type: Literal["TABLE", "VIEW"] | None = None,
    overwrite: bool = False,
    create_target_folder: bool = False,
    catalog_rest: CatalogRestClient | None = None,
    idempotency_key: str,
) -> SqlResult:
    """Move a table or view from `source_path` to `target_path` (copy, then drop).

    `entry_type` is auto-detected via INFORMATION_SCHEMA (``TABLE_TYPE``,
    see `_entry_kind`) when not given — that same query doubles as the
    source-exists check. Getting `entry_type` wrong yourself (e.g.
    `source_path` was a table, not a view) fails the DROP outright with
    "is not a VIEW"/"is not a TABLE"; auto-detection sidesteps that
    entirely. Re-querying on a retry is safe here — `source_path` itself
    is untouched until the final DROP, so what it *is* can't have changed
    out from under a stalled attempt. Pass `entry_type` explicitly only to
    skip that lookup (you already know it) or override a detection you
    don't trust.

    If `target_path` is an existing folder rather than a specific
    table/view path, the source's own name is appended to it — `cp source
    dest/` semantics, landing the move inside that folder under the same
    name (needs `catalog_rest`; skipped, using `target_path` exactly as
    given, without one).

    `overwrite=False` (the default) checks the (possibly folder-adjusted)
    target explicitly and raises `CatalogOperationError` if it already
    exists, rather than letting a bare CTAS fail with Dremio's own less
    specific error. `overwrite=True` drops whatever is actually there
    first (detecting its real kind — it might be a view, not a table, or
    vice versa, from an earlier different operation; blindly dropping the
    wrong kind fails outright, same as the `entry_type` problem above).
    When `create_target_folder` is true, also ensures the (possibly
    folder-adjusted) target's containing folder exists first — this
    requires `catalog_rest`.

    After DROP, checks that `target_path` actually exists in the catalog
    before declaring success — neither CREATE nor DROP raising is proof
    either one actually happened server-side (Flight SQL's completion
    semantics for a no-result-rows DDL statement aren't something this
    module has verified against a real deployment); raises
    `CatalogOperationError` if the target isn't there. See the module
    docstring for the one partial-failure case this doesn't fully cover
    (CREATE succeeds, DROP then stalls).
    """
    create_kind: str = entry_type if entry_type is not None else "TABLE"
    params: dict[str, Any] = {
        "source_path": source_path,
        "target_path": target_path,
        "entry_type": entry_type,
        "overwrite": overwrite,
        "create_target_folder": create_target_folder,
    }

    def check_or_detect_source() -> None:
        nonlocal create_kind
        if entry_type is not None:
            if not _entry_exists(executor, source_path, idempotency_key=idempotency_key):
                raise CatalogOperationError(f"source {source_path!r} does not exist")
            return
        create_kind = _entry_kind(executor, source_path, idempotency_key=idempotency_key)

    def resolve_target() -> None:
        nonlocal target_path
        target_path = _resolve_target_path(catalog_rest, target_path, source_path)
        params["target_path"] = target_path

    def ensure_target_folder() -> None:
        if not create_target_folder:
            return
        parent = _parent_path(target_path)
        if parent is None:
            return
        if catalog_rest is None:
            raise CatalogOperationError(
                "datamove(create_target_folder=True) needs catalog_rest= "
                "to check/create the target folder"
            )
        catalog_rest.ensure_folder_path(parent)

    def check_or_drop_target() -> None:
        if not _entry_exists(executor, target_path, idempotency_key=idempotency_key):
            return  # nothing there — nothing to check or drop
        if not overwrite:
            raise CatalogOperationError(
                f"target {target_path!r} already exists — pass overwrite=True to replace it"
            )
        existing_kind = _entry_kind(executor, target_path, idempotency_key=idempotency_key)
        executor.execute(
            f"DROP {existing_kind} IF EXISTS {_quote_path(target_path)}",
            idempotency_key=idempotency_key,
        )

    move_result: SqlResult | None = None

    def move_data() -> SqlResult:
        nonlocal move_result
        move_result = executor.execute(
            f"CREATE {create_kind} {_quote_path(target_path)} AS "
            f"SELECT * FROM {_quote_path(source_path)}",
            idempotency_key=idempotency_key,
        )
        return move_result

    def drop_source() -> SqlResult:
        return executor.execute(
            f"DROP {create_kind} {_quote_path(source_path)}", idempotency_key=idempotency_key
        )

    def verify_target_exists() -> SqlResult:
        # Neither CREATE nor DROP raising is proof either one actually
        # happened — Arrow Flight SQL's completion semantics for a DDL
        # statement with no result rows are exactly the kind of thing this
        # module's own docstring flags as unverified against a real
        # deployment. Check the catalog itself rather than trust the
        # executor's silence.
        if not _entry_exists(executor, target_path, idempotency_key=idempotency_key):
            raise CatalogOperationError(
                f"target {target_path!r} does not exist after CREATE {create_kind} — "
                "the move did not actually complete"
            )
        # _run_actions returns whichever action ran last — this one, not
        # move_data — so it has to be the one to hand back the real result.
        assert move_result is not None
        return move_result

    return _run_actions(
        [
            check_or_detect_source,
            resolve_target,
            ensure_target_folder,
            check_or_drop_target,
            move_data,
            drop_source,
            verify_target_exists,
        ],
        operation="datamove",
        target=target_path,
        idempotency_key=idempotency_key,
        params=params,
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


def getwikifrom(
    catalog_rest: CatalogRestClient,
    path: str,
    *,
    idempotency_key: str,
) -> str:
    """The Dremio wiki text attached to the catalog entity at `path`.

    REST-only (see module docstring) — takes `catalog_rest` directly rather
    than a `SqlExecutor`. Raises `CatalogOperationError` if `path` doesn't
    exist or has no wiki.
    """
    try:
        wiki = catalog_rest.get_wiki(path)
    except EngineStartingError as exc:
        retry_state.record(idempotency_key, "getwikifrom", path, str(exc), params={"path": path})
        raise
    retry_state.clear(idempotency_key)
    return wiki


def gettagsfrom(
    catalog_rest: CatalogRestClient,
    path: str,
    *,
    idempotency_key: str,
) -> list[str]:
    """The Dremio tags attached to the catalog entity at `path`.

    REST-only (see module docstring) — takes `catalog_rest` directly rather
    than a `SqlExecutor`. Raises `CatalogOperationError` only if `path`
    itself doesn't exist; an entity with zero tags is a normal state and
    returns an empty list, not an error (unlike `getwikifrom`).
    """
    try:
        tags = catalog_rest.get_tags(path)
    except EngineStartingError as exc:
        retry_state.record(idempotency_key, "gettagsfrom", path, str(exc), params={"path": path})
        raise
    retry_state.clear(idempotency_key)
    return tags


def assignwikito(
    catalog_rest: CatalogRestClient,
    path: str,
    text: str,
    *,
    idempotency_key: str,
) -> None:
    """Create or overwrite the Dremio wiki text on the catalog entity at `path`.

    REST-only (see module docstring) — takes `catalog_rest` directly rather
    than a `SqlExecutor`. Raises `CatalogOperationError` if `path` doesn't
    exist.
    """
    try:
        catalog_rest.set_wiki(path, text)
    except EngineStartingError as exc:
        retry_state.record(
            idempotency_key, "assignwikito", path, str(exc), params={"path": path, "text": text}
        )
        raise
    retry_state.clear(idempotency_key)


def assigntagsto(
    catalog_rest: CatalogRestClient,
    path: str,
    tags: list[str],
    *,
    idempotency_key: str,
) -> None:
    """Replace the Dremio tags on the catalog entity at `path` with `tags`.

    REST-only (see module docstring) — takes `catalog_rest` directly rather
    than a `SqlExecutor`. Raises `CatalogOperationError` if `path` doesn't
    exist. This *replaces* the tag set — pass the union of old and new tags
    if you want to keep the existing ones (e.g. via `gettagsfrom` first).
    """
    try:
        catalog_rest.set_tags(path, tags)
    except EngineStartingError as exc:
        retry_state.record(
            idempotency_key, "assigntagsto", path, str(exc), params={"path": path, "tags": tags}
        )
        raise
    retry_state.clear(idempotency_key)


def deletetags(
    catalog_rest: CatalogRestClient,
    path: str,
    tags: list[str],
    *,
    idempotency_key: str,
) -> None:
    """Remove `tags` from the catalog entity at `path`, leaving any others intact.

    REST-only (see module docstring) — fetches the current tag set via
    `get_tags` and calls `set_tags` with `tags` removed from it, since
    Dremio's collaboration API has no separate delete-tags endpoint to
    speak of. Raises `CatalogOperationError` if `path` doesn't exist.
    """
    try:
        remaining = [tag for tag in catalog_rest.get_tags(path) if tag not in tags]
        catalog_rest.set_tags(path, remaining)
    except EngineStartingError as exc:
        retry_state.record(
            idempotency_key, "deletetags", path, str(exc), params={"path": path, "tags": tags}
        )
        raise
    retry_state.clear(idempotency_key)


_OPERATIONS = {
    "table2view": table2view,
    "draft2version": draft2version,
    "publishversion": publishversion,
    "datacopy": datacopy,
    "datamove": datamove,
    "deleteview": deleteview,
    "gettablesfrom": gettablesfrom,
    "gettableitemsfrom": gettableitemsfrom,
    "getwikifrom": getwikifrom,
    "gettagsfrom": gettagsfrom,
    "assignwikito": assignwikito,
    "assigntagsto": assigntagsto,
    "deletetags": deletetags,
}


def retry_pending(
    executor: SqlExecutor, idempotency_key: str, **extra: Any
) -> SqlResult | list[Any] | str:
    """Re-attempt whatever `idempotency_key` last stalled on.

    Looks up the remembered operation and params (retry_state.get) and
    dispatches back to the same function that recorded it. Raises
    ``KeyError`` if nothing is pending under that key.

    `executor` and `**extra` (e.g. `catalog_rest=...`) are matched to the
    dispatched operation's parameters *by name*, not position — some
    operations (getwikifrom) don't take an `executor` at all, so it's only
    passed when the operation actually declares that parameter. Nothing
    here is persisted to retry_state: credentials/clients are never
    JSON-serializable and never should be written to that file.
    """
    pending = retry_state.get(idempotency_key)
    if pending is None:
        raise KeyError(f"no pending operation remembered for {idempotency_key!r}")
    operation = _OPERATIONS[pending.operation]
    accepted = set(inspect.signature(operation).parameters)
    kwargs: dict[str, Any] = dict(pending.params)
    kwargs["idempotency_key"] = idempotency_key
    if "executor" in accepted:
        kwargs["executor"] = executor
    for key, value in extra.items():
        if key in accepted:
            kwargs[key] = value
    return operation(**kwargs)
