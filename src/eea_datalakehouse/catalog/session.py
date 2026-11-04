"""`CatalogSession` — queue catalog verbs, then commit them as one batch.

Prototype of the design in `docs/notebook-facade-for-data-scientists.md`
("Queue calls, then commit"). Wraps an existing `Catalog`, so nothing here
reimplements catalog behaviour — it only hides the ceremony (idempotency
keys) and adds queue/commit/rollback on top::

    session = CatalogSession(catalog)
    session.data_copy("draft.raw_2026", "bwd.reference.water_temperature")
    session.set_tags("bwd.reference.water_temperature", ["reviewed"])
    session.commit(retry=True)

`commit()` is all-or-nothing: every queued step runs in order, and the
moment one fails, everything already done in *this* commit is undone before
the failure is reported — see each verb's own docstring below for exactly
what its compensating action is, and where one isn't possible (`delete_folder`,
`delete_view`/`delete_table`, or `data_copy`/`data_move` with `overwrite=True`)
that limitation is raised as part of the failure, not hidden.

This is NOT the same "session" `dds_ingestion.folder.FolderIngest` already
has (a server-side transfer identified by `session_id`, resumed via
`attach`/`retry`) — `CatalogSession` is a client-side batch queue with no
server-side counterpart at all; the two just happen to share an overloaded
English word. See `eea_datalakehouse.dds_ingestion.session.IngestSession`
for the ingestion-side equivalent of *this* kind of session — deliberately a
separate object with its own `commit()`, not a shared queue, because a
catalog operation can't run before its target has actually been ingested
(see the design doc's "Two sessions, not one").
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from . import operations
from .client import Catalog
from .errors import CatalogAuthError, CatalogOperationError, EngineStartingError
from .operations import TableInfo
from .sql import SqlExecutor

_Undo = Callable[[], None]


class CatalogSessionError(RuntimeError):
    """Base for `CatalogSession`-specific errors."""


class CatalogCommitError(CatalogSessionError):
    """`commit()` failed partway through the batch.

    `rolled_back` is `True` only if every already-succeeded step in this
    commit was cleanly undone. When it's `False`, `unresolved` names which
    steps are NOT undone — either because that verb has no compensating
    action at all (`delete_folder`, `delete_view`/`delete_table`; `data_copy`/
    `data_move` with `overwrite=True`), or
    because undoing it was attempted and itself failed. Either way, the
    catalog is left in a state this object cannot fully explain away, and a
    human needs to look.
    """

    def __init__(
        self,
        message: str,
        *,
        failed_step: str,
        original_error: Exception,
        rolled_back: bool,
        unresolved: list[str],
    ) -> None:
        super().__init__(message)
        self.failed_step = failed_step
        self.original_error = original_error
        self.rolled_back = rolled_back
        self.unresolved = unresolved


@dataclass
class CommitReport:
    """What a successful `commit()` actually did, in order.

    `notes` carries what a step wants the caller to know beyond "it
    succeeded" — e.g. the tables `create_version` skipped, and why.
    """

    succeeded: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


class CatalogListing(list[str]):  # noqa: FURB189 — a real list, so it compares/pastes like one
    """What `CatalogSession.list` returns: the full paths, as a plain
    ``list[str]`` — compares equal to one, and each entry can be passed on
    as is (e.g. in `create_version`'s `tables`) — plus `kinds`, each path's
    ``"TABLE"`` or ``"VIEW"``. Displayed (repr, or a notebook cell's output)
    as a JSON array of ``{"type": ..., "name": ...}`` objects — see
    `as_dicts`."""

    def __init__(self, entries: list[tuple[str, str]] | None = None) -> None:
        entries = entries or []
        super().__init__(path for path, _ in entries)
        self.kinds: dict[str, str] = dict(entries)

    def as_dicts(self) -> list[dict[str, str]]:
        """One ``{"type": "TABLE" | "VIEW", "name": full path}`` per entry, in order."""
        return [{"type": self.kinds.get(path, "?"), "name": path} for path in self]

    def to_json(self, indent: int | None = 2) -> str:
        """`as_dicts` as a JSON array."""
        return json.dumps(self.as_dicts(), indent=indent)

    def __repr__(self) -> str:
        return self.to_json()

    def _repr_pretty_(self, printer: Any, cycle: bool) -> None:
        printer.text(repr(self))


class _Irreversible(Exception):  # noqa: N818 — internal control-flow signal, not a user-facing error
    """Raised by a step's `run` to say: this succeeded, but cannot be undone."""


@dataclass
class _Step:
    description: str
    run: Callable[[Catalog, str], _Undo | None]
    # Filled in by `run`, reported in `CommitReport.notes` once the commit succeeds.
    notes: list[str] = field(default_factory=list)


def _read_wiki_or_none(catalog: Catalog, path: str, *, idempotency_key: str) -> str | None:
    """The wiki text at `path`, or `None` if it doesn't have one (or doesn't
    exist) — `getwikifrom` can't tell those two apart, and neither needs to
    here: either way there's nothing to restore on rollback."""
    try:
        return catalog.getwikifrom(path, idempotency_key=idempotency_key)
    except CatalogOperationError:
        return None


DRAFT_FOLDER = "draft"
# Where draft_to_version puts versions: a sibling of the draft folder, as in
# debugger/bathing_water/README.md's layout (…bwd.draft / …bwd.versions).
VERSIONS_FOLDER = "versions"

# The tag update_latest_release puts on the views it creates (and removes from
# everything under the Area's versions folder); publish copies the views that carry it.
PUBLIC_TAG = "#public"
# Where publish copies them: PUBLIC_ROOT.<the view's full catalog path>, so
# catalog.a.b.c.v lands at "public_s3"."dh-prod-data-public"."catalog"."a"."b"."c"."v".
PUBLIC_ROOT = "public_s3.dh-prod-data-public"

# The Area's own folders publish never copies from: work in progress, the
# frozen versions, and the shared reference data.
REFERENCE_FOLDER = "reference"
# Either spelling is a dataflow's reference folder.
_REFERENCE_FOLDERS = (REFERENCE_FOLDER, "references")
_NOT_PUBLISHED = (DRAFT_FOLDER, VERSIONS_FOLDER, *_REFERENCE_FOLDERS)

_ROOT_SOURCE = "catalog"
# This deployment's one real top-level source/space (see e.g. the "catalog." prefix
# on every real path in debugger/test_catalog_magic.ipynb's TEST_ROOT) — a bare path
# starting with it is therefore unambiguously absolute, context or not; see
# `_resolve_path`.


def _dotted(path: str) -> str:
    """``catalog/a/b`` (the slash form the catalog tree and DDS use) as
    ``catalog.a.b``, the form every verb here expects; a dotted path is
    returned unchanged."""
    if "/" not in path:
        return path
    return ".".join(segment for segment in path.split("/") if segment)


def _drop_entry(catalog: Catalog, path: str, *, idempotency_key: str) -> None:
    """Drop whatever entity (table or view) now sits at `path`.

    Used to undo a fresh `data_copy`. Reaches into `operations`' own private
    helpers (`_entry_kind`, `_quote_path`) rather than a public op, because
    the public `deleteview`/`deletetable` (via `Catalog`) run over REST, not
    Flight — this needs to stay on `catalog._flight_executor` like the
    `data_copy`/`data_move` it's undoing. Acceptable here since this module
    lives in the same package as `operations.py`.
    """
    executor = catalog._flight_executor  # noqa: SLF001 — same-package internal, see docstring
    kind = operations._entry_kind(executor, path, idempotency_key=idempotency_key)  # noqa: SLF001
    executor.execute(
        f"DROP {kind} IF EXISTS {operations._quote_path(path)}",  # noqa: SLF001
        idempotency_key=idempotency_key,
    )


def _quote(path: str) -> str:
    return operations._quote_path(path)  # noqa: SLF001 — same-package internal, as in _drop_entry


def _quote_name(column: str) -> str:
    """A column name as a quoted SQL identifier."""
    return '"' + column.replace('"', '""') + '"'


def _published_rows(executor: SqlExecutor, table: str, *, idempotency_key: str) -> int:
    """``COUNT(*)`` of a table `publish` just created — proof Dremio can read it."""
    try:
        rows = executor.fetch_all(
            f'SELECT COUNT(*) AS "row_count" FROM {_quote(table)}',
            idempotency_key=idempotency_key,
        )
        return int(rows[0]["row_count"])
    except (EngineStartingError, CatalogAuthError):
        raise
    except Exception as exc:
        raise CatalogOperationError(f"{table!r} was not created successfully: {exc}") from exc


def _create_folders(catalog: Catalog, folder: str | None) -> list[str]:
    """Create every missing level of `folder` (if any); the levels created, top first."""
    if folder is None:
        return []
    return catalog._catalog_rest.ensure_folder_path(folder)  # noqa: SLF001 — as in _drop_entry


def _delete_folders(catalog: Catalog, folders: list[str], *, idempotency_key: str) -> None:
    """Undo `_create_folders`: delete the levels it created, deepest first."""
    for folder in reversed(folders):
        catalog.deletefolder(folder, idempotency_key=idempotency_key)


def _plan_version_views(
    catalog: Catalog,
    dataflow: str,
    version: str,
    overwrite: bool,
    key: str,
    notes: list[str],
) -> list[tuple[str, str, bool]]:
    """``(table, view, view_exists)`` for every table in `version` — see
    `CatalogSession.update_latest_release`. Checks every target first, so nothing
    is created when one of them is refused; skipped views go into `notes`."""
    executor = catalog._executor  # noqa: SLF001 — same-package internal, as in _drop_entry
    plan: list[tuple[str, str, bool]] = []
    problems: list[str] = []
    for number, source in enumerate(
        catalog._catalog_rest.list_datasets(version.split(".")), start=1  # noqa: SLF001
    ):
        check_key = f"{key}-check-{number}"
        if operations._entry_kind(executor, source, idempotency_key=check_key) == "VIEW":  # noqa: SLF001
            notes.append(f"skipped {source!r}: view — only tables get a view")
            continue
        relative = source[len(version) + 1 :]
        if relative.split(".", 1)[0] in (DRAFT_FOLDER, VERSIONS_FOLDER):
            problems.append(f"{source!r} would put its view inside {dataflow}.{relative}")
            continue
        target = f"{dataflow}.{relative}"
        existed = operations._entry_exists(executor, target, idempotency_key=check_key)  # noqa: SLF001
        if existed:
            kind = operations._entry_kind(executor, target, idempotency_key=check_key)  # noqa: SLF001
            if kind == "TABLE":
                problems.append(f"{target!r} is a table — a view never replaces one")
            elif not overwrite:
                problems.append(f"view {target!r} already exists — pass overwrite=True")
        plan.append((source, target, existed))
    if problems:
        raise CatalogOperationError("no views created: " + "; ".join(problems))
    if not plan:
        raise CatalogOperationError(f"{version!r} has no tables to create views for")
    return plan


def _add_tag(catalog: Catalog, path: str, tag: str, *, idempotency_key: str) -> None:
    """Add `tag` to `path`'s tags, keeping the others."""
    tags = catalog.gettagsfrom(path, idempotency_key=f"{idempotency_key}-read")
    if tag not in tags:
        catalog.settagsto(path, [*tags, tag], idempotency_key=idempotency_key)


def _remove_tag_under(
    catalog: Catalog, folder: str, tag: str, removed: list[tuple[str, list[str]]], *, key: str
) -> None:
    """Remove `tag` from every table and view under `folder`, appending
    ``(path, tags before)`` to `removed` as it goes, so an undo can restore them."""
    rest = catalog._catalog_rest  # noqa: SLF001 — same-package internal, as in _drop_entry
    for number, path in enumerate(rest.list_datasets(folder.split(".")), start=1):
        tags = catalog.gettagsfrom(path, idempotency_key=f"{key}-{number}-read")
        if tag in tags:
            catalog.settagsto(
                path, [t for t in tags if t != tag], idempotency_key=f"{key}-{number}"
            )
            removed.append((path, tags))


# What `publish(partition_by=...)` takes: the same fields for every view, or
# each view's own (keyed by its path below the Area).
PartitionBy = list[str] | tuple[str, ...] | set[str] | dict[str, list[str]]


def _partition_fields(value: Any, what: str) -> tuple[str, ...]:
    """`value` as an ordered tuple of field names; a set is sorted, having no order."""
    if isinstance(value, str) or not isinstance(value, list | tuple | set | frozenset):
        raise CatalogSessionError(
            f"{what} must be a list of field names, e.g. ['year'], not {type(value).__name__}"
        )
    fields = tuple(sorted(value) if isinstance(value, set | frozenset) else value)
    if not fields:
        raise CatalogSessionError(f"{what} must name at least one field")
    bad = [f for f in fields if not isinstance(f, str) or not f.strip()]
    if bad:
        raise CatalogSessionError(f"{what} has fields that are not names: {bad!r}")
    fields = tuple(f.strip() for f in fields)
    repeated = sorted({f for f in fields if fields.count(f) > 1})
    if repeated:
        raise CatalogSessionError(f"{what} repeats {', '.join(repeated)}")
    return fields


def _partition_plan(
    partition_by: PartitionBy | None,
) -> tuple[str, ...] | dict[str, tuple[str, ...]]:
    """`publish`'s `partition_by`, checked: one field tuple for every view, or
    a dict from view path (below the Area, dotted) to its fields."""
    if partition_by is None:
        return ()
    if not isinstance(partition_by, dict):
        return _partition_fields(partition_by, "partition_by")
    plan: dict[str, tuple[str, ...]] = {}
    for view, fields in partition_by.items():
        if not isinstance(view, str) or not view.strip(" ./"):
            raise CatalogSessionError(f"partition_by key {view!r} is not a view path")
        name = _dotted(view.strip()).strip(".")
        plan[name] = _partition_fields(fields, f"partition_by[{view!r}]")
    return plan


def _view_columns(executor: SqlExecutor, view: str, *, idempotency_key: str) -> list[str]:
    """The column names of `view`, in order, from ``INFORMATION_SCHEMA."COLUMNS"``."""
    schema, _, leaf = view.rpartition(".")
    quote = operations._quote_literal  # noqa: SLF001 — same-package internal
    rows = executor.fetch_all(
        'SELECT "COLUMN_NAME" FROM INFORMATION_SCHEMA."COLUMNS" '
        f'WHERE "TABLE_SCHEMA" = {quote(schema)} AND "TABLE_NAME" = {quote(leaf)} '
        'ORDER BY "ORDINAL_POSITION"',
        idempotency_key=idempotency_key,
    )
    return [str(row["COLUMN_NAME"]) for row in rows]


def _plan_publish(
    catalog: Catalog,
    area: str,
    target_root: str,
    overwrite: bool,
    partitions: tuple[str, ...] | dict[str, tuple[str, ...]],
    key: str,
    notes: list[str],
) -> list[tuple[str, str, bool, tuple[str, ...]]]:
    """``(view, target, target_exists, partition columns)`` for every
    `PUBLIC_TAG` view under `area` outside its draft, versions and reference
    folders — see `CatalogSession.publish`. Checks every target and every
    partition field first, so nothing is copied when one is refused."""
    executor = catalog._executor  # noqa: SLF001 — same-package internal, as in _drop_entry
    rest = catalog._catalog_rest  # noqa: SLF001
    internal = tuple(f"{area}.{folder}." for folder in _NOT_PUBLISHED)
    plan: list[tuple[str, str, bool, tuple[str, ...]]] = []
    problems: list[str] = []
    for number, (path, kind) in enumerate(rest.list_dataset_kinds(area.split(".")), start=1):
        if kind != "VIEW" or path.startswith(internal):
            continue
        if PUBLIC_TAG not in rest.get_tags(path):
            notes.append(f"skipped {path!r}: no {PUBLIC_TAG} tag")
            continue
        relative = path[len(area) + 1 :]
        target = f"{target_root}.{relative}"
        existed = operations._entry_exists(  # noqa: SLF001
            executor, target, idempotency_key=f"{key}-check-{number}"
        )
        if existed and not overwrite:
            problems.append(f"{target!r} already exists — pass overwrite=True")
        fields = partitions.get(relative, ()) if isinstance(partitions, dict) else partitions
        columns: tuple[str, ...] = ()
        if fields:
            # Dremio matches column names ignoring case; use the view's own spelling.
            known = {
                name.lower(): name
                for name in _view_columns(
                    executor, path, idempotency_key=f"{key}-columns-{number}"
                )
            }
            missing = [field for field in fields if field.lower() not in known]
            if missing:
                problems.append(
                    f"{path!r} has no field(s) {', '.join(missing)} to partition by "
                    f"(its fields: {', '.join(known.values()) or 'none'})"
                )
            columns = tuple(known.get(field.lower(), field) for field in fields)
        plan.append((path, target, existed, columns))
    if isinstance(partitions, dict):
        published = {view[len(area) + 1 :] for view, *_ in plan}
        unknown = sorted(set(partitions) - published)
        if unknown:
            problems.append(
                "partition_by names view(s) that are not published: "
                + ", ".join(repr(view) for view in unknown)
            )
    if problems:
        raise CatalogOperationError("nothing published: " + "; ".join(problems))
    if not plan:
        raise CatalogOperationError(
            f"{area!r} has no views tagged {PUBLIC_TAG!r} — run update_latest_release first"
        )
    return plan


# Where publish keeps a copy of each table it replaces until the publish is done:
# a sibling of the Area's own folder in the bucket.
_BACKUP_SUFFIX = "__publish_backup"


class _Publication:
    """One `CatalogSession.publish` run: back up, copy, verify — or roll back.

    `run` first backs up every target that already exists, then publishes each
    view. Any failure drops every table it touched, restores the replaced ones
    from their backups and raises `CatalogOperationError`; the backups are
    dropped at the end either way, unless one could not be restored."""

    def __init__(self, executor: SqlExecutor, backup_root: str, key: str) -> None:
        self._executor = executor
        self._target_root, _, _ = backup_root.partition(_BACKUP_SUFFIX)
        self._backup_root = backup_root
        self._key = key
        self._statement = 0
        self.touched: list[str] = []  # targets dropped or created, in order
        self.backups: list[tuple[str, str]] = []  # (target, its backup)
        self.created: list[str] = []  # targets that did not exist before

    def run(
        self, plan: list[tuple[str, str, bool, tuple[str, ...]]], notes: list[str]
    ) -> None:
        current = "the backups"
        try:
            for _, target, existed, _ in plan:
                if existed:
                    current = f"the backup of {target!r}"
                    backup = self._backup_of(target)
                    self.backups.append((target, backup))
                    self._copy(target, backup, ())
            for view, target, existed, columns in plan:
                current = f"{view!r} to {target!r}"
                self.touched.append(target)
                rows = self._copy(view, target, columns)
                if not existed:
                    self.created.append(target)
                action = "replaced" if existed else "copied"
                by = f", partitioned by {', '.join(columns)}" if columns else ""
                notes.append(f"{action} {target!r} from {view!r} ({rows} rows{by})")
        except Exception as exc:
            kept = self._roll_back()
            restored = len(self.backups) - len(kept)
            outcome = (
                f"rolled back: every table it touched was dropped and {restored} replaced "
                "table(s) restored"
                if not kept
                else "rollback incomplete — restore these by hand from their backups: "
                + "; ".join(f"{target!r} from {backup!r}" for target, backup in kept)
            )
            raise CatalogOperationError(f"publishing {current} failed: {exc}; {outcome}") from exc
        problems = self._drop_backups([backup for _, backup in self.backups])
        notes.extend(f"could not drop backup {problem}" for problem in problems)

    def drop_published(self) -> None:
        """Undo: drop the tables this publish created (only reversible ones)."""
        for table in reversed(self.created):
            self._sql(f"DROP TABLE IF EXISTS {_quote(table)}")

    # -- internals ----------------------------------------------------------

    def _backup_of(self, target: str) -> str:
        return f"{self._backup_root}{target[len(self._target_root) :]}"

    def _sql(self, statement: str) -> None:
        self._statement += 1
        self._executor.execute(statement, idempotency_key=f"{self._key}-{self._statement}")

    def _copy(self, source: str, target: str, columns: tuple[str, ...]) -> int:
        """Drop `target`, create it from `source` as Parquet, refresh it and
        return its row count — raising if any of that fails."""
        self._sql(f"DROP TABLE IF EXISTS {_quote(target)}")
        partition = (
            f"PARTITION BY ({', '.join(_quote_name(c) for c in columns)}) " if columns else ""
        )
        self._sql(
            f"CREATE TABLE {_quote(target)} {partition}"
            f"STORE AS (type => 'parquet') AS SELECT * FROM {_quote(source)}"
        )
        self._sql(f"ALTER TABLE {_quote(target)} REFRESH METADATA AUTO PROMOTION")
        self._statement += 1
        return _published_rows(
            self._executor, target, idempotency_key=f"{self._key}-{self._statement}"
        )

    def _roll_back(self) -> list[tuple[str, str]]:
        """Drop what was touched, restore what was replaced; the
        ``(target, backup)`` pairs that could not be restored (kept)."""
        for table in reversed(self.touched):
            try:
                self._sql(f"DROP TABLE IF EXISTS {_quote(table)}")
            except Exception:  # noqa: BLE001, S112 — restoring below matters more
                continue
        replaced = set(self.touched)
        kept: list[tuple[str, str]] = []
        restored: list[str] = []
        for target, backup in self.backups:
            if target not in replaced:
                restored.append(backup)  # never dropped: only its backup needs removing
                continue
            try:
                self._copy(backup, target, ())
                restored.append(backup)
            except Exception:  # noqa: BLE001 — reported by the caller, backup kept
                kept.append((target, backup))
        self._drop_backups(restored)
        return kept

    def _drop_backups(self, backups: list[str]) -> list[str]:
        """Drop `backups`; a ``'path': error`` for each that failed."""
        problems: list[str] = []
        for backup in backups:
            try:
                self._sql(f"DROP TABLE IF EXISTS {_quote(backup)}")
            except Exception as exc:  # noqa: BLE001 — a leftover backup is reported, not fatal
                problems.append(f"{backup!r}: {exc}")
        return problems


class CatalogSession:
    """Queue catalog verbs against `catalog`, then `commit()` them as one batch.

    Every queueing method (`data_copy`, `data_move`, `create_version`, `draft_to_version`,
    `update_latest_release`, `publish`, `create_view`, `set_tags`,
    `delete_tags`, `set_wiki`, `delete_wiki`, `create_folder`,
    `delete_folder`, `delete_view`, `delete_table`) only records the intent —
    nothing reaches Dremio until `commit()`. Each returns
    `self`, so calls chain::

        session.data_copy(...).set_tags(...).commit()

    `idempotency_key`s are generated internally (`session-<id>-<n>`, one per
    step) — never pass or think about one; that's exactly the ceremony this
    class exists to hide (see the design doc's "must be fully encapsulated").
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._id = uuid.uuid4().hex[:8]
        self._steps: list[_Step] = []
        self._next_step = 1
        # Starts at the catalog root, not None — a data custodian's very
        # first relative path (no use() call yet) still has something to
        # resolve against, rather than raising "no context is set" before
        # they've ever had a chance to set one deliberately. use(None)/
        # use("") reset back here explicitly; only set_context(None) can
        # still clear it to no context at all.
        self._context: str | None = _ROOT_SOURCE

    def __repr__(self) -> str:
        return f"CatalogSession(pending={len(self._steps)})"

    def _key(self, suffix: str = "") -> str:
        key = f"session-{self._id}-{self._next_step}{suffix}"
        return key

    def set_context(self, path: str | None) -> CatalogSession:
        """Set the "current" catalog path — a later relative path (a leading
        `.`, e.g. `.water_temperature`) resolves against this. `None` clears
        it to no context at all (unlike `use(None)`/`use("")`, which reset to
        `_ROOT_SOURCE` instead — see `use`'s own docstring for why). `path`
        here is always taken literally, even if it itself starts with `.` —
        same as `use`, which differs only in that `None`/`""` handling, plus
        also making a live existence check (see `use`'s own docstring).

        `use` (via `%catalog use`/`%%catalog use(path)`) is the only
        custodian-facing way to set context — no queueing verb touches it
        as a side effect of running, deliberately, so context only ever
        changes when asked. `set_context` itself is not something a data
        custodian should normally call directly: an integration (the
        JupyterLab catalog-tree extension pushing a selected leaf through
        a Comm — see `docs/notebook-facade-for-data-scientists.md`,
        "Pre-filling catalog context") is its other caller, seeding it
        before any path has been typed yet.

        A path with ``/`` separators (``catalog/a/b``, as the catalog tree
        and DDS write them) is stored in the dot form (``catalog.a.b``) every
        other verb expects.
        """
        self._context = _dotted(path) if path else path
        return self

    def use(self, path: str | None) -> CatalogSession:
        """Set the current path for every call after this one — same as
        `set_context`, plus a live existence check (below) and different
        `None`/`""` handling: either one resets context to `_ROOT_SOURCE`
        (the catalog root — the same starting point a freshly built session
        already has), rather than clearing it to no context at all the way
        `set_context(None)` still does (see that method's own docstring for
        why the two diverge here). Unlike every other verb's
        `path`/`source_path`/`target_path`, `path` here is always taken
        literally as a whole, absolute path — never resolved against
        whatever context already exists, and never accepts a leading `.`
        or `../` fragment (pass `get_context()`'s own return value, or
        build the full path yourself, to move somewhere relative to where
        you already are).

        This is the only way to set context — no other verb touches it as
        a side effect of running (see `%%catalog use(path)` — the cell
        magic sets it once at the top of a cell, for every call in that
        cell and every cell after it, until changed again).

        Unlike everything else in this class, this makes a live call
        against Dremio: `path` must already exist as some catalog entity
        (folder, table, or view) — raises `CatalogSessionError` otherwise,
        so context never silently points somewhere real work would fail
        against later. `None`/`""` skip this check entirely — the catalog
        root always exists.
        """
        if path is None or path == "":
            self._context = _ROOT_SOURCE
            return self
        path = _dotted(path)
        try:
            found = self._catalog._catalog_rest.exists(path)  # noqa: SLF001 — see module docstring
        except (EngineStartingError, CatalogAuthError) as exc:
            raise CatalogSessionError(f"could not check whether {path!r} exists: {exc}") from exc
        if not found:
            raise CatalogSessionError(f"{path!r} does not exist in the catalog")
        self._context = path
        return self

    def get_context(self) -> str | None:
        """The current path — see `use`/`set_context`, the only two ways
        it changes after construction. Starts at `_ROOT_SOURCE` (the
        catalog root) for a freshly built session, never `None` — `None`
        only after an explicit `set_context(None)`. `use(None)`/`use("")`
        reset it back to `_ROOT_SOURCE` instead, rather than to `None`."""
        return self._context

    def _resolve_path(self, path: str) -> str:
        """Just the relative -> absolute resolution — never updates the
        context itself; only `use`/`set_context` do that. Always returns a
        full, already-resolved path (never a `.`/`..`-prefixed fragment).

        A leading `.` resolves against the current context
        (`".water_temperature"` -> `f"{context}.water_temperature"`); once a
        context exists, a bare path with no leading `.` at all resolves the
        same way too (`"water_temperature"` -> `f"{context}.water_temperature"`,
        same as `".water_temperature"`) — the dot is optional sugar there,
        not a marker that distinguishes relative from absolute. A bare path
        starting with `_ROOT_SOURCE` (e.g. `"catalog.other_root.table"`) is
        the one unambiguous exception: real absolute paths in this
        deployment always start there, so it's taken literally as absolute
        even with a context already set, rather than getting appended to
        it — no need to clear context first just to pass one alongside a
        relative path. Every verb resolves this way, including `data_copy`/
        `data_move`/`create_view`'s two paths (each resolved independently
        against the *same* current context) — since a genuinely absolute
        path in this deployment always starts with `_ROOT_SOURCE`, pairing
        a relative one with an unrelated absolute one in the same call is
        already unambiguous without needing a dot on the absolute side too.
        Otherwise, a bare path is only ever taken literally as absolute
        when no context has been set yet. `"."`/`"./"` alone (no name
        after either) resolve to the context itself, exactly as given by
        `get_context()` — not the context with a stray trailing `.`/`./`
        appended. One or more leading `../` segments (or a bare `..`)
        instead walk up that many levels of the context *first* —
        `"../stations"` is a sibling of the context, `"../../stations"` a
        level further up, and so on; `".."` alone (no name after it)
        resolves to the context's parent itself."""
        if not path.startswith(".") and "/" in path:
            path = _dotted(path)  # catalog/a/b — the slash form — means catalog.a.b
        if path == ".." or path.startswith("../"):
            return self._resolve_parent_path(path)
        if path in (".", "./"):
            if self._context is None:
                raise CatalogSessionError(
                    f"{path!r} means the current context, but no context is set yet — "
                    "use an absolute path first, or call set_context()"
                )
            return self._context
        if path.startswith("./"):
            path = f".{path[2:]}"  # "./name" means the same thing as ".name"
        if not path.startswith("."):
            is_root_absolute = path == _ROOT_SOURCE or path.startswith(f"{_ROOT_SOURCE}.")
            if self._context is None or is_root_absolute:
                return path
            path = f".{path}"
        if self._context is None:
            raise CatalogSessionError(
                f"{path!r} is relative (starts with '.') but no context is set yet — "
                "use an absolute path first, or call set_context()"
            )
        return f"{self._context}{path}"

    def _resolve_target(self, source_path: str, target_path: str) -> tuple[str, str | None]:
        """`data_copy`/`data_move`'s target: ``(the table path, the folder to
        create first or None)``.

        A `target_path` ending in ``/`` is a folder, resolved like any path
        (``"archive/"``, ``".archive/"``, ``"../archive/"``, ``"./"`` for the
        context itself): the table lands inside it under the source's own
        name (``"archive/"`` -> ``{context}.archive.<source name>``), and the
        folder is created when the step runs if it doesn't exist. Any other
        `target_path` is resolved as usual and returned with no folder."""
        if not isinstance(target_path, str) or not target_path.endswith("/"):
            return self._resolve_path(target_path), None
        stripped = target_path.rstrip("/")
        if not stripped:
            raise CatalogSessionError(
                f"target_path {target_path!r} names no folder — use './' for the current context"
            )
        folder = self._resolve_path(stripped)
        return f"{folder}.{source_path.rsplit('.', 1)[-1]}", folder

    def _resolve_parent_path(self, path: str) -> str:
        """The `..`/`../...` half of `_resolve_path` — walk `self._context`
        up one level per leading `../` (or the single `..`), then append
        whatever's left, if anything."""
        if self._context is None:
            raise CatalogSessionError(
                f"{path!r} is relative (starts with '..') but no context is set yet — "
                "use an absolute path first, or call set_context()"
            )
        ancestor = self._context
        remainder = path
        while remainder == ".." or remainder.startswith("../"):
            parent = operations._parent_path(ancestor)  # noqa: SLF001 — same-package internal
            if parent is None:
                raise CatalogSessionError(
                    f"{path!r} goes above the top of the current context {self._context!r}"
                )
            ancestor = parent
            remainder = remainder[3:] if remainder.startswith("../") else ""
        if not remainder:
            return ancestor
        if not remainder.startswith("."):
            remainder = f".{remainder}"
        return f"{ancestor}{remainder}"

    # -- queueing verbs ---------------------------------------------------

    def data_copy(
        self,
        source_path: str,
        target_path: str,
        *,
        overwrite: bool = False,
        create_target_folder: bool = False,
    ) -> CatalogSession:
        """Queue a `data_copy`. Reversible only when `overwrite=False` (there
        was nothing at `target_path` to lose) — undo drops the table this
        step created. With `overwrite=True`, whatever used to be at
        `target_path` is gone the moment this step runs; there is nothing to
        restore, so this step cannot be undone (see `CatalogCommitError`).

        A `target_path` ending in ``/`` names a folder: the copy lands inside
        it under the source's own name, and the folder (every missing level)
        is created first — see `_resolve_target`. Undo deletes the folders
        it created, too.

        Either path may be relative — with or without a leading `.` — to the session's
        current context — see `set_context`. Does not change the context itself — only
        `use` does that."""
        source_path = self._resolve_path(source_path)
        target_path, folder = self._resolve_target(source_path, target_path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            created = _create_folders(catalog, folder)
            try:
                catalog.datacopy(
                    source_path,
                    target_path,
                    overwrite=overwrite,
                    create_target_folder=create_target_folder,
                    idempotency_key=key,
                )
            except Exception:
                _delete_folders(catalog, created, idempotency_key=f"{key}-cleanup")
                raise
            if overwrite:
                raise _Irreversible("overwrote an existing target; its previous content is gone")

            def undo() -> None:
                _drop_entry(catalog, target_path, idempotency_key=f"{key}-undo")
                _delete_folders(catalog, created, idempotency_key=f"{key}-undo-folder")

            return undo

        self._steps.append(_Step(f"data_copy {source_path!r} -> {target_path!r}", run))
        return self

    def data_move(
        self,
        source_path: str,
        target_path: str,
        *,
        overwrite: bool = False,
        create_target_folder: bool = False,
    ) -> CatalogSession:
        """Queue a `data_move`, always as a TABLE — use `create_view` instead
        to create a view over `source_path` without touching it. Reversible
        only when `overwrite=False` — undo is a `data_move` back from
        `target_path` to `source_path`. Same `overwrite=True` limitation as
        `data_copy`. A `target_path` ending in ``/`` names a folder, created
        first if missing, as in `data_copy`; undo deletes it again.

        Either path may be relative — with or without a leading `.` — to the session's
        current context — see `set_context`. Does not change the context itself — only
        `use` does that."""
        source_path = self._resolve_path(source_path)
        target_path, folder = self._resolve_target(source_path, target_path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            created = _create_folders(catalog, folder)
            try:
                catalog.datamove(
                    source_path,
                    target_path,
                    entry_type="TABLE",
                    overwrite=overwrite,
                    create_target_folder=create_target_folder,
                    idempotency_key=key,
                )
            except Exception:
                _delete_folders(catalog, created, idempotency_key=f"{key}-cleanup")
                raise
            if overwrite:
                raise _Irreversible("overwrote an existing target; its previous content is gone")

            def undo() -> None:
                catalog.datamove(
                    target_path,
                    source_path,
                    entry_type="TABLE",
                    overwrite=False,
                    idempotency_key=f"{key}-undo",
                )
                _delete_folders(catalog, created, idempotency_key=f"{key}-undo-folder")

            return undo

        self._steps.append(_Step(f"data_move {source_path!r} -> {target_path!r}", run))
        return self

    def create_version(
        self,
        source_path: str,
        version_name: str,
        tables: list[str] | None = None,
        target_path: str | None = None,
        *,
        overwrite: bool = False,
        create_target_folder: bool = True,
    ) -> CatalogSession:
        """Queue a `createversion`: copy the listed `tables` from `source_path`
        into a new folder `{target_path}.{version_name}`.

        `source_path` and `version_name` must not be blank (empty or only
        whitespace). `tables` omitted or empty copies everything under
        `source_path`. Each entry in `tables` is a table, or a folder standing for every
        table under it (``"bw_assessment.assessments"``, ``"bw_assessment"``),
        named as listed under `source_path` or as the full paths `list()`
        returns; each table keeps its sub-folders inside the version. Reference and
        vocabulary tables (a folder named ``reference``/``references``/
        ``vocabulary``/``vocabularies`` in the path) and views are skipped —
        the commit report's `notes` say which, and why.

        `source_path` and `target_path` may be relative — with or without a
        leading `.` — to the session's current context. `target_path`
        omitted means the current context itself, so ``use(...)`` picks where
        versions go; with no context set either, this raises. A `target_path`
        that doesn't exist yet is created (every missing level) unless
        ``create_target_folder=False``.

        An existing version folder is an error unless ``overwrite=True``,
        which replaces the listed tables in it. Undo deletes the target levels
        and version folder this step created, or drops the tables it newly
        copied into an existing one; a table it overwrote cannot be restored, so a step that
        replaced any is irreversible (see `CatalogCommitError`)."""
        return self._queue_version(
            source_path,
            version_name,
            tables,
            target_path,
            overwrite=overwrite,
            create_target_folder=create_target_folder,
            include_views=False,
        )

    def _queue_version(
        self,
        source_path: str,
        version_name: str,
        tables: list[str] | None,
        target_path: str | None,
        *,
        overwrite: bool,
        create_target_folder: bool,
        include_views: bool,
    ) -> CatalogSession:
        """`create_version`, plus `include_views` (see `createversion`) —
        `draft_to_version` copies the draft's views as tables too."""
        try:
            source_path, version_name, tables = operations._check_version_args(  # noqa: SLF001
                source_path, version_name, tables
            )
        except CatalogOperationError as exc:
            raise CatalogSessionError(str(exc)) from None
        source_path = self._resolve_path(source_path)
        if target_path is None:
            if self._context is None:
                raise CatalogSessionError(
                    "target_path was omitted but no context is set yet — pass target_path, "
                    "or call use() first to choose where the version goes"
                )
            target_path = self._context
        else:
            target_path = self._resolve_path(target_path)
        step = _Step(
            f"create version '{target_path}.{version_name}' from {source_path!r} "
            + (f"({len(tables)} table(s) listed)" if tables else "(all tables)"),
            lambda catalog, key: None,  # replaced below, once `step` exists to report into
        )

        def run(catalog: Catalog, key: str) -> _Undo | None:
            result = catalog.createversion(
                source_path,
                version_name,
                tables,
                target_path=target_path,
                overwrite=overwrite,
                create_target_folder=create_target_folder,
                include_views=include_views,
                idempotency_key=key,
            )
            step.notes.extend(
                f"copied view {view!r} as a table" for view in result.from_views
            )
            step.notes.extend(
                f"skipped {table!r}: {reason}" for table, reason in result.skipped
            )
            step.notes.extend(f"replaced {table!r}" for table in result.replaced)
            if result.replaced:
                raise _Irreversible(
                    f"overwrote {len(result.replaced)} existing table(s) in the version"
                )
            new_tables = [target for _, target in result.copied]

            created_top = (
                result.created_target_folders[0]
                if result.created_target_folders
                else (result.version_path if result.created_folder else None)
            )

            def undo() -> None:
                if created_top is not None:
                    catalog.deletefolder(created_top, cascade=True, idempotency_key=f"{key}-undo")
                    return
                for number, table in enumerate(new_tables, start=1):
                    _drop_entry(catalog, table, idempotency_key=f"{key}-undo-{number}")

            return undo

        step.run = run
        self._steps.append(step)
        return self

    def draft_to_version(
        self,
        version_name: str,
        tables: list[str] | None = None,
        *,
        overwrite: bool = False,
    ) -> CatalogSession:
        """Queue a `create_version` from the dataflow's ``draft`` folder: copy
        `tables` from it into ``{draft's parent}.versions.{version_name}``.

        Neither path is passed — both come from the current context:

        - the context is the ``draft`` folder, or anywhere inside it (the
          nearest ``draft`` segment of the context is used);
        - or the context is the folder that holds ``draft`` (e.g. the
          dataflow, ``…bwd``).

        Otherwise this raises, saying where it looked. The ``versions``
        folder is created if it doesn't exist yet. `tables` behaves exactly
        as in `create_version`: omitted or empty copies everything in
        ``draft``; an entry is a table or a folder (every table under it),
        relative to ``draft`` or a full path; reference/vocabulary tables are
        skipped and reported. Unlike `create_version`, views are not skipped:
        each view in ``draft`` is copied into the version as a table (a
        snapshot of what it returns), and the commit report's `notes` say
        which. An existing version is an error unless ``overwrite=True``.
        Rollback is `create_version`'s."""
        draft = self._find_draft()
        parent = operations._parent_path(draft)  # noqa: SLF001 — same-package internal
        if parent is None:
            raise CatalogSessionError(f"{draft!r} has no parent folder to put versions under")
        return self._queue_version(
            draft,
            version_name,
            tables,
            f"{parent}.{VERSIONS_FOLDER}",
            overwrite=overwrite,
            create_target_folder=True,
            include_views=True,
        )

    def _find_draft(self) -> str:
        """The ``draft`` folder the current context points at — see
        `draft_to_version`. Checks the catalog only when the context is the
        folder holding ``draft``; a ``draft`` segment in the context itself
        is taken as given."""
        if self._context is None:
            raise CatalogSessionError(
                "no context is set — use() the draft folder, or the folder that holds it, first"
            )
        segments = self._context.split(".")
        if DRAFT_FOLDER in segments:
            nearest = len(segments) - 1 - segments[::-1].index(DRAFT_FOLDER)
            return ".".join(segments[: nearest + 1])
        candidate = f"{self._context}.{DRAFT_FOLDER}"
        try:
            found = self._catalog._catalog_rest.exists(candidate)  # noqa: SLF001
        except (EngineStartingError, CatalogAuthError) as exc:
            raise CatalogSessionError(
                f"could not check whether {candidate!r} exists: {exc}"
            ) from exc
        if found:
            return candidate
        raise CatalogSessionError(
            f"no {DRAFT_FOLDER!r} folder for the context {self._context!r}: it isn't inside one, "
            f"and {candidate!r} doesn't exist — use() the draft folder, or the folder that "
            "holds it, first"
        )

    def update_latest_release(
        self, version_name: str, *, overwrite: bool = True
    ) -> CatalogSession:
        """Queue a view at the Area's root for every table in
        ``{context}.versions.{version_name}``, keeping the version's
        sub-folders: ``versions.v1.eu_sdg_06_30.monitoringsites`` gets the
        view ``{context}.eu_sdg_06_30.monitoringsites`` over it. Missing
        sub-folders are created.

        The context must be an Area folder — a folder with both a ``draft``
        and a ``versions`` folder just below it, typically
        ``catalog.<domain>.<subdomain>.<area>`` — set it with `use` first.
        Checked now, against Dremio, before anything is queued: that the
        context has those two folders just below it, and that
        ``{context}.versions.{version_name}`` exists; `CatalogSessionError`
        otherwise.

        The version's tables are listed when the step runs. Views in the
        version are skipped, and the commit report's `notes` say which. A
        table whose first sub-folder is ``draft`` or ``versions`` is refused,
        since its view would land inside the Area's own draft or versions.
        Before any view is created, every target is checked: an existing view
        is dropped and created again over the version (``overwrite=True``, the
        default — re-pointing the views at a new version is the normal use);
        ``overwrite=False`` makes it an error instead. An existing table is
        always an error — a view never replaces one.

        Each view gets the `PUBLIC_TAG` (``#public``) tag, keeping any others,
        and the tag is removed from every table and view under the Area's
        ``versions`` folder — so only the latest release is public. `publish`
        then copies the tagged views out.

        Undo drops the views this step created and the folders it created
        for them, and puts back the tags it removed under ``versions``. A
        view it replaced cannot be restored, so a step that replaced any is
        irreversible (see `CatalogCommitError`)."""
        if not isinstance(version_name, str) or not version_name.strip():
            raise CatalogSessionError("version_name must not be empty")
        version_name = version_name.strip()
        if "." in version_name or "/" in version_name:
            raise CatalogSessionError(
                f"version_name must be a single folder name, without '.' or '/': {version_name!r}"
            )
        dataflow = self._require_area()
        versions = f"{dataflow}.{VERSIONS_FOLDER}"
        version = f"{versions}.{version_name}"
        if not self._is_folder(version):
            raise CatalogSessionError(f"{version!r} does not exist, or is not a folder")
        step = _Step(
            f"create views under {dataflow!r} for the tables in {version!r}",
            lambda catalog, key: None,  # replaced below, once `step` exists to report into
        )

        def run(catalog: Catalog, key: str) -> _Undo | None:
            plan = _plan_version_views(catalog, dataflow, version, overwrite, key, step.notes)
            new_views: list[str] = []
            new_folders: list[str] = []
            untagged: list[tuple[str, list[str]]] = []

            def undo() -> None:
                for number, (path, tags) in enumerate(untagged, start=1):
                    catalog.settagsto(path, tags, idempotency_key=f"{key}-undo-tag-{number}")
                for number, view in enumerate(new_views, start=1):
                    _drop_entry(catalog, view, idempotency_key=f"{key}-undo-{number}")
                for folder in reversed(new_folders):
                    catalog.deletefolder(folder, idempotency_key=f"{key}-undo-folder")

            try:
                for number, (source, target, existed) in enumerate(plan, start=1):
                    parent = operations._parent_path(target)  # noqa: SLF001
                    if parent is not None and parent != dataflow:
                        new_folders.extend(catalog._catalog_rest.ensure_folder_path(parent))  # noqa: SLF001
                    catalog.createview(
                        source, target, overwrite=existed, idempotency_key=f"{key}-{number}"
                    )
                    if existed:
                        step.notes.append(f"replaced view {target!r}")
                    else:
                        new_views.append(target)
                for number, (_, target, _) in enumerate(plan, start=1):
                    _add_tag(catalog, target, PUBLIC_TAG, idempotency_key=f"{key}-tag-{number}")
                _remove_tag_under(catalog, versions, PUBLIC_TAG, untagged, key=f"{key}-untag")
            except Exception as exc:
                try:
                    undo()
                except Exception as cleanup_error:  # noqa: BLE001 — the original failure is raised
                    exc.add_note(f"cleanup failed, check {dataflow!r} by hand: {cleanup_error}")
                raise
            if untagged:
                step.notes.append(
                    f"removed {PUBLIC_TAG!r} from {len(untagged)} table(s)/view(s) "
                    f"under {versions!r}"
                )
            replaced = sum(existed for _, _, existed in plan)
            if replaced:
                raise _Irreversible(f"replaced {replaced} existing view(s)")
            return undo

        step.run = run
        self._steps.append(step)
        return self

    def publish(
        self,
        path: str | None = None,
        *,
        overwrite: bool = True,
        partition_by: PartitionBy | None = None,
    ) -> CatalogSession:
        """Queue a copy of every released view of a dataflow into the public
        bucket: each view under the dataflow folder tagged `PUBLIC_TAG` (``#public``,
        as `update_latest_release` leaves them) becomes a table at
        ``PUBLIC_ROOT.<the view's full catalog path>`` —
        ``catalog.<domain>.<subdomain>.<dataflow>.eu_sdg_06_30.monitoringsites``
        is copied to ``public_s3."dh-prod-data-public".catalog.<domain>.
        <subdomain>.<dataflow>.eu_sdg_06_30.monitoringsites``.

        The dataflow folder is `path` when given — resolved like any path,
        absolute or relative to the context — else the context `use` set; with
        neither (no `use` yet, the context still the catalog root),
        `CatalogSessionError`. It must be a dataflow folder: one with
        ``draft``, ``versions`` and ``reference`` (or ``references``) folders
        just below it — checked now, `CatalogSessionError` otherwise. When the
        step runs, it is listed: everything inside its ``draft``,
        ``versions`` and ``reference``/``references`` folders, and every
        table, is ignored; views without the
        tag are skipped (the commit report's `notes` say which), and no
        tagged view at all is an error. Every target is checked before
        anything is copied: an existing one is replaced (``overwrite=True``,
        the default — republishing after a new release is the normal use);
        ``overwrite=False`` makes it an error instead.

        Each copy is ``DROP TABLE IF EXISTS <target>`` — always, since a
        bucket folder Dremio has not promoted is not seen by the check above
        — then ``CREATE TABLE <target> STORE AS (type => 'parquet') AS SELECT
        * FROM <view>``, so the bucket holds plain Parquet files, then
        ``ALTER TABLE <target> REFRESH METADATA AUTO PROMOTION``, and finally a
        ``SELECT COUNT(*)`` on it: a table that can't be read is a failure.
        The notes give each row count.

        All or nothing: before anything is replaced, every existing target is
        copied to ``PUBLIC_ROOT.<dataflow path>__publish_backup.<same path>`` (and read
        back). If any table then fails, every table this publish touched is
        dropped, the replaced ones are copied back from their backups, and a
        `CatalogOperationError` says which table failed and that everything
        was rolled back. The backups are dropped once the publish succeeds or
        is rolled back; one that could not be restored is kept, and the error
        names it. A restored table is not partitioned, whatever it was before.

        `partition_by` (optional) partitions the Parquet tables by existing
        fields — ``PARTITION BY ("f1", "f2")`` in the ``CREATE TABLE``, in the
        order given (a set is sorted, having no order of its own). A list
        applies to every published view, so each must have those fields; a
        dict gives each view its own, keyed by its path below the Area
        (``{"eu_sdg.monitoringsites": ["year"]}``), and views it doesn't name
        are not partitioned. Names are matched ignoring case. The fields are
        checked against the views now, when `publish` is called: a missing
        field, or a dict key that names no published view, raises
        `CatalogSessionError` and nothing is queued. They are checked again
        when the step runs, before anything is dropped or copied.

        Undo (a later step of the same commit failing) drops the tables this
        step created. The backups are gone by then, so a step that replaced
        any table is irreversible (see `CatalogCommitError`)."""
        partitions = _partition_plan(partition_by)
        area = self._require_publish_dataflow(path)
        target_root = f"{PUBLIC_ROOT}.{area}"
        if partitions:
            # Fields are checked now, so a typo fails here rather than at commit.
            try:
                _plan_publish(
                    self._catalog, area, target_root, True, partitions, self._key("-fields"), []
                )
            except (CatalogOperationError, EngineStartingError, CatalogAuthError) as exc:
                raise CatalogSessionError(str(exc)) from exc
        step = _Step(
            f"publish the {PUBLIC_TAG} views of {area!r} to {target_root!r}",
            lambda catalog, key: None,  # replaced below, once `step` exists to report into
        )

        def run(catalog: Catalog, key: str) -> _Undo | None:
            plan = _plan_publish(
                catalog, area, target_root, overwrite, partitions, key, step.notes
            )
            publication = _Publication(
                catalog._flight_executor,  # noqa: SLF001 — Flight, as Catalog.datacopy
                f"{target_root}{_BACKUP_SUFFIX}",
                key,
            )
            publication.run(plan, step.notes)
            replaced = sum(existed for _, _, existed, _ in plan)
            if replaced:
                raise _Irreversible(f"replaced {replaced} published table(s)")
            return publication.drop_published

        step.run = run
        self._steps.append(step)
        return self

    def _require_publish_dataflow(self, path: str | None) -> str:
        """`publish`'s dataflow folder: `path` resolved, or the context `use`
        set — checked against Dremio to have draft, versions and reference
        folders just below it."""
        if path is not None:
            if not isinstance(path, str) or not path.strip(" ./"):
                raise CatalogSessionError(f"path must be a catalog folder path, not {path!r}")
            dataflow = self._resolve_path(path.strip())
        elif self._context is None or self._context == _ROOT_SOURCE:
            raise CatalogSessionError(
                "no dataflow folder: run %catalog use(...) on the dataflow folder (the one with "
                "draft, versions and reference folders) first, or pass path=, e.g. "
                'publish(path="catalog.<domain>.<subdomain>.<dataflow>")'
            )
        else:
            dataflow = self._context
        missing = [
            name
            for name in (DRAFT_FOLDER, VERSIONS_FOLDER)
            if not self._is_folder(f"{dataflow}.{name}")
        ]
        if not any(self._is_folder(f"{dataflow}.{name}") for name in _REFERENCE_FOLDERS):
            missing.append(REFERENCE_FOLDER)
        if missing:
            raise CatalogSessionError(
                f"{dataflow!r} is not a dataflow folder: it has no "
                + " and no ".join(repr(name) for name in missing)
                + " folder just below it — use() the dataflow folder, or pass path="
            )
        return dataflow

    def _require_area(self) -> str:
        """The context, checked against Dremio to be an Area folder: one with
        both a ``draft`` and a ``versions`` folder just below it."""
        area = self._require_dataflow_context()
        missing = [
            name
            for name in (DRAFT_FOLDER, VERSIONS_FOLDER)
            if not self._is_folder(f"{area}.{name}")
        ]
        if missing:
            raise CatalogSessionError(
                f"{area!r} is not an Area folder: it has no "
                + " and no ".join(repr(name) for name in missing)
                + " folder just below it — use() the Area folder first"
            )
        return area

    def _is_folder(self, path: str) -> bool:
        """Whether `path` is a folder, checked against Dremio now."""
        try:
            return self._catalog._catalog_rest.is_folder(path)  # noqa: SLF001
        except (EngineStartingError, CatalogAuthError) as exc:
            raise CatalogSessionError(f"could not check whether {path!r} exists: {exc}") from exc

    def _require_dataflow_context(self) -> str:
        """The context — whether it is an Area folder is checked by `_require_area`."""
        if self._context is None:
            raise CatalogSessionError(
                "no context is set — use() the Area folder "
                f"({_ROOT_SOURCE}.<domain>.<subdomain>.<area>) first"
            )
        return self._context

    def create_view(
        self,
        source_path: str,
        target_path: str,
        *,
        overwrite: bool = False,
        create_target_folder: bool = False,
    ) -> CatalogSession:
        """Queue a `createview` — creates a VIEW at `target_path` over
        `source_path`, leaving `source_path` itself untouched (unlike
        `data_move`). Reversible only when `overwrite=False` (there was nothing
        at `target_path` to lose) — undo drops the view this step created.
        With `overwrite=True`, whatever used to be at `target_path` is gone
        the moment this step runs; there is nothing to restore, so this
        step cannot be undone (see `CatalogCommitError`).

        Either path may be relative — with or without a leading `.` — to the session's
        current context — see `set_context`. Does not change the context itself — only
        `use` does that."""
        source_path = self._resolve_path(source_path)
        target_path = self._resolve_path(target_path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            catalog.createview(
                source_path,
                target_path,
                overwrite=overwrite,
                create_target_folder=create_target_folder,
                idempotency_key=key,
            )
            if overwrite:
                raise _Irreversible("overwrote an existing target; its previous content is gone")

            def undo() -> None:
                _drop_entry(catalog, target_path, idempotency_key=f"{key}-undo")

            return undo

        self._steps.append(_Step(f"create view {source_path!r} -> {target_path!r}", run))
        return self

    def set_tags(self, path: str, tags: list[str]) -> CatalogSession:
        """Queue `settagsto` (replaces the tag set). Undo restores whatever
        tags were on `path` immediately before this step ran.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`. Does not change the context itself — only `use` does that."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo:
            existing = catalog.gettagsfrom(path, idempotency_key=f"{key}-read")
            catalog.settagsto(path, tags, idempotency_key=key)

            def undo() -> None:
                catalog.settagsto(path, existing, idempotency_key=f"{key}-undo")

            return undo

        self._steps.append(_Step(f"set tags {tags!r} on {path!r}", run))
        return self

    def delete_tags(self, path: str, tags: list[str]) -> CatalogSession:
        """Queue `deletetags`. Undo restores the full tag set `path` had
        immediately before this step ran.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`. Does not change the context itself — only `use` does that."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo:
            existing = catalog.gettagsfrom(path, idempotency_key=f"{key}-read")
            catalog.deletetags(path, tags, idempotency_key=key)

            def undo() -> None:
                catalog.settagsto(path, existing, idempotency_key=f"{key}-undo")

            return undo

        self._steps.append(_Step(f"delete tags {tags!r} from {path!r}", run))
        return self

    def set_wiki(
        self, path: str, text: str, *, tags: list[dict[str, str]] | None = None
    ) -> CatalogSession:
        """Queue `setwikito`. Undo restores the previous wiki text verbatim,
        or deletes it if `path` had none. `tags` (a list of `{tag_name,
        tag_value, tag_title}` dicts, rendered into a "# Meta Data" section
        of the wiki text) is ignored outright when `path` is a table or
        view — those already have Dremio's own native tags/labels for
        this (`set_tags`/`get_tags`); the wiki Meta Data convention exists
        only because a folder has no such native concept.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`. Does not change the context itself — only `use` does that."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo:
            existing = _read_wiki_or_none(catalog, path, idempotency_key=f"{key}-read")
            catalog.setwikito(path, text, tags=tags, idempotency_key=key)

            def undo() -> None:
                undo_key = f"{key}-undo"
                if existing is None:
                    catalog.deletewiki(path, idempotency_key=undo_key)
                else:
                    catalog.setwikito(path, existing, idempotency_key=undo_key)

            return undo

        self._steps.append(_Step(f"set wiki on {path!r}", run))
        return self

    def delete_wiki(self, path: str) -> CatalogSession:
        """Queue `deletewiki`. Undo restores the previous wiki text, if there
        was one — a no-op if `path` had no wiki to begin with.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`. Does not change the context itself — only `use` does that."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            existing = _read_wiki_or_none(catalog, path, idempotency_key=f"{key}-read")
            catalog.deletewiki(path, idempotency_key=key)
            if existing is None:
                return None

            def undo() -> None:
                catalog.setwikito(path, existing, idempotency_key=f"{key}-undo")

            return undo

        self._steps.append(_Step(f"delete wiki on {path!r}", run))
        return self

    def create_folder(self, path: str, *, create_parents: bool = False) -> CatalogSession:
        """Queue `createfolder`. Undo deletes it — but only if this step
        actually created it; `createfolder` is idempotent, so a `path`
        that already existed is left alone on rollback too.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`. Does not change the context itself — only
        `use` does that."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            already_there = catalog._catalog_rest.exists(path)  # noqa: SLF001 — see module docstring
            catalog.createfolder(path, create_parents=create_parents, idempotency_key=key)
            if already_there:
                return None

            def undo() -> None:
                catalog.deletefolder(path, cascade=False, idempotency_key=f"{key}-undo")

            return undo

        self._steps.append(_Step(f"create folder {path!r}", run))
        return self

    def delete_folder(self, path: str, *, cascade: bool = False) -> CatalogSession:
        """Queue `deletefolder`. **Never reversible** — a deleted folder's
        contents cannot be recreated, so this always leaves the commit
        unable to claim a clean rollback if a later step fails.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`. Does not change the context itself —
        there's nothing meaningful to navigate into once it's deleted."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            catalog.deletefolder(path, cascade=cascade, idempotency_key=key)
            raise _Irreversible("deleted folder contents cannot be recreated")

        self._steps.append(_Step(f"delete folder {path!r}", run))
        return self

    def delete_view(self, path: str) -> CatalogSession:
        """Queue `deleteview`. **Never reversible** — the dropped view's
        definition isn't captured anywhere, so there's nothing to recreate
        it from. Unlike `Catalog.deleteview`'s own `DROP VIEW IF EXISTS`
        (idempotent — a no-op on a missing path), this raises
        `CatalogOperationError` if `path` doesn't exist at all, so a typo
        fails clearly instead of silently doing nothing.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`. Does not change the context itself —
        there's nothing meaningful to navigate into once it's deleted."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            if not catalog._catalog_rest.exists(path):  # noqa: SLF001 — see module docstring
                raise CatalogOperationError(f"{path!r} does not exist")
            catalog.deleteview(path, idempotency_key=key)
            raise _Irreversible("dropped view's definition cannot be recreated")

        self._steps.append(_Step(f"delete view {path!r}", run))
        return self

    def delete_table(self, path: str) -> CatalogSession:
        """Queue `deletetable`. **Never reversible** — the dropped table's
        data isn't captured anywhere, so there's nothing to recreate it
        from. Unlike `Catalog.deletetable`'s own `DROP TABLE IF EXISTS`
        (idempotent — a no-op on a missing path), this raises
        `CatalogOperationError` if `path` doesn't exist at all, so a typo
        fails clearly instead of silently doing nothing.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`. Does not change the context itself —
        there's nothing meaningful to navigate into once it's deleted."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            if not catalog._catalog_rest.exists(path):  # noqa: SLF001 — see module docstring
                raise CatalogOperationError(f"{path!r} does not exist")
            catalog.deletetable(path, idempotency_key=key)
            raise _Irreversible("dropped table's data cannot be recreated")

        self._steps.append(_Step(f"delete table {path!r}", run))
        return self

    # -- read-only queries — answered immediately, never queued -------------

    def get_wiki(self, path: str) -> str:
        """The wiki text at `path` (see `getwikifrom`). Answered immediately
        — not queued, since there's nothing to commit or undo. Raises
        `CatalogOperationError` if `path` doesn't exist, or exists but has
        no wiki at all.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`."""
        path = self._resolve_path(path)
        return self._catalog.getwikifrom(path, idempotency_key=self._key())

    def get_tags(self, path: str) -> list[str]:
        """The tags on `path` (see `gettagsfrom`) — tables/views only.
        Answered immediately — not queued, since there's nothing to commit
        or undo. Raises `CatalogOperationError` if `path` doesn't exist or
        isn't a table/view (folders have Dremio's own wiki Meta Data
        section for this instead — see `Catalog.setmeta2wiki`).

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`."""
        path = self._resolve_path(path)
        return self._catalog.gettagsfrom(path, idempotency_key=self._key())

    def list(self, path: str = "") -> CatalogListing:
        """Full paths of every table and view under `path`, each shown with
        whether it's a TABLE or a VIEW (see `CatalogListing`): its own tables,
        then every child folder's, at any depth. Found by walking the catalog
        tree folder by folder through Dremio's REST catalog API (see
        `CatalogRestClient.list_datasets`), so it lists what the catalog
        holds even where a source doesn't report its tables in
        ``INFORMATION_SCHEMA``. Answered immediately — not queued, since
        there's nothing to commit or undo. Raises `CatalogOperationError`
        if `path` doesn't exist, rather than returning `[]` for a typo.

        `path` may be a whole, absolute path, or relative — with or
        without a leading `.` — to the session's current context (see
        `set_context`). Omitted (or `""`), it lists the content of the
        current context itself — raises `CatalogSessionError` if no
        context is set yet."""
        if not path:
            if self._context is None:
                raise CatalogSessionError(
                    "path was omitted but no context is set yet — pass a path, or call use() first"
                )
            path = self._context
        else:
            path = self._resolve_path(path)
        return CatalogListing(
            self._catalog._catalog_rest.list_dataset_kinds(path.split("."))  # noqa: SLF001
        )

    def schema(self, path: str) -> TableInfo:
        """The column schema and row count of `path` (see
        `gettableitemsfrom`) — never fetches the actual rows. Answered
        immediately — not queued, since there's nothing to commit or undo.
        `path` must be a table or view — raises `CatalogOperationError`
        otherwise (a folder has no schema of its own), or if `path` doesn't
        exist at all.

        `path` may be relative — with or without a leading `.` — to the session's current
        context — see `set_context`."""
        path = self._resolve_path(path)
        operations._require_table_or_view(  # noqa: SLF001 — see module docstring
            self._catalog._catalog_rest,  # noqa: SLF001
            path,
            "schema",
        )
        return self._catalog.gettableitemsfrom(path, idempotency_key=self._key())

    # -- commit -------------------------------------------------------------

    def commit(
        self, *, retry: bool = False, max_retries: int = 3, retry_delay: float = 5.0
    ) -> CommitReport:
        """Run every queued step, in order. All-or-nothing: the moment one
        fails, everything this commit already did is undone (reverse order)
        before `CatalogCommitError` is raised — see that class and each
        verb's docstring for what "undone" can and can't cover.

        `retry=True` re-attempts a step that raised `EngineStartingError`
        (a Dremio engine still warming up) up to `max_retries` times,
        `retry_delay` seconds apart, before giving up and rolling back —
        every operation here is documented safe to retry from the start
        with the same idempotency key, so this just re-runs the same step.
        Any other exception rolls back immediately regardless of `retry`;
        retrying a real error (bad path, permission denied, ...) would not
        help.

        The queue is cleared either way — a failed `commit()` does not
        leave the failing step (or the ones after it) still queued.
        """
        executed: list[tuple[_Step, _Undo | None]] = []
        irreversible: list[str] = []
        steps, self._steps = self._steps, []
        current: _Step | None = None
        try:
            for current in steps:
                key = self._key()
                self._next_step += 1
                attempt = 0
                while True:
                    try:
                        try:
                            undo = current.run(self._catalog, key)
                        except _Irreversible as marker:
                            undo = None
                            irreversible.append(f"{current.description} ({marker})")
                        executed.append((current, undo))
                        break
                    except EngineStartingError:
                        attempt += 1
                        if retry and attempt <= max_retries:
                            time.sleep(retry_delay)
                            continue
                        raise
        except Exception as exc:  # noqa: BLE001 — reported via CatalogCommitError below
            rollback_problems = self._rollback(executed)
            unresolved = irreversible + rollback_problems
            assert current is not None
            raise CatalogCommitError(
                f"commit failed at step {len(executed) + 1}/{len(steps)} "
                f"({current.description}): {exc}. "
                + (
                    "Everything before it was rolled back."
                    if not unresolved
                    else f"ROLLBACK INCOMPLETE — see .unresolved: {unresolved}"
                ),
                failed_step=current.description,
                original_error=exc,
                rolled_back=not unresolved,
                unresolved=unresolved,
            ) from exc
        return CommitReport(
            succeeded=[step.description for step, _ in executed],
            notes=[note for step, _ in executed for note in step.notes],
        )

    def _rollback(self, executed: list[tuple[_Step, _Undo | None]]) -> list[str]:
        problems: list[str] = []
        for step, undo in reversed(executed):
            if undo is None:
                continue
            try:
                undo()
            except Exception as exc:  # noqa: BLE001 — best-effort: collect and keep going
                problems.append(f"{step.description}: rollback failed ({exc})")
        return problems
