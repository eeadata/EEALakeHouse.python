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

import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from . import operations
from .client import Catalog
from .errors import CatalogOperationError, EngineStartingError
from .operations import TableInfo

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

_ROOT_SOURCE = "catalog"
# This deployment's one real top-level source/space (see e.g. the "catalog." prefix
# on every real path in debugger/test_catalog_magic.ipynb's TEST_ROOT) — a bare path
# starting with it is therefore unambiguously absolute, context or not; see
# `_resolve_path`.


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


class CatalogSession:
    """Queue catalog verbs against `catalog`, then `commit()` them as one batch.

    Every queueing method (`data_copy`, `data_move`, `create_version`, `draft_to_version`,
    `create_view`, `set_tags`,
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
        """
        self._context = path
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
        try:
            found = self._catalog._catalog_rest.exists(path)  # noqa: SLF001 — see module docstring
        except EngineStartingError as exc:
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

        Either path may be relative — with or without a leading `.` — to the session's
        current context — see `set_context`. Does not change the context itself — only
        `use` does that."""
        source_path = self._resolve_path(source_path)
        target_path = self._resolve_path(target_path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            catalog.datacopy(
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
        `data_copy`.

        Either path may be relative — with or without a leading `.` — to the session's
        current context — see `set_context`. Does not change the context itself — only
        `use` does that."""
        source_path = self._resolve_path(source_path)
        target_path = self._resolve_path(target_path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            catalog.datamove(
                source_path,
                target_path,
                entry_type="TABLE",
                overwrite=overwrite,
                create_target_folder=create_target_folder,
                idempotency_key=key,
            )
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
                idempotency_key=key,
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
        relative to ``draft`` or a full path; reference/vocabulary tables and
        views are skipped and reported. An existing version is an error
        unless ``overwrite=True``. Rollback is `create_version`'s."""
        draft = self._find_draft()
        parent = operations._parent_path(draft)  # noqa: SLF001 — same-package internal
        if parent is None:
            raise CatalogSessionError(f"{draft!r} has no parent folder to put versions under")
        return self.create_version(
            draft,
            version_name,
            tables,
            f"{parent}.{VERSIONS_FOLDER}",
            overwrite=overwrite,
            create_target_folder=True,
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
        if self._catalog._catalog_rest.exists(candidate):  # noqa: SLF001 — see module docstring
            return candidate
        raise CatalogSessionError(
            f"no {DRAFT_FOLDER!r} folder for the context {self._context!r}: it isn't inside one, "
            f"and {candidate!r} doesn't exist — use() the draft folder, or the folder that "
            "holds it, first"
        )

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

    def list(self, path: str = "") -> list[str]:
        """Full paths of every table and view under `path`, at any depth
        (see `gettablesfrom`). Answered immediately — not queued, since
        there's nothing to commit or undo. Raises `CatalogOperationError`
        if `path` doesn't exist — `gettablesfrom` itself doesn't check this
        (an empty result and "nothing there" look the same to it), so this
        checks first rather than returning `[]` for a typo'd path.

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
        if not self._catalog._catalog_rest.exists(path):  # noqa: SLF001 — see module docstring
            raise CatalogOperationError(f"{path!r} does not exist")
        return self._catalog.gettablesfrom(path, idempotency_key=self._key())

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
