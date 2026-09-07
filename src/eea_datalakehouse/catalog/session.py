"""`CatalogSession` — queue catalog verbs, then commit them as one batch.

Prototype of the design in `docs/notebook-facade-for-data-scientists.md`
("Queue calls, then commit"). Wraps an existing `Catalog`, so nothing here
reimplements catalog behaviour — it only hides the ceremony (idempotency
keys) and adds queue/commit/rollback on top::

    session = CatalogSession(catalog)
    session.copy("draft.raw_2026", "bwd.reference.water_temperature")
    session.tag("bwd.reference.water_temperature", ["reviewed"])
    session.commit(retry=True)

`commit()` is all-or-nothing: every queued step runs in order, and the
moment one fails, everything already done in *this* commit is undone before
the failure is reported — see each verb's own docstring below for exactly
what its compensating action is, and where one isn't possible (`delete_folder`,
or `copy`/`move` with `overwrite=True`) that limitation is raised as part of
the failure, not hidden.

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
from typing import Literal

from . import operations
from .client import Catalog
from .errors import CatalogOperationError, EngineStartingError

_Undo = Callable[[], None]


class CatalogSessionError(RuntimeError):
    """Base for `CatalogSession`-specific errors."""


class CatalogCommitError(CatalogSessionError):
    """`commit()` failed partway through the batch.

    `rolled_back` is `True` only if every already-succeeded step in this
    commit was cleanly undone. When it's `False`, `unresolved` names which
    steps are NOT undone — either because that verb has no compensating
    action at all (`delete_folder`; `copy`/`move` with `overwrite=True`), or
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
    """What a successful `commit()` actually did, in order."""

    succeeded: list[str] = field(default_factory=list)


class _Irreversible(Exception):  # noqa: N818 — internal control-flow signal, not a user-facing error
    """Raised by a step's `run` to say: this succeeded, but cannot be undone."""


@dataclass
class _Step:
    description: str
    run: Callable[[Catalog, str], _Undo | None]


def _read_wiki_or_none(catalog: Catalog, path: str, *, idempotency_key: str) -> str | None:
    """The wiki text at `path`, or `None` if it doesn't have one (or doesn't
    exist) — `getwikifrom` can't tell those two apart, and neither needs to
    here: either way there's nothing to restore on rollback."""
    try:
        return catalog.getwikifrom(path, idempotency_key=idempotency_key)
    except CatalogOperationError:
        return None


def _drop_entry(catalog: Catalog, path: str, *, idempotency_key: str) -> None:
    """Drop whatever entity (table or view) now sits at `path`.

    Used to undo a fresh `copy`. Reaches into `operations`' own private
    helpers (`_entry_kind`, `_quote_path`) rather than a public op, because
    there isn't one: `deleteview` only drops VIEWs, and a plain `datacopy`
    target is always a TABLE (`CREATE TABLE ... AS SELECT`). Acceptable
    here since this module lives in the same package as `operations.py`.
    """
    executor = catalog._flight_executor  # noqa: SLF001 — same-package internal, see docstring
    kind = operations._entry_kind(executor, path, idempotency_key=idempotency_key)  # noqa: SLF001
    executor.execute(
        f"DROP {kind} IF EXISTS {operations._quote_path(path)}",  # noqa: SLF001
        idempotency_key=idempotency_key,
    )


class CatalogSession:
    """Queue catalog verbs against `catalog`, then `commit()` them as one batch.

    Every queueing method (`copy`, `move`, `tag`, `untag`, `set_wiki`,
    `delete_wiki`, `set_meta`, `create_folder`, `delete_folder`) only records
    the intent — nothing reaches Dremio until `commit()`. Each returns
    `self`, so calls chain::

        session.copy(...).tag(...).commit()

    `idempotency_key`s are generated internally (`session-<id>-<n>`, one per
    step) — never pass or think about one; that's exactly the ceremony this
    class exists to hide (see the design doc's "must be fully encapsulated").
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._id = uuid.uuid4().hex[:8]
        self._steps: list[_Step] = []
        self._next_step = 1
        self._context: str | None = None

    def __repr__(self) -> str:
        return f"CatalogSession(pending={len(self._steps)})"

    def _key(self, suffix: str = "") -> str:
        key = f"session-{self._id}-{self._next_step}{suffix}"
        return key

    def set_context(self, path: str | None) -> CatalogSession:
        """Set the "current" catalog path — a later relative path (a leading
        `.`, e.g. `.water_temperature`) resolves against this. `None` clears it.

        Not something a data custodian should normally call: ordinary use
        already keeps this up to date on its own (see `_resolve` — every
        queued step updates it from whatever path it just touched), and an
        integration (the JupyterLab catalog-tree extension pushing a
        selected leaf through a Comm — see
        `docs/notebook-facade-for-data-scientists.md`, "Pre-filling catalog
        context") is the other caller, seeding it before any path has been
        typed yet.
        """
        self._context = path
        return self

    def _resolve_path(self, path: str) -> str:
        """Just the relative -> absolute resolution (a leading `.` resolves
        against the current context) — does NOT update the context; see
        `_resolve` for the verbs where the touched path also becomes the
        new context."""
        if not path.startswith("."):
            return path
        if self._context is None:
            raise CatalogSessionError(
                f"{path!r} is relative (starts with '.') but no context is set yet — "
                "use an absolute path first, or call set_context()"
            )
        return f"{self._context}{path}"

    def _resolve(self, path: str) -> str:
        """Resolve `path` (see `_resolve_path`), then update the context to
        its own parent — so the next short name keeps resolving against
        wherever this one just landed, without anyone calling
        `set_context` again. Verbs with two paths (`copy`/`move`) resolve
        both against the *same* starting context via `_resolve_path`
        directly instead — only the target should become the new context,
        not whatever `source_path`'s own folder happens to be."""
        resolved = self._resolve_path(path)
        self._context = operations._parent_path(resolved) or self._context  # noqa: SLF001
        return resolved

    # -- queueing verbs ---------------------------------------------------

    def copy(
        self,
        source_path: str,
        target_path: str,
        *,
        overwrite: bool = False,
        create_target_folder: bool = False,
    ) -> CatalogSession:
        """Queue a `datacopy`. Reversible only when `overwrite=False` (there
        was nothing at `target_path` to lose) — undo drops the table this
        step created. With `overwrite=True`, whatever used to be at
        `target_path` is gone the moment this step runs; there is nothing to
        restore, so this step cannot be undone (see `CatalogCommitError`).

        Either path may be relative (a leading `.`) to the session's current
        context — see `set_context`; `target_path` becomes the new context
        afterwards."""
        source_path = self._resolve_path(source_path)
        target_path = self._resolve_path(target_path)
        self._context = operations._parent_path(target_path) or self._context  # noqa: SLF001

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

        self._steps.append(_Step(f"copy {source_path!r} -> {target_path!r}", run))
        return self

    def move(
        self,
        source_path: str,
        target_path: str,
        *,
        entry_type: Literal["TABLE", "VIEW"] | None = None,
        overwrite: bool = False,
        create_target_folder: bool = False,
    ) -> CatalogSession:
        """Queue a `datamove`. Reversible only when `overwrite=False` — undo
        is a `datamove` back from `target_path` to `source_path` (the
        target's kind is re-detected at undo time, not assumed). Same
        `overwrite=True` limitation as `copy`.

        Either path may be relative (a leading `.`) to the session's current
        context — see `set_context`; `target_path` becomes the new context
        afterwards."""
        source_path = self._resolve_path(source_path)
        target_path = self._resolve_path(target_path)
        self._context = operations._parent_path(target_path) or self._context  # noqa: SLF001

        def run(catalog: Catalog, key: str) -> _Undo | None:
            catalog.datamove(
                source_path,
                target_path,
                entry_type=entry_type,
                overwrite=overwrite,
                create_target_folder=create_target_folder,
                idempotency_key=key,
            )
            if overwrite:
                raise _Irreversible("overwrote an existing target; its previous content is gone")

            def undo() -> None:
                undo_key = f"{key}-undo"
                kind = operations._entry_kind(  # noqa: SLF001 — see _drop_entry
                    catalog._flight_executor,  # noqa: SLF001
                    target_path,
                    idempotency_key=f"{undo_key}-detect",
                )
                catalog.datamove(
                    target_path,
                    source_path,
                    entry_type=kind,
                    overwrite=False,
                    idempotency_key=undo_key,
                )

            return undo

        self._steps.append(_Step(f"move {source_path!r} -> {target_path!r}", run))
        return self

    def tag(self, path: str, tags: list[str]) -> CatalogSession:
        """Queue `settagsto` (replaces the tag set). Undo restores whatever
        tags were on `path` immediately before this step ran.

        `path` may be relative (a leading `.`) to the session's current
        context — see `set_context`; it becomes the new context afterwards."""
        path = self._resolve(path)

        def run(catalog: Catalog, key: str) -> _Undo:
            existing = catalog.gettagsfrom(path, idempotency_key=f"{key}-read")
            catalog.settagsto(path, tags, idempotency_key=key)

            def undo() -> None:
                catalog.settagsto(path, existing, idempotency_key=f"{key}-undo")

            return undo

        self._steps.append(_Step(f"tag {path!r} with {tags!r}", run))
        return self

    def untag(self, path: str, tags: list[str]) -> CatalogSession:
        """Queue `deletetags`. Undo restores the full tag set `path` had
        immediately before this step ran.

        `path` may be relative (a leading `.`) to the session's current
        context — see `set_context`; it becomes the new context afterwards."""
        path = self._resolve(path)

        def run(catalog: Catalog, key: str) -> _Undo:
            existing = catalog.gettagsfrom(path, idempotency_key=f"{key}-read")
            catalog.deletetags(path, tags, idempotency_key=key)

            def undo() -> None:
                catalog.settagsto(path, existing, idempotency_key=f"{key}-undo")

            return undo

        self._steps.append(_Step(f"untag {tags!r} from {path!r}", run))
        return self

    def set_wiki(
        self, path: str, text: str, *, tags: list[dict[str, str]] | None = None
    ) -> CatalogSession:
        """Queue `setwikito`. Undo restores the previous wiki text verbatim,
        or deletes it if `path` had none.

        `path` may be relative (a leading `.`) to the session's current
        context — see `set_context`; it becomes the new context afterwards."""
        path = self._resolve(path)

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

        `path` may be relative (a leading `.`) to the session's current
        context — see `set_context`; it becomes the new context afterwards."""
        path = self._resolve(path)

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

    def set_meta(
        self, path: str, tags: list[dict[str, str]] | None = None, *, overwrite: bool = True
    ) -> CatalogSession:
        """Queue `setmeta2wiki`. Undo restores the whole previous wiki text
        (not just its Meta Data section) verbatim, or deletes it if `path`
        had none.

        `path` may be relative (a leading `.`) to the session's current
        context — see `set_context`. Unlike a leaf-level verb (`tag`,
        `set_wiki`, ...), `path` here is a folder (see `_require_folder`),
        so it becomes the new context *itself*, not its parent — the next
        short name is expected to name something *inside* it."""
        path = self._resolve_path(path)
        self._context = path

        def run(catalog: Catalog, key: str) -> _Undo:
            existing = _read_wiki_or_none(catalog, path, idempotency_key=f"{key}-read")
            catalog.setmeta2wiki(path, tags=tags, overwrite=overwrite, idempotency_key=key)

            def undo() -> None:
                undo_key = f"{key}-undo"
                if existing is None:
                    catalog.deletewiki(path, idempotency_key=undo_key)
                else:
                    catalog.setwikito(path, existing, idempotency_key=undo_key)

            return undo

        self._steps.append(_Step(f"set metadata on {path!r}", run))
        return self

    def create_folder(self, path: str, *, create_parents: bool = False) -> CatalogSession:
        """Queue `createfolder`. Undo deletes it — but only if this step
        actually created it; `createfolder` is idempotent, so a `path`
        that already existed is left alone on rollback too.

        `path` may be relative (a leading `.`) to the session's current
        context — see `set_context` — and becomes the new context *itself*
        afterwards (a folder's contents, not its parent, is where a
        following short name most likely points)."""
        path = self._resolve_path(path)
        self._context = path

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

        `path` may be relative (a leading `.`) to the session's current
        context — see `set_context`. Does not change the context itself —
        there's nothing meaningful to navigate into once it's deleted."""
        path = self._resolve_path(path)

        def run(catalog: Catalog, key: str) -> _Undo | None:
            catalog.deletefolder(path, cascade=cascade, idempotency_key=key)
            raise _Irreversible("deleted folder contents cannot be recreated")

        self._steps.append(_Step(f"delete folder {path!r}", run))
        return self

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
        return CommitReport(succeeded=[step.description for step, _ in executed])

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
