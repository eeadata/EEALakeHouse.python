"""`%catalog`/`%%catalog` and `%ingest` — thin syntactic sugar over
`CatalogSession`/`IngestSession` (see
`docs/notebook-facade-for-data-scientists.md`, "Two magics, two sessions").

Available the moment this package is imported inside IPython — `import
eea_datalakehouse.notebook` (see that package's `__init__.py`) registers both
magics as a side effect, so nothing needs loading explicitly. `%load_ext
eea_datalakehouse.notebook.magics` still works too (and is the only option
outside IPython's auto-import path, e.g. a config that imports this module
directly) — `load_ipython_extension` below is a no-op if the auto-import
already registered these, so using both never double-registers or drops an
`IngestSession`'s queued-but-not-committed state.

In any cell, once loaded either way::

    %catalog data_copy("draft.raw_2026", "bwd.reference.water_temperature")
    %catalog set_tags("bwd.reference.water_temperature", ["reviewed"])

    %ingest ingest(folder="./bw_2026", target_catalog_path="bwd.reference",
                    data_format="parquet", table_name="water_temperature")
    %ingest commit(retry=True)

`%catalog` executes each call immediately — there's no queue and no separate
commit step to remember. A `CatalogSession` still sits underneath it (to keep
the "current path" context, and the same retry-on-`EngineStartingError`/
rollback-on-failure safety `commit()` always had — see `CatalogSession.commit`),
but that's an implementation detail this magic hides by committing right
after every call. `%ingest`, on the other hand, still queues (`IngestSession`)
and needs an explicit `commit()` — a catalog operation can't run before its
target has actually been ingested, so `%ingest`'s batch and `%catalog`'s
immediate calls were never meant to share one queue anyway (see the design
doc's "Two sessions, not one"). `%catalog`/`%ingest` invent no vocabulary of
their own: every name after them is a real `CatalogSession`/`IngestSession`
method (`%catalog help`/`%ingest help` lists them) — this file only builds
the session(s) and turns exceptions into a short printed message instead of a
traceback, per the design doc's facade plan. `commit` with no parentheses is
accepted as a convenience for `commit()` — still meaningful for `%ingest`; a
no-op for `%catalog`, whose queue is always empty by the time a cell
finishes.

`eval()` below runs exactly the Python the user typed after the magic name,
in their own notebook namespace — no new capability over what the same user
could already type directly into the next cell; that's what makes a `%magic`
different from evaluating untrusted input.

Credentials/connection details come from the kernel environment, the same
`DREMIO_...` variables `debugger/debug_run.py` and `FolderIngest`'s default
construction already use — this module never asks for or stores a token
itself.

`CatalogSession`'s "current path" context (a leading `.` on a path resolves
against it — see that class' `set_context`/`_resolve`) is already kept
up to date automatically just from ordinary `%catalog` use, so no data
custodian ever needs to set it themselves for that. `use(path)` sets it
deliberately instead — unlike `set_context`, `path` itself may be relative
too, resolved against whatever context already exists. Most often reached
through `%%catalog`, the cell-magic form: it runs `use(...)` on its magic
line, then every other line of the cell in order, so the whole cell shares
one context without repeating a path on each line::

    %%catalog use("bwd.reference")
    tag(".water_temperature", ["reviewed"])
    create_folder(".2027")

This module also registers a Jupyter Comm target (see
`_register_context_comm`) so an integrated frontend — the JupyterLab
catalog-tree extension, in the separate `eeadata/EEALakeHouse` repo — can
push a selected leaf's path into this kernel invisibly: no cell runs,
nothing a custodian could see or type, either. See
`docs/notebook-facade-for-data-scientists.md`, "Pre-filling catalog context
from a JupyterLab tree click".
"""

from __future__ import annotations

import inspect
import os
from html import escape as _escape
from typing import Any

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from IPython.display import HTML, display

from ..catalog import Catalog
from ..catalog.session import CatalogSession, CatalogSessionError
from ..dds_ingestion.session import IngestSession, IngestSessionError

_USAGE = {
    "catalog": (
        '%catalog data_copy("a.b", "c.d")  |  %catalog set_tags("a.b", ["reviewed"])'
        "  (each call runs immediately)"
    ),
    "ingest": (
        '%ingest ingest(folder="./data", target_catalog_path="a.b", data_format="parquet")'
        "  |  %ingest commit"
    ),
}

# Shared between set_wiki's _CATALOG_HELP entry below and set_tags'/
# delete_tags' spirit (tags: a list of {tag_name, ...} dicts) — kept as one
# constant so the wiki-tags example can't quietly drift.
_META_TAGS_EXAMPLE = '[{"tag_name": "owner", "tag_value": "bw-team", "tag_title": "Owner"}]'

# Shared between copy/move/create_view's _CATALOG_HELP entries — all three
# resolve create_target_folder the same way (see _resolve_target_path's
# ensure_target_folder step in operations.py).
_CREATE_TARGET_FOLDER_NOTE = (
    "create_target_folder=True creates every missing folder level of target_path — "
    "except the space/source itself, which must already exist (raises "
    "CatalogOperationError otherwise)."
)

# One short line per public method — shown by `%catalog help`/`%ingest help`.
# Kept separate from each method's own (much longer) docstring on purpose:
# this is a quick-reference table, not a replacement for reading the real
# docstring in `catalog/session.py`/`dds_ingestion/session.py`. `commit` is
# deliberately left off `_CATALOG_HELP` — it still exists on `CatalogSession`
# (auto_commit in `_dispatch` calls it), but isn't something a custodian needs
# to call themselves any more. `set_context` is left off too — `use` does the
# same job and also resolves a relative path, so it's the one to list.
#
# Ordered deliberately, not alphabetically: `use`/`get_context` first (context
# is always the first thing to reach for), then five grouped sections — data,
# table, view, wiki, tags — each set/delete/get (create/delete for view, since
# there's no "get" for one), and `create_folder`/`delete_folder` last, since
# folders aren't one of those five groups.
_CATALOG_HELP = [
    (
        "use",
        "Set the current path for every call after this one; path may be a whole path, "
        "or relative to the current context (with or without a leading '.') once one "
        "exists — '../name' or a bare '..' walks up one level first, chainable "
        "('../../name'). None clears it. Raises if the resolved path doesn't exist in "
        "the catalog. See %%catalog to set it once at the top of a cell.",
    ),
    (
        "get_context",
        "Show the current path (None if nothing has been set yet).",
    ),
    # -- data ---------------------------------------------------------------
    (
        "data_copy",
        "Copy source_path to target_path. overwrite=False (default) raises "
        "CatalogOperationError if target_path already exists; overwrite=True replaces it "
        "and is not undoable. " + _CREATE_TARGET_FOLDER_NOTE,
    ),
    (
        "data_move",
        "Move source_path to target_path, always as a TABLE (use create_view for a view). "
        "overwrite=False (default) raises CatalogOperationError if target_path already "
        "exists; overwrite=True replaces it and is not undoable. " + _CREATE_TARGET_FOLDER_NOTE,
    ),
    # -- table ----------------------------------------------------------------
    (
        "list",
        "Show every table/view under path, at any depth, as full dot-separated paths. "
        "Raises CatalogOperationError if path doesn't exist.",
    ),
    (
        "schema",
        "Show path's column schema and row count (never fetches the actual rows). path "
        "must be a table or view — raises CatalogOperationError otherwise, or if path "
        "doesn't exist at all.",
    ),
    (
        "delete_table",
        "Delete the table at path. Not undoable. Raises CatalogOperationError if path "
        "doesn't exist (unlike a bare DROP TABLE, this doesn't quietly no-op on a typo).",
    ),
    # -- view -----------------------------------------------------------------
    (
        "create_view",
        "Create a VIEW at target_path over source_path, leaving source_path untouched. "
        "overwrite=False (default) raises CatalogOperationError if target_path already "
        "exists; overwrite=True replaces it and is not undoable. " + _CREATE_TARGET_FOLDER_NOTE,
    ),
    (
        "delete_view",
        "Delete the view at path. Not undoable. Raises CatalogOperationError if path "
        "doesn't exist (unlike a bare DROP VIEW, this doesn't quietly no-op on a typo).",
    ),
    # -- wiki -----------------------------------------------------------------
    (
        "set_wiki",
        "Set a path's wiki text. tags: a list of {tag_name, tag_value, tag_title} dicts, "
        "e.g. " + _META_TAGS_EXAMPLE + ".",
    ),
    ("delete_wiki", "Delete a path's wiki text."),
    (
        "get_wiki",
        "Show a path's wiki text. Raises CatalogOperationError if the path doesn't "
        "exist, or exists but has no wiki at all.",
    ),
    # -- tags -----------------------------------------------------------------
    (
        "set_tags",
        'Replace a path\'s tag set. tags: a list of strings, e.g. ["reviewed"]. Tables/views '
        "only — raises CatalogOperationError otherwise.",
    ),
    (
        "delete_tags",
        'Remove tags from a path\'s tag set. tags: a list of strings, e.g. ["reviewed"]. '
        "Tables/views only — raises CatalogOperationError otherwise.",
    ),
    (
        "get_tags",
        "Show the tags on a path — tables/views only. Raises CatalogOperationError if "
        "the path doesn't exist or isn't a table/view.",
    ),
    # -- folders (not one of the five groups above) ----------------------------
    (
        "create_folder",
        "Create a folder. create_parents=True creates every missing level of the full "
        "path; create_parents=False (default) raises CatalogOperationError if the parent "
        "folder doesn't already exist.",
    ),
    (
        "delete_folder",
        "Delete a folder. Not undoable. cascade=True also deletes everything at path "
        "and below; cascade=False (default) raises CatalogOperationError if the folder "
        "isn't empty. A path that's already gone is not an error.",
    ),
]
_INGEST_HELP = [
    ("ingest", "Queue one folder ingest (see FolderIngest for what each argument means)."),
    ("commit", "Run every queued ingest in order; stops at the first failure."),
]


def _build_catalog_session() -> CatalogSession:
    base_url = os.environ.get("DREMIO_BASE_URL")
    token = os.environ.get("DREMIO_TOKEN")
    username = os.environ.get("DREMIO_USERNAME")
    if not base_url or not token:
        raise RuntimeError(
            "%catalog needs DREMIO_BASE_URL and DREMIO_TOKEN set in the kernel environment"
        )
    return CatalogSession(Catalog(base_url, token, username=username))


def _is_help(line: str) -> bool:
    return line.strip() in ("help", "help()")


def _format_signature(func: Any) -> str:
    """Render `func`'s signature (minus `self`) the way it reads in source —
    `inspect.Signature`'s own `str()` wraps string annotations in quotes
    (they're plain `str`s at runtime because of this module's, and the
    session modules', `from __future__ import annotations`), which is
    accurate but noisy for a notebook help message."""
    sig = inspect.signature(func)
    parts = []
    seen_star = False
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        if param.kind is inspect.Parameter.KEYWORD_ONLY and not seen_star:
            parts.append("*")
            seen_star = True
        piece = name
        if param.annotation is not inspect.Parameter.empty:
            piece += f": {param.annotation}"
        if param.default is not inspect.Parameter.empty:
            piece += f" = {param.default!r}"
        parts.append(piece)
    rendered = f"({', '.join(parts)})"
    if sig.return_annotation is not inspect.Signature.empty:
        rendered += f" -> {sig.return_annotation}"
    return rendered


def _print_help(cls: type, methods: list[tuple[str, str]], label: str) -> None:
    """Print every method in `methods` with its real signature (introspected
    from `cls`, so it can't drift from the source) and a one-line
    description. Signatures drop `self`; everything else — parameter names,
    defaults, `*`-only markers, return types — comes straight from `cls`."""
    print(f"%{label} methods — usage: {_USAGE[label]}")
    print()
    for name, description in methods:
        print(f"  {name}{_format_signature(getattr(cls, name))}")
        print(f"      {description}")
    print()
    print(f"%{label} help  — show this message")


def _plain_params(func: Any) -> str:
    """Comma-separated parameter names (minus `self`), for a non-developer
    reading `%catalog help`'s table — no Python type-hint syntax (a bare
    `str | None` union means nothing to a data custodian, and would collide
    visually with the table's own `|` column separators anyway) and no `*`
    keyword-only marker. A parameter with a default is shown as `name=default`
    so it still reads as optional; everything else is required, in order."""
    sig = inspect.signature(func)
    parts = [
        name if param.default is inspect.Parameter.empty else f"{name}={param.default!r}"
        for name, param in sig.parameters.items()
        if name != "self"
    ]
    return ", ".join(parts)


_CATALOG_HELP_HEADER_STYLE = (
    "text-align:left; padding:4px 12px; border-bottom:2px solid currentColor;"
)
_CATALOG_HELP_CELL_STYLE = (
    "text-align:left; padding:4px 12px; border-bottom:1px solid currentColor; vertical-align:top;"
)
_CATALOG_HELP_CODE_STYLE = _CATALOG_HELP_CELL_STYLE + " font-family:monospace; white-space:pre;"


def _catalog_help_html(rows: list[tuple[str, str, str]]) -> str:
    """Build the `<table>` markup for `%catalog help` — inline styles only
    (no external stylesheet, no hardcoded background/text color — just
    `currentColor` borders) so it reads correctly in both a light and a dark
    notebook theme without knowing which one is active."""

    def th(text: str) -> str:
        return f'<th style="{_CATALOG_HELP_HEADER_STYLE}">{_escape(text)}</th>'

    def td(text: str, *, code: bool = False) -> str:
        style = _CATALOG_HELP_CODE_STYLE if code else _CATALOG_HELP_CELL_STYLE
        return f'<td style="{style}">{_escape(text)}</td>'

    head = f"<tr>{th('Command')}{th('Parameters')}{th('Description')}</tr>"
    body = "".join(
        f"<tr>{td(name, code=True)}{td(params, code=True)}{td(description)}</tr>"
        for name, params, description in rows
    )
    return f'<table style="border-collapse:collapse;">{head}{body}</table>'


def _print_catalog_help_table() -> None:
    """`%catalog help` — a real HTML `<table>` (Command / Parameters /
    Description), rendered via `IPython.display` rather than `_print_help`'s
    ASCII Python-signature listing: this magic's audience is a data
    custodian reading it in JupyterLab, not necessarily someone comfortable
    with a Python type signature or a monospace grid. Only this one magic's
    help gets the rich-display treatment — everything else in this module
    stays plain `print()` (see the module docstring's "eval() below runs
    exactly the Python the user typed" — errors and usage lines are meant to
    read like ordinary interpreter output, not a UI)."""
    print(f"%catalog methods — usage: {_USAGE['catalog']}")
    print(
        "A path/source_path/target_path starting with '.' resolves against the current "
        "context (see use); one or more leading '../' (or a bare '..') walks up that many "
        "levels first. Ordinary use already keeps context current on its own."
    )

    rows = [
        (name, _plain_params(getattr(CatalogSession, name)), description)
        for name, description in _CATALOG_HELP
    ]
    display(HTML(_catalog_help_html(rows)))


def _dispatch(
    session: Any,
    line: str,
    user_ns: dict[str, Any],
    label: str,
    error_type: type[Exception],
    *,
    auto_commit: bool = False,
) -> Any:
    line = line.strip()
    if not line:
        print(f"usage: {_USAGE[label]}")
        return None
    if line.isidentifier():  # bare "commit" (no parens) as a convenience
        line = f"{line}()"
    namespace = {**user_ns, "__session__": session}
    try:
        result = eval(f"__session__.{line}", namespace)  # see module docstring re: eval
        if auto_commit and result is session:
            # A queueing verb just chained back to `self` — commit it right
            # away instead of waiting for a separate commit() call (see the
            # module docstring: %catalog executes immediately).
            result = session.commit(retry=True)
    except error_type as exc:
        print(f"{label} error: {exc}")
        return None
    return result


_CONTEXT_COMM_TARGET = "eea_datalakehouse.catalog_context"


def _register_context_comm(shell: Any, magics: EEALakehouseMagics) -> None:
    """Wire up a Jupyter Comm target so an integrated frontend (the
    JupyterLab catalog-tree extension, in the separate `eeadata/EEALakeHouse`
    repo) can push a selected leaf's path into this kernel's CatalogSession
    invisibly — no cell runs, nothing a data custodian could see or type.

    A no-op if there's no live kernel Comm manager to register against — a
    plain IPython shell, or this package's own test shell, neither of which
    is a real Jupyter kernel.
    """
    comm_manager = getattr(getattr(shell, "kernel", None), "comm_manager", None)
    if comm_manager is None:
        return

    def _on_comm_open(comm: Any, open_msg: dict[str, Any]) -> None:
        @comm.on_msg
        def _on_msg(msg: dict[str, Any]) -> None:
            path = msg.get("content", {}).get("data", {}).get("path")
            if isinstance(path, str):
                magics._apply_context(path)

    comm_manager.register_target(_CONTEXT_COMM_TARGET, _on_comm_open)


@magics_class
class EEALakehouseMagics(Magics):
    """Registers `%catalog` and `%ingest` — see this module's docstring."""

    def __init__(self, shell: Any) -> None:
        super().__init__(shell)
        self._catalog_session: CatalogSession | None = None
        self._ingest_session: IngestSession | None = None
        self._pending_context: str | None = None
        _register_context_comm(shell, self)

    def _apply_context(self, path: str) -> None:
        """Set `path` as the CatalogSession's context. Called only by the
        Comm handler in `_register_context_comm` — never by a data
        custodian directly, and never dispatched through `%catalog`.

        Builds the session now if one doesn't exist yet, so a context
        pushed before any `%catalog` cell has run still takes effect. If
        credentials aren't available yet either, remembers `path` instead
        of failing — applied the moment `%catalog` next builds a session
        for real, rather than printing an error outside of any cell a
        custodian actually ran.
        """
        if self._catalog_session is None:
            try:
                self._catalog_session = _build_catalog_session()
            except RuntimeError:
                self._pending_context = path
                return
        self._catalog_session.set_context(path)

    def _ensure_catalog_session(self) -> bool:
        """Build `self._catalog_session` if it doesn't exist yet, applying
        any Comm-pushed context that arrived first. Returns `False` (after
        printing a friendly error) if credentials aren't available —
        `%catalog`/`%%catalog` both bail out at that point rather than
        dispatching against no session."""
        if self._catalog_session is not None:
            return True
        try:
            self._catalog_session = _build_catalog_session()
        except RuntimeError as exc:
            print(f"catalog error: {exc}")
            return False
        if self._pending_context is not None:
            self._catalog_session.set_context(self._pending_context)
            self._pending_context = None
        return True

    @line_magic
    def catalog(self, line: str) -> Any:
        if _is_help(line):
            _print_catalog_help_table()
            return None
        if not self._ensure_catalog_session():
            return None
        return _dispatch(
            self._catalog_session,
            line,
            self.shell.user_ns,
            "catalog",
            CatalogSessionError,
            auto_commit=True,
        )

    @cell_magic("catalog")
    def catalog_cell(self, line: str, cell: str) -> Any:
        """`%%catalog` — one call per line, in order, typically opened with
        `use(path)` to set the context once for the whole cell (see
        `CatalogSession.use`) instead of repeating a path on every line::

            %%catalog use("bwd.reference")
            tag(".water_temperature", ["reviewed"])
            create_folder(".2027")

        Each line dispatches exactly like a `%catalog` line-magic call
        (same immediate-execution, same friendly error printing) — the
        magic line (`use("bwd.reference")` above) runs first, then every
        non-blank line of the cell body, in order. Stops at the first line
        that fails (its error has already been printed) rather than
        running the rest against a context that isn't what the custodian
        expected."""
        if not self._ensure_catalog_session():
            return None
        result: Any = None
        for statement in (line, *cell.splitlines()):
            if not statement.strip():
                continue
            result = _dispatch(
                self._catalog_session,
                statement,
                self.shell.user_ns,
                "catalog",
                CatalogSessionError,
                auto_commit=True,
            )
            if result is None:
                break  # a friendly error was already printed by _dispatch
        return result

    @line_magic
    def ingest(self, line: str) -> Any:
        if _is_help(line):
            _print_help(IngestSession, _INGEST_HELP, "ingest")
            return None
        if self._ingest_session is None:
            self._ingest_session = IngestSession()
        return _dispatch(
            self._ingest_session, line, self.shell.user_ns, "ingest", IngestSessionError
        )


def load_ipython_extension(ipython: Any) -> None:
    # A no-op if `eea_datalakehouse.notebook`'s own auto-import already
    # registered this (see the module docstring) — registering again would
    # replace the live instance with a fresh one, silently dropping the
    # CatalogSession's "current path" context and any already-queued-but-
    # not-committed IngestSession state.
    if "EEALakehouseMagics" in ipython.magics_manager.registry:
        return
    ipython.register_magics(EEALakehouseMagics)
