"""`%catalog` and `%ingest` — thin syntactic sugar over `CatalogSession`/
`IngestSession` (see `docs/notebook-facade-for-data-scientists.md`, "Two
magics, two sessions").

Available the moment this package is imported inside IPython — `import
eea_datalakehouse.notebook` (see that package's `__init__.py`) registers both
magics as a side effect, so nothing needs loading explicitly. `%load_ext
eea_datalakehouse.notebook.magics` still works too (and is the only option
outside IPython's auto-import path, e.g. a config that imports this module
directly) — `load_ipython_extension` below is a no-op if the auto-import
already registered these, so using both never double-registers or drops a
session's queued-but-not-committed state.

In any cell, once loaded either way::

    %catalog copy("draft.raw_2026", "bwd.reference.water_temperature")
    %catalog tag("bwd.reference.water_temperature", ["reviewed"])
    %catalog commit(retry=True)

    %ingest ingest(folder="./bw_2026", target_catalog_path="bwd.reference",
                    data_format="parquet", table_name="water_temperature")
    %ingest commit(retry=True)

Each magic evaluates the rest of the line as a method call on ONE session
object, created lazily the first time that magic is used in this kernel and
kept for the kernel's lifetime (see the design doc's "Session context lives
in the Python process") — so calls in different cells accumulate on the same
queue, and `commit()` flushes it. `%catalog`/`%ingest` invent no vocabulary
of their own: every name after them is a real `CatalogSession`/`IngestSession`
method (see those modules for the full list) — this file only builds the
session and turns exceptions into a short printed message instead of a
traceback, per the design doc's facade plan. `commit` with no parentheses is
accepted as a convenience for `commit()`.

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
custodian ever needs to call `set_context` themselves. On top of that, this
module also registers a Jupyter Comm target (see `_register_context_comm`)
so an integrated frontend — the JupyterLab catalog-tree extension, in the
separate `eeadata/EEALakeHouse` repo — can push a selected leaf's path into
this kernel invisibly: no cell runs, nothing a custodian could see or type,
either. See `docs/notebook-facade-for-data-scientists.md`, "Pre-filling
catalog context from a JupyterLab tree click".
"""

from __future__ import annotations

import inspect
import os
from typing import Any

from IPython.core.magic import Magics, line_magic, magics_class

from ..catalog import Catalog
from ..catalog.session import CatalogSession, CatalogSessionError
from ..dds_ingestion.session import IngestSession, IngestSessionError

_USAGE = {
    "catalog": (
        '%catalog copy("a.b", "c.d")  |  %catalog tag("a.b", ["reviewed"])  |  %catalog commit'
    ),
    "ingest": (
        '%ingest ingest(folder="./data", target_catalog_path="a.b", data_format="parquet")'
        "  |  %ingest commit"
    ),
}

# One short line per public method — shown by `%catalog help`/`%ingest help`.
# Kept separate from each method's own (much longer) docstring on purpose:
# this is a quick-reference table, not a replacement for reading the real
# docstring in `catalog/session.py`/`dds_ingestion/session.py`.
_CATALOG_HELP = [
    (
        "set_context",
        "Set the current path; a later relative path (leading '.') resolves against it.",
    ),
    ("copy", "Queue a copy. Reversible unless overwrite=True."),
    ("move", "Queue a move. Reversible unless overwrite=True."),
    ("tag", "Queue replacing a path's tag set."),
    ("untag", "Queue removing tags from a path's tag set."),
    ("set_wiki", "Queue setting a path's wiki text."),
    ("delete_wiki", "Queue deleting a path's wiki text."),
    ("set_meta", "Queue setting a folder's Meta Data wiki section."),
    ("create_folder", "Queue creating a folder."),
    ("delete_folder", "Queue deleting a folder. Never reversible."),
    ("commit", "Run every queued step as one all-or-nothing batch."),
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


def _dispatch(
    session: Any, line: str, user_ns: dict[str, Any], label: str, error_type: type[Exception]
) -> Any:
    line = line.strip()
    if not line:
        print(f"usage: {_USAGE[label]}")
        return None
    if line.isidentifier():  # bare "commit" (no parens) as a convenience
        line = f"{line}()"
    namespace = {**user_ns, "__session__": session}
    try:
        return eval(f"__session__.{line}", namespace)  # see module docstring re: eval
    except error_type as exc:
        print(f"{label} error: {exc}")
        return None


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

    @line_magic
    def catalog(self, line: str) -> Any:
        if _is_help(line):
            _print_help(CatalogSession, _CATALOG_HELP, "catalog")
            return None
        if self._catalog_session is None:
            try:
                self._catalog_session = _build_catalog_session()
            except RuntimeError as exc:
                print(f"catalog error: {exc}")
                return None
            if self._pending_context is not None:
                self._catalog_session.set_context(self._pending_context)
                self._pending_context = None
        return _dispatch(
            self._catalog_session, line, self.shell.user_ns, "catalog", CatalogSessionError
        )

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
    # replace the live instance with a fresh one, silently dropping any
    # already-queued-but-not-committed CatalogSession/IngestSession state.
    if "EEALakehouseMagics" in ipython.magics_manager.registry:
        return
    ipython.register_magics(EEALakehouseMagics)
