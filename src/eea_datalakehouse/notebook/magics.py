"""`%catalog`/`%%catalog`, `%ingest`, `%sdi` and `%metadata` — thin syntactic
sugar over `CatalogSession`/`IngestSession`/`SdiSession`/`MetadataSession` (see
`docs/notebook-facade-for-data-scientists.md`, "Two magics, two sessions").

Available the moment this package is imported inside IPython — `import
eea_datalakehouse.notebook` (see that package's `__init__.py`) registers every
magic as a side effect, so nothing needs loading explicitly. `%load_ext
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

    %sdi get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")
    %metadata push_to_dds("catalog/water_management_resources/bathing_water/bwd")

`%sdi` (`eea_datalakehouse.sdi.session.SdiSession`) only reads the SDI
catalogue; `%metadata` (`MetadataSession`, same module) pushes metadata files
to DDS. Both run each call immediately, like `%catalog`, and are built on first
use, like `%ingest`'s session, from the kernel environment (see that module's
docstring for the variables). `%metadata push_to_dds` uploads the record
`%sdi get_xml` fetched last, so it needs only the DDS path.

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

`CatalogSession`'s "current path" context (a path resolves against it once
it's set, with or without a leading `.` — see that class' `set_context`/
`_resolve_path`) is set only by `use(path)` — no other verb changes it as
a side effect of running, even one whose own `path`/`source_path`/
`target_path` fully resolved to something that could sensibly become the
new context. Unlike every other verb's own `path`/`source_path`/
`target_path`, `use`'s `path` is always a whole, absolute path (never
resolved against whatever context already exists), and it makes a live
check that it actually exists in the catalog first. Most often reached
through `%%catalog`, the cell-magic form: it runs `use(...)` on its magic
line, then every other line of the cell in order, so the whole cell
shares one context without repeating a path on each line::

    %%catalog use("bwd.reference")
    set_tags(".water_temperature", ["reviewed"])
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

import ast
import inspect
import os
import re
from html import escape as _escape
from typing import Any

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from IPython.display import HTML, display

from ..catalog import Catalog
from ..catalog.session import CatalogSession, CatalogSessionError
from ..dds_ingestion.session import IngestSession, IngestSessionError
from ..sdi.session import MetadataSession, MetadataSessionError, SdiSession, SdiSessionError

_USAGE = {
    "catalog": (
        '%catalog data_copy("a.b", "c.d")  |  %catalog set_tags("a.b", ["reviewed"])'
        "  (each call runs immediately)"
    ),
    "ingest": (
        '%ingest ingest(folder="./data", target_catalog_path="a.b", data_format="parquet")'
        "  |  %ingest commit"
    ),
    "sdi": '%sdi get_xml("<uuid>")  |  %sdi resolve_series("<series uuid>")',
    "metadata": (
        '%metadata push_to_dds("a.b.c")  or  %metadata push_to_dds("a/b/c")'
        "  (uploads the last %sdi get_xml result)"
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
        "Set the current path for every call after this one; path must be a whole, "
        "absolute path — never relative to the current context, unlike every other "
        "verb's path/source_path/target_path. None or '' resets it to 'catalog' (the "
        "root). Raises if path doesn't exist in the catalog. See %%catalog to set it "
        "once at the top of a cell.",
    ),
    (
        "get_context",
        "Show the current path. A new session starts at 'catalog' (the root); "
        "use(None)/use('') reset back to it too — None only after an explicit "
        "set_context(None).",
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
    (
        "create_version",
        "Copy the listed tables from source_path into a new folder target_path.version_name "
        "(as CREATE TABLE snapshots, keeping each table's sub-folders). source_path and "
        "version_name must not be blank. tables omitted or empty copies everything under "
        "source_path. Each entry is a table "
        "or a folder (= every table under it), named as listed under source_path or as list() "
        "shows them. target_path omitted means the current context — use() picks where "
        "versions go; a missing target_path is created unless create_target_folder=False. "
        "Reference/vocabulary tables (a "
        "folder named reference(s)/vocabulary(ies) in the path) and views are skipped; the "
        "report's notes say which. An existing version folder is an error unless "
        "overwrite=True (replaces the listed tables, keeps the rest).",
    ),
    (
        "draft_to_version",
        "create_version from the dataflow's draft folder, with no paths to type: the source is "
        "the draft folder the context is in (or the draft folder inside the context), the "
        "target is draft's parent.versions.version_name (versions is created if missing). "
        "tables as in create_version: omitted or empty copies everything in draft.",
    ),
    # -- table ----------------------------------------------------------------
    (
        "list",
        "Show every table/view under path, at any depth, as full dot-separated paths. "
        "path may be omitted to list the current context itself — raises "
        "CatalogSessionError if none is set. Raises CatalogOperationError if path "
        "doesn't exist.",
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
        "e.g. " + _META_TAGS_EXAMPLE + ". Ignored if path is a table/view (they have "
        "set_tags/get_tags for that instead).",
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
_SDI_HELP = [
    (
        "get_xml",
        "Download the ISO 19115-3 XML of SDI record uuid, check it really is that record, "
        "and remember it for %metadata push_to_dds. Runs immediately; needs no DDS settings.",
    ),
    (
        "resolve_series",
        "Return the UUID of the one release of series_uuid that is not superseded; raises, "
        "listing every release, if there are zero or several.",
    ),
]
_SDI_HELP_NOTE = (
    "Environment: SDI_API_URL (empty means the public EEA catalogue; optional "
    "SDI_USERNAME/SDI_PASSWORD for non-public records). Pushing to DDS is %metadata's job."
)
# dds_base_url()/dremio_base_url() still work under %metadata; they are left
# out of the help table on purpose, as troubleshooting helpers rather than
# everyday commands.
_METADATA_HELP = [
    (
        "push_to_dds",
        "Upload metadata (default: the last %sdi get_xml result) to "
        "dds_path/folder/<uuid>.xml in DDS — a document upload, never an ingest. "
        "target_name renames the file (e.g. target_name=\"bwd_2025.xml\"; .xml is added if "
        "missing). dds_path "
        "must already exist in the Dremio catalog (check_catalog=False skips that check). "
        "Same bytes already there: unchanged; an older copy: replaced; a copy edited in DDS: "
        "refused unless force=True.",
    ),
    (
        "check_catalog_path",
        "Check dds_path exists in the Dremio catalog without pushing anything.",
    ),
]
_METADATA_HELP_NOTE = (
    "dds_path is the dataset's folder path, without the metadata folder. Its folders can be "
    "separated with either '.' or '/', so these are the same path:\n"
    "  catalog.water_management_resources.bathing_water.bwd\n"
    "  catalog/water_management_resources/bathing_water/bwd\n"
    "A name containing a '.' or a space is written in double quotes with '.' separators "
    '(catalog."bathing water"."v1.0"), or as it is with \'/\' separators '
    "(catalog/bathing water/v1.0).\n"
    "Environment: DDS_BASE_URL and DREMIO_BASE_URL — read from the first .env found in the "
    "notebook's folder or a parent when %metadata starts, else from the kernel environment "
    "— and a Dremio identity: _DREMIO_USER/_DREMIO_PWD (as %ingest) or "
    "DREMIO_USERNAME/DREMIO_TOKEN (as %catalog), used for both DDS and the catalog check."
)


def _build_catalog_session() -> CatalogSession:
    base_url = os.environ.get("DREMIO_BASE_URL")
    token = os.environ.get("DREMIO_TOKEN")
    username = os.environ.get("DREMIO_USERNAME")
    if not base_url or not token:
        raise RuntimeError(
            "%catalog needs DREMIO_BASE_URL and DREMIO_TOKEN set in the kernel environment"
        )
    return CatalogSession(Catalog(base_url, token, username=username))


def _build_sdi_session() -> SdiSession:
    # Nothing to check up front: SDI_API_URL defaults to the public catalogue.
    return SdiSession()


def _build_metadata_session(last: Any) -> MetadataSession:
    # MetadataSession reads DDS_BASE_URL from the nearest .env right here, when
    # it is built; a missing DDS setting or Dremio identity prints as a
    # friendly "metadata error" on the first push_to_dds.
    return MetadataSession(last=last)


def _is_help(line: str) -> bool:
    return line.strip() in ("help", "help()")


def _plain_params(func: Any) -> str:
    """Comma-separated parameter names (minus `self`), for a non-developer
    reading `%catalog help`'s/`%ingest help`'s table — no Python type-hint syntax (a bare
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


_HELP_TABLE_HEADER_STYLE = (
    "text-align:left; padding:4px 12px; border-bottom:2px solid currentColor;"
)
_HELP_TABLE_CELL_STYLE = (
    "text-align:left; padding:4px 12px; border-bottom:1px solid currentColor; vertical-align:top;"
)
_HELP_TABLE_CODE_STYLE = _HELP_TABLE_CELL_STYLE + " font-family:monospace; white-space:pre;"

# Catalog-specific: printed once above %catalog help's table, not per-row,
# since it applies across every path/source_path/target_path. %ingest help
# has no equivalent note.
_CATALOG_HELP_NOTE = (
    "A new session's context starts at 'catalog' (the root source), not empty — "
    "use(None)/use('') reset back to it too; only set_context(None) clears it to no "
    "context at all.\n"
    "Every path/source_path/target_path (except use's own) is either absolute or "
    "relative to the current context, once one is set:\n"
    "  - starts with 'catalog.' (this deployment's one real root source) -> always "
    "absolute, used exactly as given, never appended to the context\n"
    "  - '.' or './' alone -> the context itself, exactly as get_context() shows it\n"
    "  - '../' (or a bare '..'), optionally chained ('../../') -> walks up that many "
    "levels of the context first, then appends whatever's left, if anything\n"
    "  - '.name', or a bare 'name' with no leading '.' at all -> both mean relative to "
    "the context; the dot is optional sugar, not what makes it relative\n"
    "Only use() ever changes the context — no other call does, even one that fully "
    "resolved an absolute path."
)


def _help_table_html(rows: list[tuple[str, str, str]]) -> str:
    """Build the `<table>` markup shared by `%catalog help`/`%ingest help` —
    inline styles only (no external stylesheet, no hardcoded background/text
    color — just `currentColor` borders) so it reads correctly in both a
    light and a dark notebook theme without knowing which one is active."""

    def th(text: str) -> str:
        return f'<th style="{_HELP_TABLE_HEADER_STYLE}">{_escape(text)}</th>'

    def td(text: str, *, code: bool = False) -> str:
        style = _HELP_TABLE_CODE_STYLE if code else _HELP_TABLE_CELL_STYLE
        return f'<td style="{style}">{_escape(text)}</td>'

    head = f"<tr>{th('Command')}{th('Parameters')}{th('Description')}</tr>"
    body = "".join(
        f"<tr>{td(name, code=True)}{td(params, code=True)}{td(description)}</tr>"
        for name, params, description in rows
    )
    return f'<table style="border-collapse:collapse;">{head}{body}</table>'


def _print_help_table(
    cls: type, methods: list[tuple[str, str]], label: str, *, note: str | None = None
) -> None:
    """`%catalog help`/`%ingest help` — a real HTML `<table>` (Command /
    Parameters / Description), rendered via `IPython.display` rather than a
    raw Python-signature listing: both magics' audience is a data custodian
    reading them in JupyterLab, not necessarily someone comfortable with a
    Python type signature or a monospace grid. Only `help` output gets the
    rich-display treatment — everything else in this module stays plain
    `print()` (see the module docstring's "eval() below runs exactly the
    Python the user typed" — errors and usage lines are meant to read like
    ordinary interpreter output, not a UI)."""
    print(f"%{label} methods — usage: {_USAGE[label]}")
    if note:
        print(note)

    rows = [(name, _plain_params(getattr(cls, name)), description) for name, description in methods]
    display(HTML(_help_table_html(rows)))


_CALL = re.compile(r"\s*([A-Za-z_]\w*)\s*\((.*)\)\s*", re.DOTALL)
_CALLEE = re.compile(r"\s*([A-Za-z_]\w*)\s*\(")
_KEYWORD_ARG = re.compile(r"\s*([A-Za-z_]\w*)\s*=(?!=)")


def _split_args(text: str) -> list[str] | None:
    """The top-level, comma-separated arguments of a call, as written.

    Commas inside brackets or string literals don't split. ``None`` if the
    brackets or quotes don't balance — nothing useful can be said then.
    """
    args: list[str] = []
    depth = 0
    quote: str | None = None
    start = 0
    i = 0
    while i < len(text):
        char = text[i]
        if quote:
            if char == "\\":
                i += 1
            elif char == quote:
                quote = None
        elif char in "\"'":
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth -= 1
            if depth < 0:
                return None
        elif char == "," and depth == 0:
            args.append(text[start:i].strip())
            start = i + 1
        i += 1
    if quote or depth:
        return None
    last = text[start:].strip()
    if last:
        args.append(last)
    return args


def _fits(annotation: Any, value_text: str) -> bool:
    """Whether the literal ``value_text`` could be a value for ``annotation``.

    Anything that isn't a literal (a variable name, an expression) could be
    anything, so it fits every parameter.
    """
    try:
        value = ast.literal_eval(value_text)
    except (ValueError, SyntaxError):
        return True
    if annotation is inspect.Parameter.empty:
        return True
    allowed = {part.strip() for part in str(annotation).split("|")}
    return type(value).__name__ in allowed or (value is None and "None" in allowed)


# Commands whose every error also prints the correct form of the call — the
# ones with several positional parameters, where a slot mix-up is the likely
# cause. A call that doesn't fit a method's signature shows it for any command.
_USAGE_ON_ERROR = frozenset({"create_version", "draft_to_version"})


def _called_method(session: Any, line: str) -> Any | None:
    """The session method `line` calls (``name(...)``), or ``None``."""
    callee = _CALLEE.match(line)
    method = getattr(type(session), callee.group(1), None) if callee else None
    return method if callable(method) else None


def _usage_for(session: Any, line: str) -> str | None:
    """``usage: name(params)`` for the session method `line` calls, if known."""
    method = _called_method(session, line)
    if method is None:
        return None
    return f"usage: {method.__name__}({_plain_params(method)})"


def _is_call_shape_error(exc: TypeError) -> bool:
    """Whether `exc` came from the call itself not fitting the method's
    signature (unknown keyword, too many / missing arguments), rather than
    from inside the method: the traceback then ends in the evaluated magic
    line, never entering the method's own code."""
    tb = exc.__traceback__
    if tb is None:
        return False
    while tb.tb_next is not None:
        tb = tb.tb_next
    return tb.tb_frame.f_code.co_filename == "<string>"


def _explain_syntax_error(session: Any, line: str, exc: SyntaxError) -> str:
    """A magic line that isn't valid Python, explained against the method it calls.

    For the commonest slip — an unnamed argument after a ``name=value`` one —
    names the offending argument and suggests the parameters it could be;
    always ends with the correct form of the call when the method is known.
    """
    method = _called_method(session, line)
    if method is None:
        return f"invalid syntax: {exc.msg}"
    usage = f"usage: {method.__name__}({_plain_params(method)})"
    match = _CALL.fullmatch(line)
    args = _split_args(match.group(2)) if match else None
    if args:
        keywords = [_KEYWORD_ARG.match(arg) for arg in args]
        first_keyword = next((i for i, kw in enumerate(keywords) if kw), None)
        stray = next(
            (i for i, kw in enumerate(keywords) if first_keyword is not None
             and i > first_keyword and kw is None),
            None,
        )
        if first_keyword is not None and stray is not None:
            named = {kw.group(1) for kw in keywords if kw}
            params = [
                p for name, p in inspect.signature(method).parameters.items() if name != "self"
            ]
            candidates = [
                f"{p.name}={args[stray]}"
                for p in params[first_keyword:]
                if p.name not in named and _fits(p.annotation, args[stray])
            ]
            hint = f" Did you mean {' or '.join(candidates)}?" if candidates else ""
            return (
                f"argument {stray + 1} ({args[stray]}) has no name, but comes after "
                f"{args[first_keyword]} — once an argument is given by name, every argument "
                f"after it needs a name too.{hint}\n{usage}"
            )
    return f"invalid syntax: {exc.msg}\n{usage}"


_DISPATCH_FAILED = object()
# Sentinel `_dispatch` returns when it printed a friendly error — distinct
# from a legitimate `None` result (e.g. `use(...)` printing its own status
# instead of an empty commit report, or `get_context()` with nothing set
# yet). `%%catalog`'s own loop needs to tell those apart to know whether to
# keep running the rest of the cell;
# every caller converts this back to a plain `None` before it reaches
# IPython, so it's never actually displayed.


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
            report = session.commit(retry=True)
            if report.succeeded:
                result = report
            else:
                # use()/set_context() also chain back to `self`, but queue
                # nothing — commit() then has nothing to report, so print
                # the context they just set instead of an empty CommitReport.
                print(f"context set to {session.get_context()!r}")
                result = None
    except SyntaxError as exc:
        # The line itself isn't valid Python, so the call never happened.
        print(f"{label} error: {_explain_syntax_error(session, line, exc)}")
        return _DISPATCH_FAILED
    except error_type as exc:
        print(f"{label} error: {exc}")
        callee = _CALLEE.match(line)
        if callee and callee.group(1) in _USAGE_ON_ERROR:
            print(_usage_for(session, line))
        return _DISPATCH_FAILED
    except TypeError as exc:
        usage = _usage_for(session, line)
        if usage is None or not _is_call_shape_error(exc):
            raise  # a bug inside the method: keep the traceback
        print(f"{label} error: {exc}\n{usage}")
        return _DISPATCH_FAILED
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
    """Registers `%catalog`, `%ingest`, `%sdi` and `%metadata` — see this module's docstring."""

    def __init__(self, shell: Any) -> None:
        super().__init__(shell)
        self._catalog_session: CatalogSession | None = None
        self._ingest_session: IngestSession | None = None
        self._sdi_session: SdiSession | None = None
        self._metadata_session: MetadataSession | None = None
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
            _print_help_table(CatalogSession, _CATALOG_HELP, "catalog", note=_CATALOG_HELP_NOTE)
            return None
        if not self._ensure_catalog_session():
            return None
        result = _dispatch(
            self._catalog_session,
            line,
            self.shell.user_ns,
            "catalog",
            CatalogSessionError,
            auto_commit=True,
        )
        return None if result is _DISPATCH_FAILED else result

    @cell_magic("catalog")
    def catalog_cell(self, line: str, cell: str) -> Any:
        """`%%catalog` — one call per line, in order, typically opened with
        `use(path)` to set the context once for the whole cell (see
        `CatalogSession.use`) instead of repeating a path on every line::

            %%catalog use("bwd.reference")
            set_tags(".water_temperature", ["reviewed"])
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
            if result is _DISPATCH_FAILED:
                return None  # a friendly error was already printed by _dispatch
        return result

    @line_magic
    def ingest(self, line: str) -> Any:
        if _is_help(line):
            _print_help_table(IngestSession, _INGEST_HELP, "ingest")
            return None
        if self._ingest_session is None:
            self._ingest_session = IngestSession()
        result = _dispatch(
            self._ingest_session, line, self.shell.user_ns, "ingest", IngestSessionError
        )
        return None if result is _DISPATCH_FAILED else result

    @line_magic
    def sdi(self, line: str) -> Any:
        """`%sdi` — read ISO 19115-3 metadata from SDI, each call runs immediately::

            %sdi get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")
            %sdi resolve_series("c3858959-90da-4c1b-b9ca-492db0e514df")
        """
        if _is_help(line):
            _print_help_table(SdiSession, _SDI_HELP, "sdi", note=_SDI_HELP_NOTE)
            return None
        if self._sdi_session is None:
            self._sdi_session = _build_sdi_session()
        result = _dispatch(self._sdi_session, line, self.shell.user_ns, "sdi", SdiSessionError)
        return None if result is _DISPATCH_FAILED else result

    @line_magic
    def metadata(self, line: str) -> Any:
        """`%metadata` — push metadata files to DDS, each call runs immediately::

            %metadata push_to_dds("catalog/water_management_resources/bathing_water/bwd")
            %metadata dds_base_url()

        `push_to_dds` uploads the record `%sdi get_xml` fetched last unless
        `metadata=` is given.
        """
        if _is_help(line):
            _print_help_table(
                MetadataSession, _METADATA_HELP, "metadata", note=_METADATA_HELP_NOTE
            )
            return None
        if self._metadata_session is None:
            # Looked up at push time, so a get_xml run after %metadata started still counts.
            self._metadata_session = _build_metadata_session(
                lambda: self._sdi_session.last if self._sdi_session is not None else None
            )
        result = _dispatch(
            self._metadata_session, line, self.shell.user_ns, "metadata", MetadataSessionError
        )
        return None if result is _DISPATCH_FAILED else result


def load_ipython_extension(ipython: Any) -> None:
    # A no-op if `eea_datalakehouse.notebook`'s own auto-import already
    # registered this (see the module docstring) — registering again would
    # replace the live instance with a fresh one, silently dropping the
    # CatalogSession's "current path" context and any already-queued-but-
    # not-committed IngestSession state.
    if "EEALakehouseMagics" in ipython.magics_manager.registry:
        return
    ipython.register_magics(EEALakehouseMagics)
