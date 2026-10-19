"""`%catalog`/`%%catalog`, `%ingest`, `%sdi`, `%dds` and `%gpkg` — thin syntactic
sugar over `CatalogSession`/`IngestSession`/`SdiSession`/`DdsSession`/`GpkgSession` (see
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
    %ingest diagnose
    %ingest commit(retry=True)

    %catalog use("catalog.water_management_resources.bathing_water.bwd")
    %dds push(DELIVERABLES, "report.pdf", "annex/*.xlsx")
    record = %sdi get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")
    %dds push(METADATA, record)

`%sdi` (`eea_datalakehouse.sdi.session.SdiSession`) only reads the SDI
catalogue; `%dds` (`eea_datalakehouse.dds_documents.session.DdsSession`)
uploads files to one of the `%catalog` context's document folders in DDS
(`DdsFolder`: DERIVED, METADATA, METHODOLOGY, ASSESSMENTS, ANALYSES,
REFERENCES, DELIVERABLES, OTHERS — compulsory, typed bare on a `%dds` line). Both run each call
immediately, like `%catalog`, and are built on first use, like `%ingest`'s
session, from the kernel environment (see each module's docstring for the
variables).

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
from ..dds_documents.controller import DdsFolder, PushReport
from ..dds_documents.session import DdsSession, DdsSessionError
from ..dds_ingestion.session import IngestSession, IngestSessionError
from ..geopackage.session import GpkgSession, GpkgSessionError
from ..sdi.session import SdiSession, SdiSessionError
from ..session_utils import plain_param

_USAGE = {
    "catalog": (
        '%catalog data_copy("a.b", "c.d")  |  %catalog set_tags("a.b", ["reviewed"])'
        "  (each call runs immediately)"
    ),
    "ingest": (
        '%ingest ingest(folder="./data", target_catalog_path="a.b", data_format="parquet")'
        "  |  %ingest diagnose  |  %ingest commit"
    ),
    "sdi": '%sdi get_xml("<uuid>")  |  %sdi resolve_series("<series uuid>")',
    "gpkg": (
        '%gpkg export()  |  %gpkg export(tables=["stations"], limit=1000, srid=3035)'
        "  (the %catalog use(...) folder, into the notebook's folder)"
    ),
    "dds": (
        '%dds push(DELIVERABLES, "report.pdf")  |  %dds push(METADATA, "a.xml", "b.xml")'
        "  (into the %catalog use(...) folder)"
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
        "tables as in create_version: omitted or empty copies everything in draft. Unlike "
        "create_version, views in draft are not skipped: each is copied into the version as a "
        "table (a snapshot of what it returns); the report's notes say which.",
    ),
    (
        "update_latest_release",
        "Create a view at the Area's root for every table in versions.version_name, "
        "keeping its sub-folders (versions.v1.a.t gets the view a.t; missing folders are "
        "created). The context must be the Area folder — one with draft and versions folders "
        "just below it, typically catalog.<domain>.<subdomain>.<area> — "
        "and versions.version_name must exist — raises at once otherwise. Views in the version "
        "are skipped. An existing view "
        "is dropped and recreated over the version (overwrite=True, the default; not undoable) — "
        "overwrite=False makes it an error instead; an existing table is "
        "always an error.",
    ),
    # -- table ----------------------------------------------------------------
    (
        "list",
        "Show every table/view under path as a JSON array of {type, name} — type TABLE or "
        "VIEW, name the full dot-separated path: its own, then every child folder's, at any "
        "depth — walked folder by folder "
        "through Dremio's catalog API, no SQL. path may use '.' or '/' separators. "
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
    (
        "ingest",
        "Queue one folder ingest (see FolderIngest for what each argument means). "
        "upload_mode='proxy' when S3 is unreachable; csv_options={'delimiter': ';'}; "
        "table_definition={'columns': [{'name': 'value', 'type': 'DOUBLE'}]} for the column "
        "types of a CSV/JSON table.",
    ),
    (
        "diagnose",
        "Check every queued ingest without running it: DDS, your credential, the file "
        "sizes, DDS's storage, and a dry run. Nothing is uploaded; the queue is kept.",
    ),
    ("commit", "Run every queued ingest in order; stops at the first failure."),
]
_SDI_HELP = [
    (
        "get_xml",
        "Download the ISO 19115-3 XML of SDI record uuid and check it really is that record. "
        "Runs immediately; needs no DDS settings. Push it with "
        "record = %sdi get_xml(uuid), then %dds push(METADATA, record) (stored as <uuid>.xml).",
    ),
    (
        "resolve_series",
        "Return the UUID of the one release of series_uuid that is not superseded; raises, "
        "listing every release, if there are zero or several.",
    ),
]
_SDI_HELP_NOTE = (
    "Environment: SDI_API_URL (empty means the public EEA catalogue; optional "
    "SDI_USERNAME/SDI_PASSWORD for non-public records). Pushing to DDS is %dds's job."
)
_DDS_HELP = [
    (
        "push",
        "Upload files to path/folder/ in DDS with PUT, one request per file — a document "
        "upload, never an ingest. path defaults to the %catalog use(...) context and is "
        "resolved like any %catalog path (absolute, or relative to the context). folder is "
        "compulsory and must be one of DDS's document folders, typed bare (no quotes): DERIVED, "
        "METADATA, METHODOLOGY, ASSESSMENTS, ANALYSES, REFERENCES, DELIVERABLES, OTHERS. "
        "files are one or more, given one after another or as a list: local paths (globs like "
        '"annex/*.pdf" too), %sdi get_xml records (stored as <uuid>.xml) or %gpkg export '
        "reports (the .gpkg written). Every file is "
        "read and checked before the first upload. target_name renames files in DDS: a string "
        'for a single file (target_name="bwd_2025.xml"), or a dict from each file\'s own name to '
        'its new name (target_name={"draft_v3.pdf": "report_2025.pdf"}); files left out, or an '
        "empty target_name, keep their own name. "
        "overwrite=True (default) replaces a file "
        "already there; overwrite=False uploads nothing if any is.",
    ),
    ("target", "Show the DDS folder push would upload to, without uploading anything."),
    ("folders", "List the folders push accepts."),
    ("dds_base_url", "Show the DDS URL push uses, and where it came from."),
]
_DDS_HELP_NOTE = (
    "Files go to <dataset folder>/<folder>/<file name>, e.g. after\n"
    '  %catalog use("catalog.water_management_resources.bathing_water.bwd")\n'
    '  %dds push(DELIVERABLES, "report.pdf")\n'
    "-> catalog/water_management_resources/bathing_water/bwd/deliverables/report.pdf\n"
    "\n"
    "push examples:\n"
    "  files — one or more, one after another, as a list, or as a glob:\n"
    '    %dds push(DELIVERABLES, "report.pdf", "annex.xlsx")\n'
    '    %dds push(DELIVERABLES, ["report.pdf", "annex.xlsx"])\n'
    '    %dds push(ANALYSES, "results/*.csv")\n'
    "  a GeoPackage from %gpkg export (the report is the file):\n"
    "    report = %gpkg export()\n"
    "    %dds push(DELIVERABLES, report)\n"
    "  an SDI record from %sdi get_xml (stored as <uuid>.xml):\n"
    '    record = %sdi get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")\n'
    "    %dds push(METADATA, record)\n"
    "  folder — compulsory, one of DERIVED, METADATA, METHODOLOGY, ASSESSMENTS, ANALYSES,\n"
    '  REFERENCES, DELIVERABLES, OTHERS, typed bare (a string like "methodology" is refused):\n'
    '    %dds push(METHODOLOGY, "method.docx")\n'
    "  path= — another dataset than the %catalog context, absolute or relative to it:\n"
    '    %dds push(DELIVERABLES, "report.pdf", path="catalog.water_management_resources.wfd")\n'
    '    %dds push(DELIVERABLES, "report.pdf", path="../wfd")\n'
    "  overwrite= — True (default) replaces a file already there;\n"
    "  False uploads nothing if any is:\n"
    "    %dds push(METADATA, record, overwrite=False)\n"
    "  target_name= — another name in DDS: a string for one file, a dict for several\n"
    "  (files left out keep their name):\n"
    '    %dds push(METADATA, record, target_name="bwd_2025.xml")\n'
    '    %dds push(DELIVERABLES, "draft_v3.pdf", "annex.xlsx", '
    'target_name={"draft_v3.pdf": "report_2025.pdf"})\n'
    "  all together:\n"
    '    %dds push(DELIVERABLES, "draft_v3.pdf", "annex/*.xlsx", path="../wfd", '
    'overwrite=False, target_name={"draft_v3.pdf": "report_2025.pdf"})\n'
    "  (a %dds call is one line — it cannot continue on the next one)\n"
    "  check first, without uploading:  %dds target(DELIVERABLES)  or  "
    '%dds target(DELIVERABLES, path="../wfd")\n'
    "\n"
    "Environment: DDS_BASE_URL — read from the first .env found in the notebook's folder or "
    "a parent when %dds starts, else from the kernel environment — and a Dremio identity: "
    "_DREMIO_USER/_DREMIO_PWD (as %ingest) or DREMIO_USERNAME/DREMIO_TOKEN (as %catalog)."
)
# Folder names usable bare on a %dds line: `%dds push(METADATA, ...)`. They
# win over a notebook variable of the same name, since folder must be one of
# them (see _dispatch's extra_ns).
_DDS_NAMESPACE: dict[str, Any] = {
    "DdsFolder": DdsFolder,
    **{member.name: member for member in DdsFolder},
}


_GPKG_HELP = [
    (
        "export",
        "Write the tables and views under path to a GeoPackage file: spatial ones as feature "
        "layers, the rest as attribute tables. path defaults to the %catalog use(...) context "
        "and is resolved like any %catalog path. tables picks some, by name below path "
        '(["stations", "sub.areas"]); default all, at any depth. limit caps the rows per table. '
        'srid reprojects every layer (3035 or "EPSG:3035"); default: each keeps its own CRS. '
        "filename / output_dir default to <dataset>.gpkg in the notebook's folder; overwrite=True "
        "(default) replaces an existing file. workers tables are streamed from Dremio at once, "
        "each over its own Arrow Flight channel.",
    ),
]
_GPKG_HELP_NOTE = (
    "A table is spatial when a column holds geometries — WKB/EWKB, WKT/EWKT, hex WKB or "
    "GeoJSON (columns named geom, geometry, wkt, shape, ... are tried first) — or, failing "
    "that, it has latitude/longitude columns (points in EPSG:4326).\n"
    "Each row's CRS: the SRID inside the geometry (EWKB/EWKT), else a CRS column (srid, sr_id, "
    'epsg, crs, ...: 3035, "EPSG:3035", a URN or a CRS name). A layer with no CRS at all is '
    "written with the undefined SRS (-1).\n"
    "Streaming: rows are read over Arrow Flight a batch at a time — workers tables at once, "
    "each on its own channel — decoded and encoded in those workers, and written by one "
    "thread; memory stays a few batches per worker whatever the table size.\n"
    "Progress: a line as each table/view is written (✓ rows, seconds) or fails (✗ with the "
    "table's name — the export then stops and no file is written), and a total at the end.\n"
    "\n"
    "export examples:\n"
    "    %gpkg export()\n"
    '    %gpkg export(tables=["stations", "areas"])\n'
    "    %gpkg export(limit=1000)\n"
    "    %gpkg export(srid=3035)\n"
    "    %gpkg export(workers=8)\n"
    '    %gpkg export(path="../wfd", filename="wfd_2025.gpkg", output_dir="exports")\n'
    '    %gpkg export(path="catalog.water.bwd", tables=["stations"], limit=100, srid="EPSG:4326", '
    "overwrite=False, workers=2)\n"
    "\n"
    "then push the file to DDS — the returned report is the file:\n"
    "    report = %gpkg export()\n"
    "    %dds push(DELIVERABLES, report)\n"
    "\n"
    "Reads with %catalog's connection: DREMIO_BASE_URL / DREMIO_TOKEN. Written with sqlite3, "
    "shapely and pyproj (no GDAL)."
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


def _build_gpkg_session(resolve_path: Any, catalog: Any) -> GpkgSession:
    return GpkgSession(resolve_path=resolve_path, catalog=catalog)


def _build_dds_session(resolve_path: Any) -> DdsSession:
    # DdsSession reads DDS_BASE_URL from the nearest .env right here, when it
    # is built; a missing DDS setting or Dremio identity prints as a friendly
    # "dds error" on the first push.
    return DdsSession(resolve_path=resolve_path)


def _is_help(line: str) -> bool:
    return line.strip() in ("help", "help()")


def _plain_params(func: Any) -> str:
    """Comma-separated parameter names (minus `self`), for a non-developer
    reading `%catalog help`'s/`%ingest help`'s table — no Python type-hint syntax (a bare
    `str | None` union means nothing to a data custodian, and would collide
    visually with the table's own `|` column separators anyway) and no `*`
    keyword-only marker. A parameter with a default is shown as `name=default`
    so it still reads as optional, one taking any number of values (`*name`)
    as `name...`; everything else is required, in order."""
    sig = inspect.signature(func)
    parts = [plain_param(name, param) for name, param in sig.parameters.items() if name != "self"]
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
            (
                i
                for i, kw in enumerate(keywords)
                if first_keyword is not None and i > first_keyword and kw is None
            ),
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
                if p.name not in named
                and p.kind is not inspect.Parameter.VAR_POSITIONAL
                and _fits(p.annotation, args[stray])
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
    extra_ns: dict[str, Any] | None = None,
) -> Any:
    line = line.strip()
    if not line:
        print(f"usage: {_USAGE[label]}")
        return None
    if line.isidentifier():  # bare "commit" (no parens) as a convenience
        line = f"{line}()"
    namespace = {**user_ns, **(extra_ns or {}), "__session__": session}
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


_DDS_PUSHED_COMM_TARGET = "eea_datalakehouse.dds_pushed"


def _notify_dds_pushed(shell: Any, dds_path: str) -> None:
    """Tell an integrated frontend (the JupyterLab Document Service panel,
    in the separate `eeadata/EEALakeHouse` repo) that `%dds push` just
    wrote a file, so the panel can refresh immediately instead of waiting
    for its next poll tick. Called once per file pushed.

    Opposite direction from `_register_context_comm`: here the kernel opens
    the comm rather than reacting to one. Best-effort and silent either
    way — no live kernel comm_manager, or a frontend that never registered
    `_DDS_PUSHED_COMM_TARGET` (no matching extension, or one without this
    feature), both just leave the frontend as it was.
    """
    comm_manager = getattr(getattr(shell, "kernel", None), "comm_manager", None)
    if comm_manager is None:
        return
    try:
        from comm import create_comm

        create_comm(target_name=_DDS_PUSHED_COMM_TARGET, data={"dds_path": dds_path}).close()
    except Exception:
        pass


@magics_class
class EEALakehouseMagics(Magics):
    """Registers `%catalog`, `%ingest`, `%sdi`, `%dds` and `%gpkg` — see this module's docstring."""

    def __init__(self, shell: Any) -> None:
        super().__init__(shell)
        self._catalog_session: CatalogSession | None = None
        self._ingest_session: IngestSession | None = None
        self._sdi_session: SdiSession | None = None
        self._dds_session: DdsSession | None = None
        self._gpkg_session: GpkgSession | None = None
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

    def _context_target(self, path: str | None) -> str | None:
        """The catalog path `%dds` pushes under and `%gpkg` exports: `path` resolved like any
        `%catalog` path, or the current context when `path` is `None` —
        including one a frontend pushed before any `%catalog` cell ran."""
        session = self._catalog_session
        if path is None:
            return session.get_context() if session is not None else self._pending_context
        if session is None:
            return path
        try:
            return session._resolve_path(path)  # noqa: SLF001 — same resolution as %catalog
        except CatalogSessionError as exc:
            raise ValueError(str(exc)) from exc

    @line_magic
    def dds(self, line: str) -> Any:
        """`%dds` — upload files to the `%catalog` context's DDS folders, each
        call runs immediately::

            %dds push(DELIVERABLES, "report.pdf", "annex/*.xlsx")
            %dds push(METADATA, record, path="catalog.water.bwd", overwrite=False)
            %dds target(ASSESSMENTS)
        """
        if _is_help(line):
            _print_help_table(DdsSession, _DDS_HELP, "dds", note=_DDS_HELP_NOTE)
            return None
        if self._dds_session is None:
            # Looked up at push time, so a `use` run after %dds started still counts.
            self._dds_session = _build_dds_session(self._context_target)
        result = _dispatch(
            self._dds_session,
            line,
            self.shell.user_ns,
            "dds",
            DdsSessionError,
            extra_ns=_DDS_NAMESPACE,
        )
        if isinstance(result, PushReport):
            for dds_path in result.paths:
                _notify_dds_pushed(self.shell, dds_path)
        return None if result is _DISPATCH_FAILED else result

    def _catalog_for_gpkg(self) -> Catalog:
        """`%catalog`'s own Catalog, building its session if need be."""
        if self._catalog_session is None:
            self._catalog_session = _build_catalog_session()  # raises without credentials
            if self._pending_context is not None:
                self._catalog_session.set_context(self._pending_context)
                self._pending_context = None
        return self._catalog_session._catalog  # noqa: SLF001 — one login for both magics

    @line_magic
    def gpkg(self, line: str) -> Any:
        """`%gpkg` — export the `%catalog` context's tables and views to a
        GeoPackage file, streamed from Dremio, each call runs immediately::

            %gpkg export()
            %gpkg export(tables=["stations", "areas"], limit=1000, srid=3035)
            %gpkg export(path="../wfd", filename="wfd_2025.gpkg", output_dir="exports")
        """
        if _is_help(line):
            _print_help_table(GpkgSession, _GPKG_HELP, "gpkg", note=_GPKG_HELP_NOTE)
            return None
        if self._gpkg_session is None:
            self._gpkg_session = _build_gpkg_session(self._context_target, self._catalog_for_gpkg)
        result = _dispatch(self._gpkg_session, line, self.shell.user_ns, "gpkg", GpkgSessionError)
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
