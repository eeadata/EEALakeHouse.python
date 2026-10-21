# EEADataLakehouse

Two main class domains for the EEA data lakehouse:

- `eea_datalakehouse.dds_ingestion` — notebook-side client for the Dremio Document
  Service (DDS) Ingest API (DI-8.4/8.5). `FolderIngest` transfers a folder of
  data files to S3 (via DDS-issued presigned URLs only — never an S3 SDK or
  S3 credentials) and registers it as a Dremio table, coordinated entirely
  through the DDS REST API.
- `eea_datalakehouse.catalog` — Dremio catalog operations once data has
  landed: converting a table to a view, copying/moving/deleting catalog
  entries, managing folders, wiki text and tags, and listing what's there.
  `datacopy`/`datamove` always run over Arrow Flight SQL; everything else
  runs over Dremio's REST SQL Jobs API (folders/wiki/tags have no SQL
  equivalent at all, so those go through Dremio's own catalog REST API
  directly) — and every operation treats a stalled call as a Dremio engine
  cold-starting rather than a failure, so it can be remembered and retried
  later instead of blocking.

## Install

[![main](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fapi.github.com%2Frepos%2Feeadata%2FEEALakeHouse.python%2Freleases&query=%24%5B%3F%28%40.prerelease%3D%3Dfalse%29%5D.tag_name&label=main&color=blue)](https://github.com/eeadata/EEALakeHouse.python/releases/latest)
[![staging](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fapi.github.com%2Frepos%2Feeadata%2FEEALakeHouse.python%2Freleases&query=%24%5B%3F%28%40.prerelease%3D%3Dtrue%29%5D.tag_name&label=staging&color=orange)](https://github.com/eeadata/EEALakeHouse.python/releases)

These badges are live — each one queries the GitHub API directly and always shows whatever tag
is *currently* released for that branch, updating on its own every time `main`/`staging` cuts a
new release (see [Releasing a new version](#releasing-a-new-version)).

`@main`/`@staging` always installs whatever was most recently released for that branch — a
floating tag sharing the branch's own name, moved forward to the latest release automatically
each time one is cut, so there's nothing to look up or keep in sync yourself:

```bash
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@main"       # latest stable
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@staging"    # latest early access
```

# staging's latest release (early access) — pin to the tag the "staging" badge above shows
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.35-staging"
```

## Usage

Dremio credentials are read from the injected kernel env vars `_DREMIO_USER` /
`_DREMIO_PWD`, and the service URL from `DDS_BASE_URL`. None of these are ever
logged or printed.

```python
from eea_datalakehouse.dds_ingestion import FolderIngest

outcome = FolderIngest(
    folder="./my_data",
    target_catalog_path="biodiversity.uploads",
    data_format="parquet",      # one of parquet | csv | json
    intent="read_only",         # or "editable"
    conflict_mode="fail",
    parallelism=4,               # concurrent uploads (default 4)
).run()

print(outcome.commit.table_path, outcome.commit.record_count)
```

`run()` performs `begin → upload(all files) → commit`. Re-running the same
session resumes by skipping files the server reports as already uploaded.

```python
from eea_datalakehouse.catalog import Catalog, EngineStartingError

# base_url/token are Dremio's own (not DDS). `username` is only needed for
# datacopy/datamove (Arrow Flight's basic-auth handshake) — everything else
# runs over Dremio's REST SQL Jobs API regardless of EEA_CATALOG_TRANSPORT.
with Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME) as catalog:
    try:
        catalog.table2view(
            "bwd.consumer", "bwd.draft.bw_assessment", idempotency_key="bwd-v2025_1"
        )
    except EngineStartingError:
        # A cold Dremio engine looks like a stalled call, not a failure — the
        # attempt is remembered under its idempotency_key; call it again later:
        catalog.retry_pending("bwd-v2025_1")

    info = catalog.gettableitemsfrom("bwd.versions.v2025_1.assessments", idempotency_key="check-1")
    print(info.schema, info.row_count)             # schema + row count, no rows fetched

    catalog.gettablesfrom("bwd", idempotency_key="list-1")   # recurses into every subfolder
# `with` calls catalog.close() on exit, disposing the REST/Flight session(s).
# If you don't use `with`, call catalog.close() yourself when done — a
# process-exit/SIGTERM fallback covers a caller that forgets either way.
```

Every operation takes `idempotency_key=` (keyword-only). On `EngineStartingError` — a stalled
call, most likely a cold-starting Dremio engine, not a real failure — the attempt is remembered
under that key; call `catalog.retry_pending(idempotency_key)` later to pick it back up with the
exact same arguments.

### Catalog operations

**Table/view lifecycle**
- `table2view(view_path, source_path, create_target_folder=True)` — the one-time `DROP TABLE` +
  `CREATE VIEW` transition for a location that started as a physical table (straight off ingest)
  and is moving to being a view. Checks `source_path` exists first; by default also creates
  `view_path`'s containing folder if missing.
- `createview(source_path, target_path, overwrite=False, create_target_folder=False)` —
  `CREATE VIEW ... AS SELECT * FROM source_path`, leaving `source_path` untouched (unlike
  `datamove`). No data of its own — a view is a saved query — so this runs over REST like
  every other metadata-only operation, not Arrow Flight. Same folder-append and `overwrite`
  behavior as `datacopy`/`datamove`.
- `createversion(source_path, version_name, tables=None, target_path=..., overwrite=False,
  create_target_folder=True)` — copies the listed `tables` into a new folder
  `target_path.version_name`, one `datacopy` snapshot each, keeping their sub-folders.
  `source_path` and `version_name` must not be blank; `tables` omitted or empty copies everything
  under `source_path`. Each entry is a table or a folder (every table under it), named as under
  `source_path` or as a full path. A missing `target_path` is created, every level, unless
  `create_target_folder=False`. Reference and vocabulary tables (a folder named
  `reference(s)`/`vocabulary(ies)` in the path) and views are skipped and listed in
  `VersionResult.skipped` — views are copied as tables instead with `include_views=True`
  (listed in `VersionResult.from_views`). Entries missing from the source (all named at once) or an existing
  version folder without `overwrite=True` raise before anything is written. On failure it
  removes what it created. `%catalog create_version(source_path, version_name, tables)` uses the
  current context as `target_path` when it's omitted.
- `%catalog draft_to_version(version_name, tables=None, overwrite=False)` — `create_version`
  from the dataflow's `draft` folder without typing any path: the source is the `draft` folder
  the context is in (or the `draft` inside the context), the target is
  `{draft's parent}.versions.{version_name}` (`versions` is created if missing). `tables` works
  as in `create_version`. Unlike `create_version`, views in `draft` are not skipped: each one is
  copied into the version as a table — a snapshot of what the view returns — and the commit
  report's notes list them.
- `%catalog update_latest_release(version_name, overwrite=True)` — a view at the Area's root for
  every table in `{context}.versions.{version_name}`, keeping its sub-folders
  (`versions.v1.eu_sdg_06_30.monitoringsites` gets the view `eu_sdg_06_30.monitoringsites`;
  missing folders are created). The context, set with `use`, must be the Area folder — one with
  `draft` and `versions` folders just below it, typically
  `catalog.<domain>.<subdomain>.<area>` — and `versions.{version_name}` must exist —
  checked when the call is made. Views in the version
  are skipped. Every target is checked before any view is created: an existing view is dropped
  and created again over the version (`overwrite=True`, the default — re-pointing the views at a
  new version is the normal use; it can't be undone), `overwrite=False` makes it an error
  instead, and an existing table is always an error.
- `draft2version(draft_path, version_path)` / `publishversion(consumer_view_path, version_path)`
  — **not implemented yet** (both raise `NotImplementedError`); promoting a draft table into a
  permanent version and repointing a consumer-facing view at it.
- `createviewas(view_path, sql, overwrite=False, create_target_folder=True)` — `table2view`'s
  general form: creates a view from any query body you hand it, not just `SELECT * FROM <source>`.
  The SQL is sent through unchanged.
- `deleteview(view_path)` — `DROP VIEW IF EXISTS`.
- `deletetable(table_path)` — `DROP TABLE IF EXISTS`; `deleteview`'s counterpart.

**Copying/moving data** — always over Arrow Flight SQL, never REST, regardless of
`EEA_CATALOG_TRANSPORT` (needs `username=` on `Catalog(...)` for the Flight auth handshake):
- `datacopy(source_path, target_path, overwrite=False, create_target_folder=False)` — `CREATE
  TABLE ... AS SELECT`. If `target_path` is an existing *folder* rather than a specific
  table/view path, the source's own name is appended to it (`cp source dest/` semantics).
  `overwrite=False` (the default) raises if the (possibly folder-adjusted) target already
  exists; `overwrite=True` detects whatever is actually there — it might be a view, not a table
  — and drops it with the matching verb first.
- `datamove(source_path, target_path, entry_type=None, overwrite=False,
  create_target_folder=False)` — copies then drops the source. `entry_type` (`"TABLE"`/`"VIEW"`)
  is auto-detected via `INFORMATION_SCHEMA` when not given, rather than trusting a caller to get
  it right. Same folder-append and `overwrite` behavior as `datacopy`. After the move, verifies
  `target_path` actually exists in the catalog before declaring success, rather than trusting a
  silent CREATE/DROP.

**A whole folder at once** (`catalog/foldertree.py` — orchestration over the operations above,
not one statement each):
- `folderscan(source_path, target_path)` — read-only: every sub-folder, table and view under
  `source_path`, mapped to where it would land under `target_path`, with each view's SQL already
  rewritten and the views in dependency order. Returns a `FolderTree`.
- `foldercopy(source_path, target_path, dry_run=False, overwrite=False, skip_existing=True,
  verify_views=True, copy_metadata=True, tree=None)` — reproduces the subtree and **proves it**:
  folders first (empty ones included), then tables via `datacopy` with `COUNT(*)` compared on
  both sides, then views recreated from their own SQL with every reference into the moved
  subtree remapped to the new location, so the copy is self-contained. Returns a
  `FolderTransferReport`; a per-entry failure is reported, not raised.
- `foldermove(source_path, target_path, ..., delete_source_root=True)` — `foldercopy`, and then
  removes the source **only if `report.verified`**: views in reverse dependency order, then
  tables, then folders deepest-first. Anything less than a clean copy leaves the source exactly
  where it is.
- `remap_view_sql(sql, source_path=..., target_path=..., entries=..., context=...)` — the view
  rewriter on its own, if you want to see what it would do to one definition.

**Discovery**
- `gettablesfrom(schema_path)` — every table/view under `schema_path`, recursing into
  subfolders; returns full dot-separated paths.
- `getentriesfrom(schema_path)` — the same sweep, but each path carries whether it is a
  `"TABLE"` or a `"VIEW"` (`CatalogEntry`), so a folder-level operation knows what each entry is
  without a lookup per path.
- `gettableitemsfrom(table_path)` — one table/view's schema (`{column: DATA_TYPE}`) and row
  count, without fetching any actual rows.
- `entryexists(path)` — whether `path` is a table or view Dremio knows about. Fetches no rows.
- `getviewsql(view_path)` — the SQL a view is defined by, as a `ViewDefinition(sql, context)`.
  Read from Dremio's catalog entity where it answers (which also carries the `sqlContext` the
  SQL was written against), falling back to `INFORMATION_SCHEMA."VIEWS"`, which has the text but
  no context. Raises rather than guessing if neither answers.
- `getfoldersfrom(path, recursive=True)` — the folders under `path`, shallowest first. REST-only:
  folders are not in `INFORMATION_SCHEMA`, so an *empty* folder can only be found this way.

**Wiki & tags** (Dremio's catalog collaboration API — REST-only, no SQL/Flight equivalent):
- `getwikifrom(path)` / `setwikito(path, text, tags=None)` / `deletewiki(path)` — read,
  create/overwrite, or clear the wiki text on a catalog entity. `deletewiki` is idempotent — a
  missing `path`, or one with no wiki at all, is a no-op, not an error (Dremio's collaboration
  API has no separate delete-wiki endpoint, so this clears the text to empty). `tags`, if given
  to `setwikito`, is a list of `{"tag_name", "tag_value", "tag_title"}` dicts rendered into a
  `# Meta Data` section appended to `text` (both a human-readable `title : value` line per tag
  and the same data as `<meta><tag name=... value=... title=.../>...</meta>`) — Dremio's wiki is
  plain markdown with no structured-metadata concept of its own, so this is embedded directly in
  the text.
- `setmeta2wiki(path, tags=None, overwrite=True)` / `getmetafromwiki(path, tag_name=None,
  field=None)` — **folders only** (raises `CatalogOperationError` on a table/view — those have
  Dremio's own tags/labels for this instead). `setmeta2wiki` updates just the `# Meta Data`
  section of the wiki already at `path`, keeping whatever text comes before it untouched:
  `overwrite=True` (the default) replaces the whole section with one built fresh from `tags`;
  `overwrite=False` merges `tags` into whatever tags are already there (parsed back out of the
  existing `<meta>` block), appended after them, with no deduplication. If `path` has no wiki
  yet, starts from empty base text rather than raising. `getmetafromwiki` reads it back:
  without `tag_name` (or if it doesn't match one there), returns every tag as a list; with a
  matching `tag_name`, returns a single `{"tag_name", ...}` dict instead — both `tag_value` and
  `tag_title` if `field` isn't given, or just the one `field` (`"tag_value"`/`"tag_title"`) asks
  for.
- `gettagsfrom(path)` / `settagsto(path, tags)` / `deletetags(path, tags)` — **tables/views
  only** (the mirror image of `setmeta2wiki`/`getmetafromwiki` — raises
  `CatalogOperationError` on a folder, which has no Dremio tags/labels concept of its own). Read
  the full tag list; replace it wholesale; or remove just the given tags, leaving the rest
  untouched.

**Folders** (REST-only, idempotent — an already-there/already-gone folder is not an error):
- `createfolder(path, create_parents=False)` — `create_parents=False` (the default) raises if
  the parent is missing rather than creating a deep new path; `create_parents=True` creates
  every missing level.
- `deletefolder(path, cascade=False)` — `cascade=False` (the default) raises if the folder still
  has contents; `cascade=True` deletes every table/view and subfolder inside first, depth-first.

**Retrying**
- `retry_pending(idempotency_key)` — re-attempt whatever last stalled on an `EngineStartingError`,
  using the same arguments it was originally called with.

**Lifecycle**
- `catalog.close()` (or `with Catalog(...) as catalog:`) — disposes the REST/Flight session(s).
  Idempotent, and covered by a process-exit/SIGTERM fallback if you forget.

## SDI metadata

`eea_datalakehouse.sdi` reads a dataset's ISO 19115-3 metadata record from the EEA SDI
catalogue (GeoNetwork). The calling code always passes the SDI UUID. Storing the record in DDS
is `eea_datalakehouse.dds_documents`'s job (next section): a record is a file it can push, as
`{uuid}.xml`. See `docs/sdi-integration-plan.md`.

```python
from eea_datalakehouse.dds_documents import DdsController, DdsFolder
from eea_datalakehouse.sdi import SdiController

with SdiController.from_env() as sdi, DdsController.from_env() as dds:
    metadata = sdi.get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")   # SDI -> bytes
    dds.push("catalog/water_management_resources/bathing_water/bwd", DdsFolder.METADATA, metadata)
    # -> .../bwd/metadata/070d9baa-448d-4168-8514-7dadb3ad876d.xml
```

- `get_xml(uuid)` checks the response is `mdb:MD_Metadata` and identifies `uuid`
  (`NotIso19115_3`, `UuidMismatch`; `SdiNotFound` / `SdiAuthError` for 404 / 401-403).
- `resolve_series(series_uuid)` returns the one release of a series that is not superseded, or
  raises `NotCurrentError` listing every release.
- `metadata.provenance_tags()` gives `sdi_record_uuid` / `sdi_edition` / `sdi_date_stamp` in
  `Catalog.setmeta2wiki`'s `tags=` shape, if you want them on the dataset's wiki.

Environment: `SDI_API_URL` (empty means `https://sdi.eea.europa.eu/catalogue`), optional
`SDI_USERNAME` / `SDI_PASSWORD` for non-public records.

## DDS documents

`eea_datalakehouse.dds_documents` uploads files to the document folders DDS shows under every
catalog node, with one `PUT` per file. It is a **document upload**, not an ingest: nothing
becomes a Dremio table. `DdsFolder` holds DDS's eight built-in folders — `DERIVED`, `METADATA`,
`METHODOLOGY`, `ASSESSMENTS`, `ANALYSES`, `REFERENCES`, `DELIVERABLES`, `OTHERS` — and the target
folder must always be one of them.

```python
from eea_datalakehouse.dds_documents import DdsController, DdsFolder

with DdsController.from_env() as dds:
    report = dds.push("catalog.water_management_resources.bathing_water.bwd",
                      DdsFolder.DELIVERABLES, ["report.pdf", "annex/*.xlsx"])
    report.paths   # (".../bwd/deliverables/report.pdf", ".../bwd/deliverables/annex1.xlsx", ...)
```

- `push(dds_path, folder, files, overwrite=True, target_name="")` puts each file at
  `{dds_path}/{folder}/{file name}`. `dds_path` is in catalog format (`a.b.c`; quoted names like
  `"v1.0"` lose their quotes and stay one segment) or storage format (`a/b/c`), below the
  catalog root. `folder` is compulsory and must be a `DdsFolder` member; anything else, a
  string like `"metadata"` included, raises `TypeError` and uploads nothing.
- `files` is one file or a list: local paths (a glob pattern too), `DdsFile(name, data)`, or
  any object with `filename` / `content` (an `SdiMetadata` record). Every file is read and
  checked before the first upload; the content type is guessed from the name.
- `target_name` renames files in DDS: a string for the one file pushed
  (`target_name="bwd_2025.xml"`), or a dict from a file's own name to its new name
  (`target_name={"draft_v3.pdf": "report_2025.pdf"}`) when there are several. Files the dict
  leaves out, or an empty `target_name` (the default), keep their own name; a key that matches no
  file, or two files ending up with one name, uploads nothing.
- `overwrite=True` replaces files already there (sent as DDS's `X-DDS-Overwrite: true` header).
  `overwrite=False` checks every target first and
  raises `DdsFilesExistError`, uploading nothing, if any is taken. A failure part-way raises
  `DdsPushError`, whose `uploaded` lists what already went.
- `target(dds_path, folder)` shows the folder without uploading.

Environment: `DDS_BASE_URL` plus `_DREMIO_USER` / `_DREMIO_PWD`, as for ingest. The DDS document
endpoints (`dds_documents/client.py`) are still assumptions to confirm with the DDS team.

## Notebook magics for SDI and DDS (`%sdi` / `%dds`)

`%sdi` (`SdiSession`) reads SDI and `%dds` (`DdsSession`) pushes files to DDS, lists them and
gets them back. Both are built on first use from the kernel environment like `%catalog`'s and
`%ingest`'s sessions. `%dds push` puts files in a folder of the dataset `%catalog use(...)` last
selected, `%dds list` shows the file names already in one, and `%dds get` downloads one of them
into the notebook's folder (`output_dir=` for another; a missing file is an error). `path=`
picks another dataset, absolute or relative to that context, as any `%catalog` path. The folder
is compulsory and is one of DDS's eight document folders, typed bare on a `%dds` line; a
misspelt, quoted or missing folder is an error that lists the valid ones (worked example:
`debugger/sdi_session_example.ipynb`):

```python
import eea_datalakehouse.notebook   # registers %catalog/%ingest/%sdi/%dds

%catalog use("catalog.water_management_resources.bathing_water.bwd")
%dds push(DELIVERABLES, "report.pdf", "annex/*.xlsx")
record = %sdi get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")
%dds push(METADATA, record, overwrite=False)
%dds list(DELIVERABLES)        # report.pdf and the annexes, sorted
%dds list(DELIVERABLES, "*.pdf")   # filter: a wildcard, ".xlsx", "report", or a list of them
f = %dds get(DELIVERABLES, "report.pdf")   # -> ./report.pdf; f is the local file
%dds target(ASSESSMENTS)       # where a push would go
%dds help
```

`DDS_BASE_URL` is read from the first `.env` in the notebook's folder or a parent when `%dds`
builds its session (the kernel environment's value only applies if that `.env` doesn't set one;
`%dds dds_base_url()` shows which is in use). The Dremio identity is `_DREMIO_USER` /
`_DREMIO_PWD` (as `%ingest`), falling back to `DREMIO_USERNAME` / `DREMIO_TOKEN` (as `%catalog`).
After each push, `%dds` tells an integrated JupyterLab panel which files changed (comm target
`eea_datalakehouse.dds_pushed`, one message per file).

## GeoPackage export (`%gpkg`)

`eea_datalakehouse.geopackage` writes the tables and views under a Dremio catalog path to one
OGC GeoPackage (`.gpkg`) file: each spatial one as a feature layer, each other one as an
attributes table. Written with `sqlite3`, `shapely` and `pyproj` — no GDAL.

```python
import eea_datalakehouse.notebook   # registers %gpkg with the other magics

%catalog use("catalog.water_management_resources.bathing_water.bwd")
%gpkg export()                                   # every table and view -> ./bwd.gpkg
%gpkg export(tables=["stations", "areas"], limit=1000, srid=3035)
%gpkg export(path="../wfd", filename="wfd_2025.gpkg", output_dir="exports", overwrite=False)
%gpkg export(workers=8)                          # more tables streamed at once
%gpkg help
```

- `path` defaults to the `%catalog use(...)` context and is resolved like any `%catalog` path;
  it can also name a single table or view.
- `tables` picks some, by name below `path` (`"stations"`, `"sub.areas"`); default all, at any
  depth. Two with the same name become `sub_a_stations` / `sub_b_stations`.
- `limit` caps the rows per table; `srid` (`3035`, `"EPSG:3035"`) reprojects every layer to that
  CRS; `filename` / `output_dir` default to `<dataset>.gpkg` in the notebook's folder.
- **Spatial or tabular** is decided from the data: a column whose values decode as WKB/EWKB,
  WKT/EWKT, hex WKB or GeoJSON is the geometry (columns named `geom`, `geometry`, `wkt`,
  `shape`, ... are tried first); failing that, latitude/longitude columns make points.
- **CRS** per row: the SRID inside the geometry (EWKB/EWKT), else a CRS column (`srid`,
  `sr_id`, `epsg`, `crs`, ...: `3035`, `"EPSG:3035"`, an EPSG URN or a CRS name), else EPSG:4326
  for latitude/longitude. A layer takes `srid`, or its first rows' CRS; rows in another CRS are
  reprojected to it. A layer with no CRS at all is written with the undefined SRS (-1), and the
  report says so.
- **CRS definitions** come from `geopackage/epsg_definitions.json` (`{code: {name, wkt}}`, shipped
  with the package): both ends of every reprojection and the `gpkg_spatial_ref_sys` rows are built
  from it, so the coordinates and the SRS that describes them agree. A code the file lacks falls
  back to pyproj's EPSG database; codes and CRS names in the file (`900913`, `"ETRS89 / UTM zone
  32N"`) are accepted for `srid` and in CRS columns.

**Streaming.** Nothing is read whole. `workers` tables (default 4) are read at once, each over
its own Arrow Flight gRPC channel (`Catalog.open_flight_channel`, one login shared), a batch at
a time with no overall deadline on the stream. Each worker decodes, reprojects and encodes its
batches; one thread owns the SQLite file and only inserts, fed through a bounded queue — a few
batches per worker — so memory stays flat whatever the table size. The file is built under a
temporary name with no journal or fsync in one transaction, and renamed into place only when
every table succeeded; a failure in any table stops the others and leaves no partial file.
Against a remote Dremio the parallel channels hide the network wait (about 2× faster with 4
workers in a simulated 50 ms/batch test); without network wait, Python's per-row work caps the
export at roughly 50–60k rows/s whatever the number of workers.

**Progress.** `%gpkg export` prints a line as each table or view is written (`✓ 3/4 areas ←
catalog...areas: features, POINT, SRS 3035, 1,500 rows, 0.1s`), or as one fails (`✗` with its
name and the error — the export then stops and no file is written), and a total at the end.

**Then push it to DDS.** The report `%gpkg export` returns is also the written file's path
(`os.PathLike`), so `%dds push` takes it like any file — uploaded as
`application/geopackage+sqlite3` to the same dataset's folder:

```python
report = %gpkg export(srid=3035)
%dds push(DELIVERABLES, report)        # -> catalog/.../bwd/deliverables/bwd.gpkg
```

In Python: `GpkgController(DremioSource(catalog)).export(catalog_path, "out.gpkg", tables=...,
limit=..., srid=..., workers=..., progress=print)` returns a `GpkgReport` listing each layer, its geometry
source, type, SRS and row count. `Catalog.stream_rows(sql, idempotency_key=...)` streams any
SELECT as Arrow batches the same way.

## Notebook facade (`%catalog` / `%ingest`)

For interactive use in JupyterLab, `eea_datalakehouse.notebook` registers two line magics
aimed at data custodians rather than application developers, thin wrappers over
`CatalogSession`/`IngestSession`. `%catalog` runs each call **immediately** — no commit
step to remember. `%ingest` still queues and needs an explicit `commit()`, since a catalog
operation can't run before its target has actually been ingested. See
`docs/notebook-facade-for-data-scientists.md` for the full design, and
`docs/notebooks/catalog_session_example.ipynb` / `ingest_session_example.ipynb` for worked
examples. Install the extra this needs once: `pip install "EEADataLakehouse[notebook]"`.

```python
import eea_datalakehouse.notebook  # registers %catalog/%ingest — no %load_ext needed

%catalog data_copy("draft.raw_2026", "bwd.reference.water_temperature")
%catalog set_tags("bwd.reference.water_temperature", ["reviewed"])

%ingest ingest(folder="./bw_2026", target_catalog_path="bwd.reference",
                data_format="parquet", table_name="water_temperature")
%ingest diagnose          # optional: check the queued ingests first; nothing is uploaded
%ingest commit(retry=True)
```

`%catalog help` (or `%catalog help()`) renders every command as an HTML table — name,
parameters, description — rather than a raw Python signature, since its audience is a data
custodian, not necessarily a developer. `%ingest help` does the same for `IngestSession`'s
(fewer) methods — handy when you don't remember an exact parameter name mid-notebook.

`%ingest ingest(...)` also takes what a one-off ingest may need, each on that same line:
`upload_mode="proxy"` when S3 is unreachable from the kernel, `csv_options={"delimiter": ";"}`,
`json_options={"format": "array"}`, `table_definition={"columns": [{"name": "value",
"type": "DOUBLE"}]}` for the column types of a CSV or JSON table, and `cleanup_on_failure=True`
so a failed commit deletes its upload instead of leaving it for a retry.

A freshly created session already has a context — it starts at `"catalog"` (the catalog
root), not empty — so a relative path works even before the first `use()` call.
`use(None)`/`use("")` reset it back to `"catalog"` the same way; `set_context(None)` (the
lower-level primitive `use` wraps, not normally called directly) is the one way left to
clear it to no context at all.

`%%catalog` (the cell-magic form) sets the context once, with `use(path)` on its magic line,
then runs every other line of the cell in order under that context, without repeating the full
path on each line. `use` is the *only* way context ever changes — no other verb touches it as
a side effect of running, even one whose own `path` fully resolves to something that could
sensibly become the new context (`create_folder`, notably, used to leave its own path as the
new context; it no longer does — every verb only *reads* context, never writes it). `use`'s
own `path` must always be a whole, absolute path — unlike every other verb, it's never
resolved against whatever context already exists, and never accepts a leading `.`/`../`. It
does make a live check that `path` actually exists in the catalog first, raising if it
doesn't, rather than silently pointing context somewhere later calls would fail against
anyway; `use(None)`/`use("")` skip that check and reset context to `"catalog"` instead.
Every other verb's own `path`/`source_path`/
`target_path` — including `data_copy`/`data_move`/`create_view`'s two, each resolved
independently against that same context — still resolves against the current context once
one is set — with or without a leading `.` (`"2027"` and `".2027"` mean the same thing) —
unless it already starts with `catalog` (this deployment's one real root source), in which
case it's always taken literally as absolute rather than appended to the context, dot or
not. That's what makes pairing a relative `source_path` with a genuinely unrelated absolute
`target_path` in the same `data_copy`/`data_move`/`create_view` call unambiguous without
needing a dot on the absolute side: a real absolute path here always starts with `catalog`.
One or more leading `../` (or a bare `..`) instead walks up that many levels of the context
first, everywhere a relative path is understood except inside `use` itself —
`"../water_temperature"` is a sibling of the context, `"../../water_temperature"` a level
further up; `"."`/`"./"` alone (no name after either) mean the context itself. `get_context()`
shows what the context currently is — always exactly what `use`/`set_context` last stored —
and `use(path)` prints the same thing itself (`context set to <path>`), since there's nothing
to commit — `use` never queues anything, so `%catalog`'s usual auto-commit would otherwise
have nothing to show:

```python
%%catalog use("bwd.reference")
set_tags("water_temperature", ["reviewed"])  # bare, no dot — same as ".water_temperature"
create_folder(".2027")   # creates bwd.reference.2027 for real, but leaves context untouched

%catalog get_context()               # -> 'bwd.reference' — still what use() set, not create_folder
%catalog use("bwd.reference.2027")   # prints: context set to 'bwd.reference.2027'
%catalog get_context()               # -> 'bwd.reference.2027'
%catalog list(".")   # '.' alone -> the context itself: same as list("bwd.reference.2027")
%catalog set_tags("../water_temperature", ["archived"])  # ../ works for any verb except use()
%catalog get_tags("catalog.other_root.assessments")  # starts with 'catalog' — absolute, not appended
%catalog data_copy(".water_temperature", "catalog.other_root.archive.water_temperature_2027")
#                                          ^ starts with 'catalog' — absolute, not appended either
%catalog data_copy(".water_temperature", "archive")
#                                          ^ bare, no dot — relative too, appends to the context
```

`get_wiki(path)`, `get_tags(path)`, `list(path)` (every table/view under `path`, at any depth)
and `schema(path)` (column types + row count) answer immediately, like `get_context()` — nothing
to commit or undo. `delete_view(path)`/`delete_table(path)` queue like every other write, but —
unlike the idempotent `DROP ... IF EXISTS` `Catalog.deleteview`/`deletetable` wrap — require
`path` to already exist, and (like `delete_folder`) can never be undone. Every one of these
raises `CatalogOperationError` if `path` doesn't exist; `schema` also requires it to be a table
or view, not a folder. `list`'s `path` may also be omitted (or `""`) to list the current
context itself — raises `CatalogSessionError` if none is set yet:

```python
%catalog get_wiki("bwd.reference.water_temperature")
%catalog get_tags("bwd.reference.water_temperature")
%catalog list("bwd.reference")      # -> ['bwd.reference.water_temperature', ...]
%catalog use("bwd.reference")
%catalog list()                     # same result — path omitted, lists the context itself
%catalog schema("bwd.reference.water_temperature")   # -> TableInfo(schema={...}, row_count=...)
%catalog delete_view("bwd.reference.old_view")
```

## Layout

| Path | Purpose |
|---|---|
| `dds_ingestion/credentials.py` | env-var creds + redacted `DremioCreds` |
| `dds_ingestion/models.py` | typed request/response models for the DDS ingest contract |
| `dds_ingestion/client.py` | thin, unit-testable HTTP client (`IngestClient`) |
| `dds_ingestion/progress.py` | tqdm progress bar with graceful fallback |
| `dds_ingestion/folder.py` | `FolderIngest` orchestration (scan/parallel/resume) |
| `dds_documents/client.py` | `DocumentsClient` — put/get/list plain files in DDS folders (no ingest) |
| `dds_documents/controller.py` | `DdsController` / `DdsFolder` — push, list and get files in a catalog node's DDS document folders (metadata, deliverables, ...) |
| `dds_documents/paths.py` | catalog (`a.b.c`) ⇄ storage (`a/b/c`) path conversion |
| `dds_documents/session.py` | `DdsSession` — what `%dds` dispatches onto |
| `geopackage/controller.py` | `GpkgController` — Dremio tables/views to a GeoPackage, streamed over parallel Flight channels |
| `geopackage/spatial.py` | spatial-or-tabular detection, geometry decoding, CRS lookup and reprojection |
| `geopackage/writer.py` | `GeoPackageWriter` — the `.gpkg` file itself, with `sqlite3`, bulk-loaded |
| `geopackage/session.py` | `GpkgSession` — what `%gpkg` dispatches onto |
| `sdi/controller.py` | `SdiController` — `get_xml()`, `resolve_series()` |
| `sdi/catalogue.py` | `SdiCatalogue` — read-only GeoNetwork REST client |
| `sdi/session.py` | `SdiSession` — what `%sdi` dispatches onto |
| `session_utils.py` | friendly session errors, strict argument types, `.env` lookup |
| `sdi/iso.py` | reads UUID, title, edition, dates and series children from ISO 19115-3 |
| `catalog/client.py` | `Catalog` — two connections (REST + Flight), every operation as a method |
| `catalog/operations.py` | the operations themselves (table2view, datacopy, createfolder, ...), as functions taking an executor and/or a `CatalogRestClient` |
| `catalog/foldertree.py` | `folderscan`/`foldercopy`/`foldermove` — a whole folder at a time, with the view-SQL rewriter (`remap_view_sql`) that keeps a copied subtree self-contained |
| `catalog/sql.py` | `SqlExecutor` protocol + REST/Flight implementations, `resolve_executor()` (transport env var) |
| `catalog/rest.py` | `CatalogRestClient` — Dremio's native `/api/v3/catalog` for folders, wiki, and tags (none of which have a SQL/Flight equivalent) |
| `catalog/retry_state.py` | persisted memory of stalled attempts, for `retry_pending()` |
| `catalog/errors.py` | `CatalogOperationError`, `EngineStartingError` |

## Development

```bash
pip install -e ".[dev]"
pytest
```

### Redmine (taskman) notes

A commit whose message references an issue as `refs #1234` or `fixes #1234`
(not a bare `#1234`) is posted as a note on that issue when you push, under
your own Redmine account. This only adds a note; it never changes the issue's
status. It runs from git hooks on your machine using your own API key, so the
key never goes to GitHub. One-time setup per clone:

```bash
git config core.hooksPath .githooks   # VS Code's "Install git hooks" task does this on folder open
echo "REDMINE_API_KEY=<your key>" >> debugger/.env   # taskman > My account > API access key
```

CI ("Redmine hook check") fails for any commit that references an issue but
was made without the hooks or without a key. To fix one, set up the hooks as
above, then run
`git rebase --exec 'git commit --amend --no-edit' <base-branch>` and
force-push.

## Releasing a new version

1. Update `version` in `pyproject.toml` (e.g. `0.2.0`).
2. Commit and push to `main`.
3. The `Release` GitHub Actions workflow runs on every push to `main`: it reads
   the version from `pyproject.toml`, builds the package, and publishes a
   `v0.2.0` GitHub Release (creating the matching tag automatically), with
   notes auto-generated from merged PRs since the previous release
   (categorized via `.github/release.yml`).

   Pushing to `main` without bumping the version re-publishes the release for
   the current version with the latest build artifacts.

**Only one release/tag exists per branch at a time** — `main` always has exactly one `vX.Y.Z`
release, `staging` exactly one `vX.Y.Z-staging` prerelease. Each new release deletes its branch's
previous release *and* tag first, so **tags aren't permanent** — pin to whatever the
[Install](#install) badge shows *now*, not to an old tag number, since it won't exist once a
newer release replaces it.

**A separate floating tag literally named `main`/`staging`** always points at that branch's
latest release — the workflow force-moves it (`git tag -f`, force-push) once the real release
above succeeds. It deliberately shares its name with the branch: git resolves the ambiguity
deterministically (a tag always wins over a same-named branch), so `@main`/`@staging` in an
install command means "latest release," not "current branch tip" — expect (and ignore) a
"refname is ambiguous" warning from git/pip when that happens.

The workflow also rewrites this README's `pip`/`%pip install ...@vX.Y.Z[-staging]` example lines
to the version it just released, committing that change back to the branch (`[skip ci]`, so it
doesn't re-trigger itself) — so the examples above never go stale, without anyone having to
remember to update them by hand.

## Install in JupyterLab

Run this in a notebook cell — `@main`/`@staging` always resolves to whatever was most recently
released for that branch (see [Install](#install) above):

```python
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@main"       # latest stable
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@staging"    # latest early access
```

To pin to one specific release instead, use the exact tag the live badges under
[Install](#install) show (kept in sync automatically, see
[Releasing a new version](#releasing-a-new-version)):

```python
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.35"

# staging's latest release (early access)
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.35-staging"
```

Use the `%pip` magic rather than `!pip` — it installs into the kernel the
notebook is actually running on, instead of whatever `pip` happens to be
first on `PATH`. After installing, restart the kernel (**Kernel > Restart
Kernel...**) so the import below picks up the newly installed package:

```python
from eea_datalakehouse.dds_ingestion import FolderIngest
```

Alternatively, download the wheel attached to the [GitHub Release page](https://github.com/eeadata/EEALakeHouse.python/releases/latest)
for that tag and install the local file instead of pulling from git:

```python
%pip install /path/to/EEADataLakehouse-<version>-py3-none-any.whl
```
