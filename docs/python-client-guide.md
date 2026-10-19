# EEADataLakehouse — Python client guide

Full reference for the `eea_datalakehouse` library: every public class, method,
model and error, with the behaviour that is not obvious from the signature.

The library has two independent halves, and they talk to two different systems:

| Package | Talks to | For |
|---|---|---|
| `eea_datalakehouse.dds_ingestion` | the **Dremio Document Service** (DDS) REST API, plus S3 via DDS-issued presigned URLs | getting a folder of files *into* the lakehouse as a table |
| `eea_datalakehouse.catalog` | **Dremio itself** — its SQL Jobs API, Arrow Flight SQL, and its `/api/v3/catalog` REST API | everything *after* the data has landed: views, copies/moves, folders, wiki, tags |

They share no code and no credentials object. You can use either alone.

- Source layout, install instructions and release process: [`../README.md`](../README.md)
- Ingest quick-start for notebook users: [`../src/eea_datalakehouse/dds_ingestion/README.md`](../src/eea_datalakehouse/dds_ingestion/README.md)
- Open findings and proposals: [`dev-notes.md`](dev-notes.md)

**Just want to get something done?** Go straight to
[Recipes](#recipes--every-task-as-one-snippet) — one setup cell, then a complete
pasteable snippet per task. The two reference parts after it explain the
behaviour behind those snippets.

> **Scope of verification.** Everything below is documented from the code as it
> stands. Where the code itself flags an assumption as *unverified against a real
> Dremio deployment*, this guide repeats that warning rather than hiding it —
> see [Unverified assumptions](#unverified-assumptions).

---

## Contents

- [Install and requirements](#install-and-requirements)
- [Environment variables](#environment-variables)
- **[Recipes — every task as one snippet](#recipes--every-task-as-one-snippet)** ← start here
  - [Cheat sheet](#cheat-sheet)
  - [The setup cell](#the-setup-cell)
  - [Ingest recipes](#ingest-recipes)
  - [Catalog recipes](#catalog-recipes)
    - [Move a whole folder](#move-a-whole-folder-somewhere-else)
  - [End to end, in one cell](#end-to-end-in-one-cell)
- [Part 1 — `dds_ingestion`](#part-1--dds_ingestion)
  - [The transfer model](#the-transfer-model)
  - [`FolderIngest`](#folderingest)
  - [`intent` — what a transfer leaves behind](#intent--what-a-transfer-leaves-behind)
  - [`sub_path` — read-only data that accumulates](#sub_path--read-only-data-that-accumulates)
  - [Table definitions, CSV/JSON options, inferred columns](#table-definitions-csvjson-options-inferred-columns)
  - [`upload_mode` — how the bytes travel](#upload_mode--how-the-bytes-travel)
  - [Checking a transfer first: `dry_run()` and `diagnose()`](#checking-a-transfer-first-dry_run-and-diagnose)
  - [Managing, resuming and abandoning a transfer](#managing-resuming-and-abandoning-a-transfer)
  - [`IngestOutcome`, `scan_folder`, `ingest_folder`](#ingestoutcome-scan_folder-ingest_folder)
  - [`IngestClient` — one method per endpoint](#ingestclient--one-method-per-endpoint)
  - [Models](#models)
  - [Credentials](#credentials)
  - [Progress bars](#progress-bars)
  - [Ingest errors](#ingest-errors)
  - [Notebook helpers (`dds_ingestion.common`)](#notebook-helpers-dds_ingestioncommon)
- [Part 2 — `catalog`](#part-2--catalog)
  - [`Catalog` and its two transports](#catalog-and-its-two-transports)
  - [`idempotency_key` and the retry contract](#idempotency_key-and-the-retry-contract)
  - [Operation reference](#operation-reference)
    - [Whole folders](#whole-folders--catalogfoldertree)
  - [Executors (`catalog.sql`)](#executors-catalogsql)
  - [`CatalogRestClient` (`catalog.rest`)](#catalogrestclient-catalogrest)
  - [`retry_state`](#retry_state)
  - [Catalog errors](#catalog-errors)
- [Cross-cutting](#cross-cutting)
  - [Secret handling](#secret-handling)
  - [Lifecycle and cleanup](#lifecycle-and-cleanup)
  - [Unverified assumptions](#unverified-assumptions)
  - [Not implemented yet](#not-implemented-yet)
- [Development](#development)

---

## Install and requirements

Python **3.11+**. From the repository root:

```bash
pip install -e ".[dev]"      # dev extra adds pytest / ruff / mypy / respx / types-tqdm / ipykernel
```

Or a released build (see the badges in [`../README.md`](../README.md#install) for
what `@main` / `@staging` currently resolve to):

```bash
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@main"
```

Runtime dependencies are declared in `pyproject.toml`. Two are worth calling out
for this guide:

- **`httpx`** — every HTTP call in both halves of the library goes through it.
- **`pyarrow`** — only `FlightSqlExecutor` needs it, and it is imported *inside*
  the methods that use it, so a caller who never touches `datacopy`/`datamove`
  never pays for the Flight stack.

`tqdm` is a declared dependency but the ingest side degrades gracefully without
it (see [Progress bars](#progress-bars)).

> **Note on `__version__`.** `pyproject.toml` carries the real, released version
> (currently `0.1.10`); `eea_datalakehouse.__version__` is a separate literal in
> `src/eea_datalakehouse/__init__.py` that has not been kept in step with it.
> Read the installed distribution's metadata, not `__version__`, if you need to
> know which release you have.

---

## Environment variables

| Variable | Read by | Purpose |
|---|---|---|
| `_DREMIO_USER` | `dds_ingestion.load_creds()` | Dremio username, injected by the JupyterLab extension |
| `_DREMIO_PWD` | `dds_ingestion.load_creds()` | Dremio **PAT**, sent as the `Authorization: Bearer` token |
| `DDS_BASE_URL` | `dds_ingestion.load_base_url()` | root URL of the Document Service |
| `EEA_CATALOG_TRANSPORT` | `catalog.resolve_executor()` **only** | `flight` selects Arrow Flight; anything else (including unset) means REST. **`Catalog` does not consult this** — see [`Catalog` and its two transports](#catalog-and-its-two-transports) |
| `DREMIO_FLIGHT_LOCATION` | `catalog.sql.resolve_flight_location()` | explicit Flight endpoint, e.g. `grpc+tls://host:32010` |
| `EEA_CATALOG_RETRY_STATE` | `catalog.retry_state` | override the retry-state file path (default `~/.cache/eea_datalakehouse/catalog_retry_state.json`) |

`Catalog(base_url, token, username=...)` takes its Dremio credentials as
arguments, not from the environment — they are *not* the DDS credentials above.

---

# Recipes — every task as one snippet

Everything here is written for **single-shot execution**: a notebook cell you run
once, or a scheduled script. Each recipe is complete and pasteable on its own,
assumes only [the setup cell](#the-setup-cell) has run, and is kept to the
fewest lines that actually work.

## Cheat sheet

| I want to… | Call |
|---|---|
| send a folder to the lakehouse as a table | `ingest("./data", "path.to.target", data_format="parquet")` |
| …and keep the handle, so a failure is resumable | `job = FolderIngest(..., **_dds); job.run()` |
| add this year to a table that grows | `ingest(..., intent="read_only", table_name="t", sub_path="2026")` |
| redo one year only | `ingest(..., sub_path="2026", conflict_mode="replace")` |
| CSV / JSON into an editable table | `ingest("./csv", "path.to.target", data_format="csv", intent="editable")` — columns inferred |
| …with the column types I choose | `ingest(..., table_definition=TableDefinition(columns=(ColumnDef("v", "DOUBLE"),)))` |
| check a transfer before starting it | `job.diagnose()` · `job.dry_run()` |
| resume a failed transfer | `job.retry()` |
| …after fixing the columns | `job.retry(table_definition=…)` |
| S3 is unreachable from this kernel | `FolderIngest(..., upload_mode="proxy", **_dds).run()` |
| pick a transfer up in a later kernel | `FolderIngest.attach(session_id, ..., **_dds)` |
| abandon a transfer | `job.cancel()` |
| see my transfers | `IngestClient(**_dds).list_sessions(state="failed")` |
| create the target folders first | `ensure_catalog_path(url, token, "src/dom/sub/flow/draft")` |
| turn an ingested table into a view | `catalog.table2view(view, source, idempotency_key=KEY)` |
| …for every table in a folder | `for p in catalog.gettablesfrom(SRC, …): catalog.table2view(…)` |
| copy / move a table | `catalog.datacopy(a, b, …)` · `catalog.datamove(a, b, …)` |
| **move a whole folder** — subfolders, tables and views | `foldermove(catalog, src, dst, idempotency_key=KEY)` |
| …see what that would do, first | `foldermove(…, dry_run=True)` → `print(report.summary())` |
| copy a folder without removing the source | `foldercopy(catalog, src, dst, idempotency_key=KEY)` |
| list everything under a folder | `catalog.gettablesfrom("bwd", idempotency_key=KEY)` |
| schema + row count, no rows fetched | `catalog.gettableitemsfrom(path, idempotency_key=KEY)` |
| create / delete a folder | `catalog.createfolder(p, …)` · `catalog.deletefolder(p, …)` |
| describe a folder | `catalog.setwikito(p, text, …)` · `catalog.setmeta2wiki(p, tags=…, …)` |
| tag a table | `catalog.settagsto(path, ["a", "b"], idempotency_key=KEY)` |
| the engine was cold — finish it later | `catalog.retry_pending(KEY)` |

---

## The setup cell

One cell, once per kernel. It assumes the usual globals are already bound —
`DREMIO_USERNAME`, `DREMIO_TOKEN`, `DREMIO_URL`, `DDS_BASE_URL` — which is what
`%init` leaves behind in a Hub kernel and what the local stack and scheduled
runs export:

```python
from functools import partial
from eea_datalakehouse.catalog import Catalog
from eea_datalakehouse.dds_ingestion import DremioCreds, FolderIngest, IngestClient, ingest_folder

_dds = {"base_url": DDS_BASE_URL, "creds": DremioCreds(DREMIO_USERNAME, DREMIO_TOKEN)}
ingest = partial(ingest_folder, **_dds)                                    # one-shot transfers

catalog = Catalog(DREMIO_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)      # catalog operations
KEY = "my-job-2026-09-08"          # any stable string; reuse it to resume — see below
```

That is the only setup any recipe below needs. Three things worth knowing about
it, none of which need code:

- **`**_dds` is only carrying credentials.** Where the environment instead
  carries `_DREMIO_USER` / `_DREMIO_PWD` / `DDS_BASE_URL` — the names
  `FolderIngest` reads by default — you can drop `**_dds` from every call and
  pass nothing at all. Keeping it works either way, which is why the recipes use
  it.
- **`username=` is only needed for `datacopy`/`datamove`**, which authenticate
  over Arrow Flight. Every other catalog operation works without it.
- **Close the catalog** in your last cell: `catalog.close()`. A forgotten one is
  still disposed at interpreter exit or on SIGTERM — see
  [Lifecycle and cleanup](#lifecycle-and-cleanup).

If `%init` itself is missing (this project's own container ships no Hub
extension), register the stand-in before it — same cell is fine; see
[Notebook helpers](#notebook-helpers-dds_ingestioncommon) for what it binds:

```python
from eea_datalakehouse.dds_ingestion.common.dremio_identity import ensure_init_magic
ensure_init_magic()
%init
```

---

## Ingest recipes

### Send a folder to the lakehouse as a table

The whole `begin → upload → commit` flow, one call:

```python
out = ingest("./my_data", "biodiversity.uploads", data_format="parquet")
print(out.commit.table_path, out.commit.record_count, out.commit.storage_path)
```

`data_format` is `"parquet"`, `"csv"` or `"json"` and picks which files are
scanned — anything else in the folder is ignored, not an error. Sub-folders are
kept as they are. Defaults you did not pass: `intent="read_only"`,
`conflict_mode="fail"`, `parallelism=4`, progress bar on.

### …and keep the handle, so a failure is resumable

`ingest()` is one-shot: it closes the client and the session handle is gone. If
the load might fail, drive the class instead — same call, two more lines:

```python
job = FolderIngest("./my_data", "biodiversity.uploads", data_format="parquet", **_dds)
out = job.run()
```

Everything below that says `job` assumes this form.

### Add this year to a table that grows

`sub_path` files the upload under a named slice of the same table:

```python
ingest("./bw_2026", "water_management_resources.bathing_water.bwd.reference",
       data_format="parquet", intent="read_only",
       table_name="water_temperature", sub_path="2026")
```

Next year, the same call with `sub_path="2027"`. The name is normalised
server-side (`"Q1 2026"` → `q1_2026`). If your folder already has the structure
locally (`2026/*.parquet`), drop `sub_path` — the layout is preserved as-is.

### Redo one year, leave the others standing

```python
ingest("./bw_2026", "water_management_resources.bathing_water.bwd.reference",
       data_format="parquet", intent="read_only",
       table_name="water_temperature", sub_path="2026", conflict_mode="replace")
```

### CSV or JSON into an editable table

An editable table is built from column types. For CSV and JSON the client reads
every file locally, infers them, prints them (`Inferred columns: …`) and sends
them with the transfer — nothing to write:

```python
out = ingest("./stations_csv", "biodiversity.uploads", data_format="csv", intent="editable")
```

To choose the types yourself, pass a table definition — and parsing hints, if
the files need them:

```python
from eea_datalakehouse.dds_ingestion import ColumnDef, TableDefinition

columns = TableDefinition(columns=(
    ColumnDef("station_id", "VARCHAR"),
    ColumnDef("measured_at", "TIMESTAMP"),
    ColumnDef("value", "DOUBLE"),
))
out = ingest("./stations_csv", "biodiversity.uploads", data_format="csv", intent="editable",
             table_definition=columns, csv_options={"delimiter": ";"})
```

A JSON *array* file cannot be inferred — pass its columns this way. Details:
[Table definitions](#table-definitions-csvjson-options-inferred-columns).

### Check a transfer before you start it

```python
job = FolderIngest("./my_data", "biodiversity.uploads", data_format="parquet", **_dds)
job.diagnose()       # one ✓ / ✗ line per check, ending with a dry run of this folder
job.dry_run()        # only the dry run: what commit would do
```

Neither uploads anything or opens a session. `diagnose()` says, in plain
language, which of DDS, your credential, the file sizes, DDS's storage and the
target is the problem — see
[Checking a transfer first](#checking-a-transfer-first-dry_run-and-diagnose).

### The transfer failed — resume it without re-uploading

```python
job = FolderIngest("./my_data", "biodiversity.uploads", data_format="parquet", **_dds)
try:
    job.run()
except Exception:
    print(job.status().error)
    job.retry()          # re-runs only the step that failed, when the server kept the files
```

`retry()` decides from the server's own report. It re-runs just the load step
when the staged files survived, starts a fresh session when they did not, and
**raises rather than guessing** for a permanently-stored transfer that a blind
re-run would duplicate. An *upload* failure has already cancelled the session
(the error's note says so); `retry()` then uploads again if the failure can pass
— a network error, a 5xx — and otherwise raises with the upload error. The full
decision table is [here](#managing-resuming-and-abandoning-a-transfer).

### The load failed on its columns — fix them and resume

A commit that failed with `schema_required` or `schema_mismatch` (the error's
`error_slug`) needs no new upload — only the right columns:

```python
from eea_datalakehouse.dds_ingestion import ColumnDef, TableDefinition

job.retry(table_definition=TableDefinition(columns=(ColumnDef("value", "DOUBLE"),)))
```

The corrected definition replaces the stored one and only the load re-runs. It
needs a DDS server with DI-12.6; an older one ignores the definition and the
retry fails as before.

### Pick a transfer up in a later kernel

The session lives on the server, so yesterday's transfer is still there:

```python
job = FolderIngest.attach("sess-123", "./my_data", "biodiversity.uploads",
                          data_format="parquet", **_dds)
s = job.status()
print(s.status, s.placement, s.error)
job.retry()              # or job.cancel()
```

`folder` must still point at the same local data, so a re-upload is possible if
the staged copy is gone.

### Abandon a transfer

Deletes the staged data and drops the session. It does **not** drop a table an
earlier successful commit already created:

```python
job.cancel()
```

### See my transfers

```python
with IngestClient(**_dds) as c:
    for s in c.list_sessions(state="failed"):     # "active" | "failed" | "all"
        print(s.session_id, s.status, s.placement, s.error)
```

### How many rows actually staged?

For a session that has uploaded but not yet committed — typically one whose
commit failed. On a managed catalog the count comes from the staged Parquet
footers; CSV and JSON have none and report `0`:

```python
job = FolderIngest.attach("sess-123", "./my_data", "bio.uploads",
                          data_format="parquet", **_dds)
print(job.estimate())        # EstimateResult(record_count=…, size_class=…)
```

### Create the target folders before ingesting

Ingest does not create its target, and `commit` fails with
`Namespace does not exist: …` if it is absent. Note the **slashes** here — this
helper takes a path, not a dotted catalog name. Check first with `dry_run=True`:

```python
from eea_datalakehouse.dds_ingestion.common.catalog import ensure_catalog_path, print_report

path = "water_management_resources/bathing_water/bwd/draft"
print_report(ensure_catalog_path(DDS_BASE_URL, DREMIO_TOKEN, path, dry_run=True))
print_report(ensure_catalog_path(DDS_BASE_URL, DREMIO_TOKEN, path))
```

The source, domain and subdomain must already exist — a missing one raises,
because that is a taxonomy question to raise, not a folder to invent. Only the
dataflow level and below are created.

### S3 is unreachable from this kernel — send the bytes through the server

Every file goes through DDS (`POST /api/v1/ingest/stage`) instead of to the
presigned S3 targets. Slower, since every byte transits DDS:

```python
job = FolderIngest("./my_data", "biodiversity.uploads", data_format="parquet",
                   upload_mode="proxy", **_dds)
out = job.run()
print(out.upload_mode, out.files_staged)     # "proxy", every file
```

Usually you do not need to ask. With the default `upload_mode="auto"` the client
follows the server, which can say `proxy` itself; and when it says `auto` (DDS's
default), a file whose presigned upload fails at the network layer goes through
DDS on its own. See [`upload_mode`](#upload_mode--how-the-bytes-travel).

### Tell staged from permanent

Whether your uploaded files *are* the table matters before you re-run anything:

```python
s = job.status()
print(s.placement)            # "staged" (copied in, upload deleted) | "read_permanent" (kept)
print(s.stores_permanently)   # True only for read_permanent
```

---

## Catalog recipes

Every operation takes `idempotency_key=` and it is **required**. Use one stable
string per job (the `KEY` from the setup cell): if a cold Dremio engine stalls
the call, that key is what lets you finish it later.

### Turn a freshly ingested table into a view

The one-time transition for a location that arrived as a physical table:

```python
catalog.table2view("bwd.consumer", "bwd.draft.bw_assessment", idempotency_key=KEY)
```

The source is checked first, and `view_path`'s containing folder is created if
missing (`create_target_folder=True` by default).

### Turn every table in a folder into views in another folder

`table2view` is **one table to one view** — it does not accept a folder. A
folder as `source_path` fails the existence check with
`source 'bwd.draft' does not exist`, because that check queries
`INFORMATION_SCHEMA."TABLES"`, which lists tables and views and never folders.
(The folder-aware `cp source dest/` behaviour belongs to `datacopy`/`datamove`,
and even there only for the *target*.)

Compose it from `gettablesfrom` instead — one listing query, then one view per
table:

```python
SRC, DST = "bwd.draft", "bwd.consumer"

for path in catalog.gettablesfrom(SRC, idempotency_key=f"{KEY}-list"):
    rel = path[len(SRC) + 1:]                 # "bw_assessment", "nested.deep_table"
    catalog.table2view(f"{DST}.{rel}", path, idempotency_key=f"{KEY}-{rel}")
```

> ⚠ **Never point `DST` at `SRC`.** `table2view` starts with
> `DROP TABLE IF EXISTS <view_path>` — that is the whole point when a location
> is *becoming* a view — so identical paths drop the source table and then
> create a view selecting from what was just dropped.

Four things that loop is doing deliberately:

- **`DST` must not sit inside `SRC`.** `gettablesfrom` recurses the entire
  subtree, so a target nested under the source gets re-read on the next run and
  you end up building views over your own views.
- **The listing includes views, not just tables.** `INFORMATION_SCHEMA."TABLES"`
  covers both and `gettablesfrom` does not return `TABLE_TYPE`, so there is no
  public way to filter. Harmless when the source folder holds only ingested
  tables; check it yourself if it might not.
- **`rel` preserves nesting.** `bwd.draft.nested.deep_table` becomes
  `bwd.consumer.nested.deep_table`, and the missing `bwd.consumer.nested` is
  created on the way. Using `path.rsplit(".", 1)[-1]` instead would flatten the
  tree, and two sub-folders holding the same table name would silently collide.
- **One key per table.** A cold engine stalling on the fifth table then leaves
  the first four cleared and that one alone retryable, with
  `catalog.retry_pending(f"{KEY}-{rel}")`.

### Copy a table

Always over Arrow Flight, so the setup cell's `username=` matters here:

```python
catalog.datacopy("bwd.draft.bw", "bwd.versions.v2026", idempotency_key=KEY)
```

Into an existing **folder** — `cp source dest/` semantics, the source's own name
is appended:

```python
catalog.datacopy("bwd.draft.bw", "bwd.versions", idempotency_key=KEY)   # → bwd.versions.bw
```

Replacing something already there, and creating the target folder on the way:

```python
catalog.datacopy("bwd.draft.bw", "bwd.versions.v2026",
                 overwrite=True, create_target_folder=True, idempotency_key=KEY)
```

### Move a table or view

Copy, then drop the source. The kind (`TABLE`/`VIEW`) is detected for you, and
the target is verified to exist before it reports success:

```python
catalog.datamove("bwd.draft.bw", "bwd.versions.v2026", idempotency_key=KEY)
```

### Move a whole folder somewhere else

Sub-folders, tables and views together — the views rewritten so the copy points
at its own tables rather than back at the originals. Look before you leap:

```python
from eea_datalakehouse.catalog import foldermove

report = foldermove(catalog, "bwd.draft", "bwd.archive.2025",
                    idempotency_key=KEY, dry_run=True)
print(report.summary())          # every folder, table (with its row count) and rewritten view
```

Nothing was written. When the plan reads right, drop `dry_run`:

```python
report = foldermove(catalog, "bwd.draft", "bwd.archive.2025",
                    idempotency_key=KEY, on_progress=print)
print(report.summary())
report.verified, report.source_deleted
```

What it actually does, in this order:

1. **Folders** first, shallowest first — including empty ones, which
   `INFORMATION_SCHEMA` cannot see and only the REST catalog knows about.
2. **Tables** with `datacopy`, then `COUNT(*)` **on both sides**. A table whose
   counts disagree fails the whole move.
3. **Views** recreated from their own SQL, with every reference into the moved
   subtree remapped to the new location, in dependency order (a view over
   another view is created after it). They are counted on both sides too.
4. **Only then** the source is taken apart — views, tables, folders
   deepest-first, then the source folder itself.

**If anything fails, the source is left exactly where it is** and
`report.verified` is `False`. Fix what the report names and run it again:
tables that already landed with a matching count are skipped, so a re-run is
cheap and safe. To copy without ever removing the source, call `foldercopy`
with the same arguments.

Worth knowing:

- **`overwrite=False` by default.** A target table that already exists with a
  *different* row count fails that entry rather than being silently accepted.
  Pass `overwrite=True` to replace it.
- **`report.summary()` is the thing to read** — per entry: what happened, both
  row counts, and any note about how a view's SQL was rewritten.
- **The view rewrite is textual**, not a parse of Dremio's SQL dialect. It
  never touches string literals or comments, and only rewrites chains it can
  resolve. Read the rewritten definitions in a dry run before applying a
  subtree of complicated views.
- **Dremio wiki and tags travel with each entry** (`copy_metadata=True`),
  best-effort — a wiki that will not copy is a note, not a failure. The **DDS**
  `meta.json` sidecar is a different system and is *not* carried.

### Delete a view

`DROP VIEW IF EXISTS` — safe to re-run, and it fails loudly rather than dropping
a table by mistake:

```python
catalog.deleteview("bwd.consumer", idempotency_key=KEY)
```

### List everything under a folder

Recurses the whole subtree in one query, returning full dotted paths:

```python
for path in catalog.gettablesfrom("bwd", idempotency_key=KEY):
    print(path)
```

### Schema and row count, without fetching a single row

```python
info = catalog.gettableitemsfrom("bwd.versions.v2026.assessments", idempotency_key=KEY)
print(info.row_count)
for column, dtype in info.schema.items():
    print(f"{column:30} {dtype}")
```

### Create and delete folders

Both are idempotent — already there, or already gone, is not an error:

```python
catalog.createfolder("bwd.versions.v2026", create_parents=True, idempotency_key=KEY)
catalog.deletefolder("bwd.scratch", cascade=True, idempotency_key=KEY)   # cascade = rm -r
```

Without `create_parents`, a missing parent raises instead of quietly creating a
deep path. Without `cascade`, a non-empty folder raises. The space or source
itself (the leftmost segment) is never created automatically.

### Describe a folder — wiki text and metadata tags

Dremio's own tags exist only on tables and views, so **folders** carry metadata
as tags embedded in the wiki instead:

```python
catalog.setwikito("bwd.reference", "Bathing water reference data.", idempotency_key=KEY)

catalog.setmeta2wiki("bwd.reference", tags=[
    {"tag_name": "owner",  "tag_value": "EEA",  "tag_title": "Owner"},
    {"tag_name": "update", "tag_value": "2026", "tag_title": "Last update"},
], idempotency_key=KEY)

catalog.getmetafromwiki("bwd.reference", "owner", idempotency_key=KEY)
# {'tag_name': 'owner', 'tag_value': 'EEA', 'tag_title': 'Owner'}
```

`setmeta2wiki` rewrites only the `# Meta Data` section and leaves the prose
above it alone. Pass `overwrite=False` to merge into the tags already there
(nothing is deduplicated). Without a `tag_name`, `getmetafromwiki` returns the
whole list.

```python
print(catalog.getwikifrom("bwd.reference", idempotency_key=KEY))
catalog.deletewiki("bwd.reference", idempotency_key=KEY)     # idempotent
```

### Tag a table or view

Dremio's real tags — **tables and views only**, and `settagsto` *replaces* the
set rather than adding to it:

```python
catalog.settagsto("bwd.versions.v2026.assessments", ["bathing-water", "2026"],
                  idempotency_key=KEY)

print(catalog.gettagsfrom("bwd.versions.v2026.assessments", idempotency_key=KEY))
catalog.deletetags("bwd.versions.v2026.assessments", ["2026"], idempotency_key=KEY)
```

To add without losing what is there, pass the union:

```python
current = catalog.gettagsfrom(path, idempotency_key=KEY)
catalog.settagsto(path, [*current, "new-tag"], idempotency_key=KEY)
```

### The engine was cold — finish the job later

`EngineStartingError` is a **stall, not a failure**. The attempt is written to
disk under your key, so you can pick it up from a later cell, a later notebook,
or another process entirely:

```python
from eea_datalakehouse.catalog import EngineStartingError

try:
    catalog.table2view("bwd.consumer", "bwd.draft.bw_assessment", idempotency_key=KEY)
except EngineStartingError:
    print("Dremio engine is starting — run the retry cell in a few minutes")
```

```python
catalog.retry_pending(KEY)      # same arguments, no need to retype them
```

### What is still waiting on a retry?

```python
from eea_datalakehouse.catalog import retry_state

for p in retry_state.list_pending():
    print(p.idempotency_key, p.operation, p.target, f"{p.attempts}x", p.last_error)
```

### Close the connection

```python
catalog.close()
```

---

## End to end, in one cell

Land a folder and publish it as a view — the whole single-shot flow:

```python
out = ingest("./bw_2026", "water_management_resources.bathing_water.bwd.draft",
             data_format="parquet", table_name="bw_assessment")
print(out.commit.table_path, out.commit.record_count)

catalog.table2view("water_management_resources.bathing_water.bwd.consumer.bw_assessment",
                   out.commit.table_path, idempotency_key=KEY)
```

---

# Part 1 — `dds_ingestion`

```python
from eea_datalakehouse.dds_ingestion import FolderIngest
```

Notebook-side client for the DDS Ingest API (DI-8.4 / DI-8.5). The **only**
object-storage access anywhere in this package is through presigned URLs that
DDS issues; it never uses an S3 SDK and never holds S3 credentials.

## The transfer model

A transfer is a **server-side session**, not a local loop. `FolderIngest.run()`
drives three steps:

```
begin  →  upload (all files, in parallel)  →  commit
```

1. **`begin`** — sends the file list (relative paths + sizes) and the target. The
   server validates the target *here*, so an unauthorised or non-existent
   catalog folder fails before a single byte moves. It answers with an
   [`S3Plan`](#models): a bucket, a key prefix, and one presigned
   [`UploadTarget`](#models) per file — and, from a server with contract v0.8,
   a [`BeginPlan`](#models) saying what commit will do, which `run()` shows
   before uploading.
2. **upload** — each file's bytes go straight from your machine to object
   storage using the target exactly as issued. Small files use a presigned PUT
   (older servers issue a POST policy); large ones use S3 multipart with a
   presigned URL per part. The DDS bearer token is **never** sent on these
   requests — each presigned URL carries its own auth. Where S3 cannot be
   reached from your kernel the bytes go through DDS instead — see
   [`upload_mode`](#upload_mode--how-the-bytes-travel). If an upload fails,
   `run()` cancels the session before raising, so the files that did land are
   deleted rather than left behind.
3. **`commit`** — the server loads what landed into the table. This is where the
   Dremio work happens (`CREATE TABLE` + `COPY INTO` on a managed catalog), so
   it is the step most likely to fail, and the one
   [`retry()`](#managing-resuming-and-abandoning-a-transfer) is built around.

The session outlives your kernel. `FolderIngest.attach(session_id, ...)` picks
one back up later.

## `FolderIngest`

```python
FolderIngest(
    folder,                      # str | Path — must be an existing directory
    target_catalog_path,         # str
    *,
    data_format,                 # "parquet" | "csv" | "json"   (required)
    intent="read_only",          # "read_only" | "editable"
    conflict_mode="fail",
    table_name=None,
    sub_path=None,
    parallelism=4,
    idempotency_key=None,
    multipart=None,
    table_definition=None,       # TableDefinition | dict
    cleanup_on_failure=False,
    csv_options=None,            # dict, sent only with data_format="csv"
    json_options=None,           # dict, sent only with data_format="json"
    infer_schema=True,
    show_progress=True,
    upload_mode="auto",          # "auto" | "direct" | "proxy"
    client=None,                 # inject an IngestClient (tests, custom transport)
    base_url=None,               # else DDS_BASE_URL
    creds=None,                  # else _DREMIO_USER / _DREMIO_PWD
)
```

| Parameter | Meaning |
|---|---|
| `folder` | Local directory, recursed. `NotADirectoryError` if it is not one. |
| `target_catalog_path` | Where the table should appear in the Dremio catalog. |
| `data_format` | Selects which extensions are scanned — `parquet` → `.parquet`; `csv` → `.csv`; `json` → `.json`, `.ndjson`. Files of other formats in the folder are **ignored**, not an error. |
| `intent` | See [below](#intent--what-a-transfer-leaves-behind). |
| `conflict_mode` | Passed through to the server (`"fail"`, `"replace"`, …); the server decides what a collision means. |
| `table_name` | Override the table's name; omitted, the server derives one. |
| `sub_path` | File this upload under a named sub-folder of the table — see [`sub_path`](#sub_path--read-only-data-that-accumulates). |
| `parallelism` | Concurrent uploads (a `ThreadPoolExecutor`). Must be ≥ 1 — `ValueError` otherwise. |
| `idempotency_key` | Passed to `begin` so a repeated call replays the same session rather than starting a second one. **Dropped automatically** when `retry()` has to start over, or the server would just replay the failed session. |
| `multipart` | `True`/`False` forces the server's multipart decision; `None` leaves it to the server. |
| `table_definition` | The table's column types (and, for later servers, partitioning and sorting) — a `TableDefinition` or a dict in the contract's shape. See [Table definitions](#table-definitions-csvjson-options-inferred-columns). |
| `cleanup_on_failure` | After a commit failure the server *could* retry, cancel the session — deleting its uploaded files — instead of keeping it for `retry()`. Default `False`: kept. With it, a `retry()` afterwards uploads again under a new session. |
| `csv_options` / `json_options` | Parsing hints sent on begin, each only for its own format. CSV: `delimiter`, `quote`, `header` — DDS also accepts `encoding` but passes it on to nothing, so anything other than UTF-8 is refused (see [CSV must be UTF-8](#table-definitions-csvjson-options-inferred-columns)). JSON: `format`, `"lines"` or `"array"`. Inference uses them too. |
| `infer_schema` | For CSV/JSON into an `editable` table without a `table_definition`: infer the columns locally from every file and send them. `False` turns that off. |
| `show_progress` | `False` suppresses the progress bar, and the plan, inferred columns and `diagnose()` lines that are otherwise printed. |
| `upload_mode` | `"auto"` follows the server; `"direct"` uploads only to S3; `"proxy"` sends every file through DDS. `ValueError` otherwise. See [`upload_mode`](#upload_mode--how-the-bytes-travel). |
| `client` / `base_url` / `creds` | Injection points. Pass `client=` and the object never builds its own (and never closes yours). Otherwise `base_url` defaults to `DDS_BASE_URL` and `creds` to `load_creds()`. |

Credentials live in the `IngestClient`, never on `FolderIngest` itself.

### Methods

| Method | Does |
|---|---|
| `run() -> IngestOutcome` | The whole `begin → upload → commit` flow. Cancels the session if an upload fails. Leaves the client open so you can still `status()`/`retry()`/`cancel()`. |
| `begin() -> BeginResult` | Step 1 alone. Raises `FileNotFoundError` if the scan found no files of `data_format`. Sets `session_id`. |
| `dry_run() -> DryRunResult` | Every check `begin` runs, and what commit would do — **nothing uploaded, no session**. See [Checking a transfer first](#checking-a-transfer-first-dry_run-and-diagnose). |
| `diagnose() -> Diagnosis` | Plain-language checks, one per line, ending with a dry run. See [Checking a transfer first](#checking-a-transfer-first-dry_run-and-diagnose). |
| `commit(*, multipart_etags=None) -> CommitResult` | Step 3 alone. `IngestStateError` if nothing has begun. |
| `status() -> StatusResult` | Server-side state of this transfer. |
| `estimate() -> EstimateResult` | Row count + size class of the staged data, between upload and commit. |
| `retry(*, table_definition=None) -> IngestOutcome` | Resume or re-run, optionally with corrected columns — see [below](#managing-resuming-and-abandoning-a-transfer). |
| `cancel() -> None` | Delete the staged data and drop the session. Does **not** drop a table an earlier successful commit already created. Clears `session_id`. |
| `close() -> None` | Release the HTTP client, but only if this object created it. |
| `session_id` (property) | `None` until `begin()` runs or `attach()` supplies one. |
| `FolderIngest.attach(session_id, folder, target_catalog_path, *, data_format, **kwargs)` (classmethod) | Bind to an **existing** session instead of starting one. `folder` must still point at the same local data, so a re-upload is possible if the staged copy is gone. |

`FolderIngest` is a context manager; `with … as job:` calls `close()` on exit.

## `intent` — what a transfer leaves behind

|  | `read_only` | `editable` |
|---|---|---|
| your uploaded files | **kept** — they *are* the table | copied into the catalog, then deleted |
| the table | the folder, registered as the dataset, with a view at the catalog path | an Iceberg table |
| `CommitResult.storage_path` | the Dremio path of the promoted folder | `None` |
| good for | published data, data that accumulates | a table you will write to |

Registering the folder is also what builds Dremio's metadata for it — schema,
file listing, Parquet statistics — and the files keep the shape they were
exported in.

**Permanent read-only storage is a server setting.** Against a server that has
not enabled it, both intents stage-and-load exactly as before, the difference is
only the table's shape, and `storage_path` is `None`. The client sends `intent`
and uploads to whatever targets `begin` issues; **where the bytes land is always
the server's choice**, which is why enabling this needed no new parameter here.

Read the placement back from the server rather than assuming:

```python
status = job.status()
status.placement            # "staged" | "read_permanent"
status.stores_permanently   # True only for "read_permanent"
```

## `sub_path` — read-only data that accumulates

`sub_path` files the upload under a named sub-folder of the table, so one table
can grow a slice at a time:

```python
FolderIngest(
    folder="./bw_2026",                        # a flat folder of parquet files
    target_catalog_path="water_management_resources/bathing_water/bwd/reference",
    data_format="parquet",
    intent="read_only",
    table_name="water_temperature",
    sub_path="2026",                           # → .../water_temperature/2026/
    conflict_mode="fail",                      # refuses if 2026 is already there
).run()
```

- Next year, the same call with `sub_path="2027"` adds to the same table.
- `conflict_mode="replace"` with a `sub_path` redoes **that slice only** and
  leaves the others standing.
- The name is normalised server-side to the EEA convention (lowercase,
  underscores), so `"Q1 2026"` becomes `q1_2026`.
- It applies to a **read-only** ingest whose files are stored permanently; the
  server refuses it otherwise rather than filing the data somewhere else.
- If your local folder already has the structure (`2026/*.parquet`), you need
  nothing: `scan_folder` keeps sub-folders and the server preserves the original
  `rel_path`.

## Table definitions, CSV/JSON options, inferred columns

A table definition says what the table should look like. Every field is
optional, and only the fields you set are sent:

```python
from eea_datalakehouse.dds_ingestion import ColumnDef, TableDefinition

TableDefinition(
    columns=(ColumnDef("station_id", "VARCHAR"), ColumnDef("value", "DOUBLE")),
    partition_by=(),      # accepted; the plan says whether this server applies it
    sort_by=(),
    version=None,         # None → version 1
)
```

A dict in the contract's shape works anywhere a `TableDefinition` does:
`{"columns": [{"name": "value", "type": "DOUBLE"}]}`. Types are Dremio's
(`VARCHAR`, `BIGINT`, `DOUBLE`, `DECIMAL(10,2)`, `TIMESTAMP`, …).
`ColumnDef.nullable` and `description` are accepted by the server but not
applied yet.

**Where it goes.** On `begin`. A server with contract v0.8 answers with a plan
(`BeginResult.plan`); `run()` prints `plan.describe()` — the sentence the
JupyterLab extension shows — and logs a warning for each part of the definition
this server does not apply (`plan.definition_ignored`). A server without plans
ignored it, so it is sent again on `commit`.

**When you give none — inferred columns.** For CSV or JSON into an `editable`
table (the one case where the server builds the table from columns), the client
reads every file with pyarrow, widens the types across files, prints them as
`Inferred columns: …`, and sends them as the definition. `csv_options` and
`json_options` are used for that reading too. A file that cannot be inferred — a
JSON *array*, for one — logs a warning and sends nothing; the server then asks
for the columns. `infer_schema=False` turns inference off. Parquet needs none:
its files carry their own schema.

**CSV must be UTF-8.** Dremio reads CSV as UTF-8 and DDS passes no encoding on
to it, so a Latin-1 or Windows-1252 file would load garbled — and inference alone
would read its text columns as binary. The client refuses such a file before
anything is uploaded: a `csv_options` `encoding` other than UTF-8 (however
spelled: `utf8`, `UTF-8` are fine), or, while inferring columns, a file that is
not valid UTF-8, which the error names. Save the files as UTF-8 first.

**Parquet with columns given.** They are checked against the files' own footers
before anything is uploaded, with the server's comparison rules. A mismatch
raises `IngestApiError` (422, `error_slug="schema_mismatch"`) in the server's
wording, and `where` says it was the local check. The server checks again at
commit and stays the authority.

**After a failure.** `retry(table_definition=…)` sends corrected columns with
the retry — see [Managing](#managing-resuming-and-abandoning-a-transfer).

## `upload_mode` — how the bytes travel

Two parties decide: `FolderIngest(upload_mode=…)` and the server's hint in
`BeginResult.upload_mode` (DDS's `DDS_INGEST_UPLOAD_MODE`).

| `FolderIngest(upload_mode=…)` | the server says | the files go |
|---|---|---|
| `"auto"` (default) | `"proxy"` | all through DDS (`POST /api/v1/ingest/stage`) |
| `"auto"` | `"auto"` — DDS's default | to S3; a file whose upload fails at the network layer goes through DDS instead |
| `"auto"` | `"direct"`, or nothing (an older server: `None`) | to S3 only |
| `"direct"` | anything | to S3 only |
| `"proxy"` | anything | all through DDS |

- **Only a network-layer failure falls back** — an unreachable endpoint, DNS, TLS,
  a timeout. An HTTP error from S3 is a signing or permission problem that DDS
  would not fix, so it is raised.
- **`/stage` has its own size cap.** DDS checks it only when each file arrives,
  so the client asks first (`GET /api/v1/ingest/limits`, at most once per run and
  only when a file may go through DDS). With `"proxy"`, a file over the cap is
  refused before anything is uploaded. With the fallback, such a file is not
  sent through DDS, and the network error carries a note saying why. A server
  without that endpoint is not checked locally.
- A file the server planned as multipart goes through DDS whole.
- `IngestOutcome.upload_mode` is `"proxy"` when every file went through DDS, else
  `"direct"`; `IngestOutcome.files_staged` counts the files that went through
  DDS, so a fallback shows.

## Checking a transfer first: `dry_run()` and `diagnose()`

Neither uploads anything or leaves a session behind.

**`dry_run() -> DryRunResult`** runs every check `begin` runs — privileges, the
target folder, storage, collisions, the table definition — and returns what
commit would do: `plan` (a `BeginPlan`), `key_prefix` and `collision`. The plan
is printed too. It raises what a real `begin` would raise. A DDS server without
dry runs would open a real session instead; the client cancels it straight away
and raises `UnsupportedByServerError`.

**`diagnose() -> Diagnosis`** runs these checks in order and prints one line
each (`✓` passed, `✗` failed, `–` skipped, with the reason):

| Step | Checks | Skipped when |
|---|---|---|
| DDS reachable | `GET /health` | — if it fails, every step after it is skipped |
| Credential accepted | `GET /api/v1/ingest/limits`, which needs a login (`GET /api/v1/ingest` on a server without it) | — |
| File sizes | each file against `max_bytes` (what begin accepts) and `stage_max_bytes` (what `/stage` accepts; failing only with `upload_mode="proxy"`) | DDS reported no limits |
| Storage write test | DDS's own S3 write test (`POST /storage-selftest`) | no verdict yet, or an older server |
| Dry run of this folder | everything `dry_run()` checks, including WRITE on the target | a server without dry runs |

```
✓ DDS reachable: the service answers
✓ Credential accepted: DDS accepts your Dremio credential
✓ File sizes: within DDS's limits (largest: sub/b.parquet, 7 bytes)
✓ Storage write test: DDS can write to its S3 storage
✓ Dry run of this folder: Importing as a new editable table at bio/uploads/t.
```

`Diagnosis.steps` holds one `DiagnosisStep(name, ok, detail)` per line, `ok`
being `None` for a skipped step, and `Diagnosis.ok` is true when no step failed.

## Managing, resuming and abandoning a transfer

```python
with FolderIngest(folder="./my_data", target_catalog_path="catalog.theme.sub",
                  data_format="parquet") as job:
    try:
        job.run()
    except Exception:
        if job.status().is_resumable:
            job.retry()      # re-runs only the step that failed — no re-upload
```

`retry()` decides from the **server's own report**, not from a local guess:

| Server says | `retry()` does |
|---|---|
| `status == "failed"` and `resumable` is truthy | Re-runs only the load step (`POST /ingest/{id}/retry`). The upload is **not** repeated. Returns an `IngestOutcome` with `resumed=True` and `files_uploaded=0`. |
| failed, not resumable, and `stores_permanently` | **Raises `IngestStateError`** — see the warning below. |
| failed, not resumable, staged placement | Opens a fresh session and uploads the folder again, dropping any `idempotency_key` first. |
| `done` | `IngestStateError` — already succeeded, nothing to retry. |
| `pending` / `uploading` / `committing` | `IngestStateError` — wait for it to finish. |
| — this object's own `run()` cancelled the session: after a failed **upload**, or after a failed commit with `cleanup_on_failure=True` | Uploads again under a new session when the failure can pass — a network error, a 5xx, or DDS saying `retryable` — and the cancel went through. Otherwise `IngestStateError`, quoting the error. The server is not asked: that session is gone, or never got all its files. |
| no session at all | `IngestStateError`. |

**With corrected columns.** `retry(table_definition=…)` replaces the
definition this transfer was given. A resumed load uses it without uploading
again — the fix for a `schema_required` or `schema_mismatch` failure (DI-12.6; a
server before that ignores it) — and an upload under a new session sends it on
`begin`.

**What a failure leaves behind.** A failed *upload* cancels the session before
the error reaches you, deleting the files that did land; the error's note says
so, or says the cancel failed and the server's sweep will remove them. A failed
*commit* keeps the session for `retry()` when the server says it can be retried,
unless `cleanup_on_failure=True`; one it cannot retry the server has already
cleaned up.

> ⚠ **A transfer whose files are stored permanently is never re-uploaded
> blindly.** There the upload landed in the table's own folder and stayed, so a
> fresh session would add a *second* copy: the server numbers an incoming name
> that already exists — precisely so an append can never overwrite live data —
> and that protection turns a silent re-run into duplicated rows. Such a
> transfer is resumable server-side by design, so the first row above handles
> it; if the server says it is not, `retry()` raises rather than guessing. The
> message tells you to inspect the table and re-ingest deliberately —
> `conflict_mode="replace"` to redo it, or a `sub_path` for data that belongs
> beside what is already there.

`is_resumable` deliberately reads the server's own `resumable` verdict rather
than re-deriving it from `failed_stage`: a load-stage failure whose bytes the
server could not hold reports `failed_stage="load"` too, and inferring from that
sent `retry()` down the resume path only to be told the staged data was gone. A
server that does not send the field reads as *not* resumable — the safe
direction.

**Prefer `attach()` + `retry()` over re-running `run()`.** Re-running the *same*
session is harmless (identical keys, so a second upload overwrites the first);
starting a *new* one against permanently stored files is not.

### Resume within a run

`run()` skips files the server explicitly names under `raw["uploaded"]` in the
session status, so resume never wrongly drops a file. Note that the server
populates that list for **server-run** transfers (`POST /ingest/folder`); a
client-uploaded session reports nothing there, so re-running one uploads every
file again. A failure to fetch status is swallowed — resume is best-effort and
falls back to uploading everything.

## `IngestOutcome`, `scan_folder`, `ingest_folder`

```python
@dataclass
class IngestOutcome:
    begin: BeginResult | None      # None when the transfer was resumed, not started here
    commit: CommitResult
    files_uploaded: int
    files_skipped: int
    etags: dict[str, str]          # rel_path -> single-shot ETag (multipart entries are not here)
    resumed: bool = False
    upload_mode: str = "direct"    # "proxy" when every file went through DDS
    files_staged: int = 0          # files that went through DDS: all with "proxy", the fallbacks otherwise
```

```python
scan_folder(folder: Path, data_format: DataFormat) -> list[FileSpec]
```

Recurses `folder`, keeps only the extensions for `data_format`, returns
folder-relative POSIX paths (stable across platforms) sorted for deterministic
ordering.

```python
ingest_folder(folder, target_catalog_path, *, data_format, **kwargs) -> IngestOutcome
```

One-shot convenience: build a `FolderIngest`, `run()` it, close it. **If the
transfer might need a `retry()`, drive the class directly** (ideally as a context
manager) so the session handle survives.

`DEFAULT_PARALLELISM` is exported and is `4`.

## `IngestClient` — one method per endpoint

The HTTP layer, with no knowledge of local folders, parallelism or progress
bars. Useful directly if you want the endpoints without the orchestration.

```python
IngestClient(base_url, creds, *, timeout=60.0, http_client=None)
```

| Method | Endpoint / action |
|---|---|
| `begin(*, target_catalog_path, intent, data_format, conflict_mode, files, table_name=None, sub_path=None, idempotency_key=None, multipart=None, definition=None, csv_options=None, json_options=None)` | `POST /api/v1/ingest/begin` → `BeginResult`. A server that took `definition` returns `BeginResult.plan`. Only the fields that were set are sent. |
| `dry_run(*, …)` | `POST /api/v1/ingest/begin` with `dry_run: true` → `DryRunResult`. Takes `begin`'s arguments except `idempotency_key` and `multipart`. Against a server without dry runs: cancels the session it opened and raises `UnsupportedByServerError`. |
| `commit(*, session_id, definition=None, multipart_etags=None)` | `POST /api/v1/ingest/commit` → `CommitResult` |
| `get_status(session_id)` | `GET /api/v1/ingest/{id}` → `StatusResult` |
| `list_sessions(state=None)` | `GET /api/v1/ingest` → `list[StatusResult]`. `state` filters server-side: `"active"`, `"failed"`, `"all"`. |
| `retry(session_id, *, definition=None)` | `POST /api/v1/ingest/{id}/retry` → `StatusResult`. With `definition`, the body `{"definition": …}` replaces the stored one (DI-12.6); without it, no body, as before. |
| `cancel(session_id)` | `DELETE /api/v1/ingest/{id}` |
| `estimate(session_id)` | `POST /api/v1/ingest/estimate` → `EstimateResult`. On a managed catalog the count comes from staged Parquet footers; CSV/JSON have none and report `0`. |
| `stage(session_id, rel_path, data)` | `POST /api/v1/ingest/stage` → `StageResult`. Uploads one file **through the server** — the fallback when the S3 endpoint is not reachable from this kernel. Slower (the bytes transit the server), so prefer `upload_file`. |
| `upload_file(target, data)` | To S3, not DDS. Presigned PUT with the target's signed `headers` — the normal path since contract v0.7 — or, from an older server, a POST policy (form `fields` first, `file` part **last**, so S3 honours the policy conditions). Returns the `ETag` header or `None`. An `httpx.TransportError` (unreachable, TLS, timeout) passes through unchanged. |
| `upload_file_multipart(target, data)` | To S3. Splits `data` into `len(target.parts)` chunks (ceil division; last part takes the remainder) and `PUT`s each to its presigned part URL. Returns `[{"part_number", "etag"}, …]` for `commit`'s `multipart_etags`. Raises if S3 omits an ETag for any part. |
| `health()` | `GET /health` (no login); raises unless DDS answers. |
| `limits(runner="notebook")` | `GET /api/v1/ingest/limits` → dict: `max_bytes` (the largest file begin accepts from this kind of client), `stage_max_bytes` (through `/stage`), `multipart_threshold`. Needs a login. |
| `storage_selftest()` | `POST /storage-selftest` (no login) → dict; `storage_write_test.ok` says whether DDS can write to its S3. |
| `close()` | Closes the `httpx.Client` **only if** the client created it. Also a context manager. |

Both upload methods enforce `target.max_bytes` locally (a `413`
`IngestApiError`) before sending anything.

The `Authorization: Bearer` header is applied to DDS calls only. `__repr__`
deliberately omits it.

## Models

All models parse permissively — unknown keys are ignored, so the client keeps
working when the server adds fields.

| Model | Fields |
|---|---|
| `FileSpec` | `rel_path`, `size`; `as_payload()` |
| `UploadPart` | `part_number`, `url` |
| `UploadTarget` | `rel_path`, `url`, `method="POST"`, `fields`, `headers`, `max_bytes`, `upload_id`, `parts`; `is_multipart` (true when `upload_id` **and** `parts` are set) |
| `S3Plan` | `bucket`, `key_prefix`, `uploads: tuple[UploadTarget, ...]` |
| `BeginResult` | `session_id`, `status`, `s3: S3Plan`, `collision`, `upload_mode` (the server's hint; `None` when it sends none), `plan: BeginPlan \| None` |
| `BeginPlan` | `placement`, `table_path`, `storage_path`, `action` (`"create"`, `"append"`, `"replace"`, `"fail"`), `definition_source`, `definition_applied`, `definition_ignored`; `describe()` — one sentence, the extension's wording |
| `DryRunResult` | `plan: BeginPlan`, `key_prefix`, `collision` |
| `TableDefinition` | `columns: tuple[ColumnDef, ...]`, `partition_by`, `sort_by`, `version`; `as_payload()` sends only what was set |
| `ColumnDef` | `name`, `type` (a Dremio type), `nullable`, `description` |
| `CommitResult` | `session_id`, `status`, `table_path`, `record_count`, `storage_path` |
| `Progress` | `files_done`, `files_total`, `step` |
| `StatusResult` | `status`, `progress`, `raw` — plus the properties below |
| `EstimateResult` | `record_count`, `size_class` |
| `StageResult` | `rel_path`, `key`, `bytes_written` |

`Intent = Literal["read_only", "editable"]`,
`DataFormat = Literal["parquet", "csv", "json"]`.

**`table_path` vs `storage_path` on `CommitResult`.** `table_path` is where the
table is *queried* — the catalog path. `storage_path` is where the files
physically *are* (the Dremio path of the promoted folder), and only for a
read-only ingest whose files are stored permanently. It is `None` for a staged
ingest and against any server predating the field.

**`StatusResult` properties.** `raw` keeps the whole payload, so anything the
client does not model yet is still reachable.

| Property | Meaning |
|---|---|
| `session_id`, `table_path`, `record_count` | pulled from `raw`, `None` if absent |
| `placement` | `"staged"` or `"read_permanent"`. A server without DI-11 omits the field and reads as `"staged"`. |
| `stores_permanently` | `placement == "read_permanent"` — the uploaded files **are** the table |
| `error` | why it failed, including the stage (`"Dremio load failed: …"`, `"S3 upload failed: …"`). `None` unless failed. |
| `is_terminal` | status is `done`, `failed` or `cancelled` |
| `is_resumable` | failed **and** the server's own `resumable` flag is truthy |
| `failed_stage` | where a failed transfer broke: `"upload"` or `"load"` |
| `error_code` | the failure's slug, e.g. `"schema_required"` (contract v0.8) |
| `describe_failure()` | one sentence on the failure — stage, slug, reason, and what a retry would do — worded exactly as the JupyterLab Transfers panel words it |

Lifecycle: `pending` → `uploading` → `committing` → `done`, or `failed` /
`cancelled`.

## Credentials

```python
from eea_datalakehouse.dds_ingestion import DremioCreds, load_creds, load_base_url

creds = load_creds()          # _DREMIO_USER / _DREMIO_PWD, or a dict you pass as env=
base  = load_base_url()       # DDS_BASE_URL, trailing slash stripped
```

Both raise `MissingCredentialsError` when the variables are absent. Both take an
optional `env=` mapping so tests need not mutate process state.

`DremioCreds` is a frozen dataclass of `username` + `_password` (the Dremio PAT,
sent as the bearer token). Its `__repr__`/`__str__` render the password as
`***`, so it cannot reach notebook output, tracebacks or log handlers. Read it
deliberately via the `.password` property.

## Progress bars

`make_progress_bar(total, desc="Uploading") -> ProgressBar` returns a real
`tqdm.auto` bar when tqdm imports, and otherwise a minimal reporter that prints
`desc: done/total` lines. Both satisfy the same `ProgressBar` protocol
(`update(n=1)`, `close()`), so the caller never branches. `show_progress=False`
skips the bar altogether.

## Ingest errors

```
RuntimeError
├── MissingCredentialsError      credentials or base URL absent
├── IngestStateError             asked for locally-impossible state, before any HTTP call
└── IngestApiError               a DDS call returned non-success
    ├── StorageUnavailableError  DDS cannot write to its own S3 (503)
    ├── UnsupportedByServerError the DDS server predates the feature (e.g. dry runs)
    └── S3UploadError            a presigned upload failed — NOT a DDS error
```

**What every `IngestApiError` tells you**, besides its message:

| Attribute | Meaning |
|---|---|
| `error_slug` | DDS's machine-readable `error`, e.g. `schema_required`, `schema_mismatch`; `""` when the body had none |
| `retryable` | whether repeating — for a failed transfer, `retry()` — can succeed unchanged; `None` from a server before contract v0.8 |
| `failed_stage` | on a failed commit from `FolderIngest`: `"upload"` or `"load"`, from the session; `None` otherwise |

A failed commit also carries the session's own failure text as an exception
note — `StatusResult.describe_failure()`, the sentence the Transfers panel
shows. A failed upload carries a note on what happened to its session.

**`IngestApiError`** carries `status_code`, `message` and `where` — the call that
failed, e.g. `POST /api/v1/ingest/begin`. `where` is in the message because a
transfer makes several calls to two different systems, and "API error 500" alone
does not say which one broke. Error bodies are quoted back to you but capped at
500 characters, so an HTML error page cannot bury the status line it came with;
a DDS `{error, message, path}` body is unwrapped to its `message`.

**`StorageUnavailableError`** — the service verified, *with its own
credentials*, that the object storage behind the catalog rejects writes, and
said so instead of handing out upload targets that every file would fail
against. Nothing has been uploaded. Not a fault in your folder, your target or
your rights; the fix is an administrator's (credentials, bucket policy,
endpoint) and the transfer can simply be re-run once storage works. Raised
**only** on DDS's own `storage_unavailable` slug, so a 503 from a proxy or load
balancer in front of the service stays a plain `IngestApiError` rather than
being mislabelled a storage fault. Its message is the server's own text,
unwrapped — the class name carries the rest.

**`S3UploadError`** — the bytes go straight from your machine to object storage,
so this failure belongs to S3 (or whatever proxy fronts it): a rejected policy,
an unreachable endpoint, a gateway error. It adds `rel_path` and `url`, and its
message names both, so you can tell "your upload never landed" from "the
Document Service refused the request".

**`UnsupportedByServerError`** — the DDS server is older than the feature a call
needs (e.g. `dry_run()`). A subclass of `IngestApiError`, so code that catches
that keeps working.

**`IngestStateError`** is raised locally, before any HTTP call — committing a
transfer that never began, retrying one that is still running, cancelling
nothing, or retrying an upload that would fail the same way (its message quotes
the upload error).

## Notebook helpers (`dds_ingestion.common`)

Shared helpers for the ingest notebooks. Not exported from
`dds_ingestion/__init__.py` — import them from the submodule.

### `common.dremio_identity` — making `%init` work everywhere

`%init` is the EEA convention for getting Dremio credentials into a notebook;
the JupyterLab Dremio extension registers it as a line magic and binds
`DREMIO_USERNAME`, `DREMIO_TOKEN`, `DREMIO_URL`, `DREMIO_PASSWORD`, plus service
endpoints (`DDS_BASE_URL`, `SCHEDULER_URL`) as kernel globals. The magic ships
*with the Hub extension*, which this project's own container deliberately does
not have — a bare `%init` there fails with "Line magic function %init not
found".

| Function | Does |
|---|---|
| `ensure_init_magic(verbose=True) -> str` | Guarantees `%init` resolves. Returns `"extension"` when the real Hub magic is in play, `"fallback"` when the local stand-in was registered — it never shadows the extension. Call it *before* the `%init` line; the same cell is enough, since magics are looked up at run time. |
| `endpoint(name, aliases=()) -> str` | Resolves one endpoint, most authoritative source first: the JupyterLab "Dremio Catalog" panel → whatever `%init` bound into the kernel → the process environment (including `aliases`). `""` when nothing supplies it; never a trailing slash. |
| `dds_credentials(username=None, token=None) -> DremioCreds` | `DremioCreds` built from the resolved identity. **Use this in a Hub kernel**: `load_creds()` reads `_DREMIO_USER`/`_DREMIO_PWD`, which a Hub kernel does not set (it exports `DREMIO_USERNAME`/`DREMIO_TOKEN`), so the default path raises exactly where the transfer starts. Pass the result as `FolderIngest(creds=…)`. |
| `resolve() -> dict[str, str]` | The identity and endpoints, panel first then environment. Returns values; prints nothing. |
| `present() -> dict[str, bool]` | Which parts are set — safe to print, carries no values. |
| `read_lab_settings()`, `settings_paths()` | The JupyterLab settings side of the above. |

No function here prints, logs or returns a secret — only whether one is present.

### `common.catalog` — create the target before ingest writes into it

Ingest does not create its target: `commit` runs `CREATE TABLE` + `COPY INTO`,
and Dremio refuses both if the containing namespace is absent
(`Namespace does not exist: …`).

```python
ensure_catalog_path(base_url, token, path, *, dry_run=False,
                    timeout=120.0, dataflow_depth=DATAFLOW_DEPTH)
    -> list[tuple[str, str]]        # (path, "exists" | "created" | "would create")

print_report(report)                # one line per level, indented by depth
```

Two decisions worth knowing:

- **Existence is established by listing the parent, not by probing the path.** A
  missing folder does not answer 404 — DDS returns 502 wrapping Dremio's
  "catalog lookup failed (400)". Treating any error as "missing" would turn a
  DDS or Dremio outage into a burst of folder creation, so this only ever acts
  on a `200`.
- **Taxonomy levels are never created.** For a
  `{source}/{domain}/{subdomain}/{dataflow}/…` path, the source, domain and
  subdomain must already exist and a missing one raises. Only the dataflow level
  and below are created — which is exactly what a new dataflow legitimately
  owns. A missing domain is a taxonomy question to raise, not a folder to
  create.

Folders are created **through the Document Service**, never Dremio's API
directly, because DDS is what owns catalog structure in this platform (it is
also what writes the folder's wiki page for meta tags).

---

# Part 2 — `catalog`

```python
from eea_datalakehouse.catalog import Catalog, CatalogOperationError, EngineStartingError
```

Dremio catalog operations once data has landed. `base_url`/`token` here are
**Dremio's own**, not DDS's.

## `Catalog` and its two transports

```python
Catalog(
    base_url, token, *,
    username=None,            # required only for datacopy/datamove
    flight_location=None,
    timeout=900.0,
    executor=None,            # inject a SqlExecutor (e.g. a test fake)
    flight_executor=None,
    catalog_rest=None,
)
```

A `Catalog` holds **three** connections and picks per operation, not per
instance:

| Transport | Used by |
|---|---|
| **Arrow Flight SQL** | `datacopy`, `datamove` — the operations that actually move data, not just metadata |
| **Dremio REST SQL Jobs API** (`/api/v3/sql`) | `table2view`, `draft2version`, `publishversion`, `deleteview`, `gettablesfrom`, `gettableitemsfrom` |
| **Dremio REST catalog API** (`/api/v3/catalog`) | folders, wiki and tags — none of which have a SQL or Flight equivalent at all |

**This split is fixed, not env-var-driven.** `Catalog` does *not* consult
`EEA_CATALOG_TRANSPORT`; that variable only affects `resolve_executor()`, for a
caller using the module-level `operations.*` functions with an executor they
manage themselves. Override either executor directly with
`executor=`/`flight_executor=`.

**`username` is a Flight-only requirement.** REST authenticates with `token`
alone; Flight authenticates via a basic-auth handshake (username + `token` as
the password), not a raw bearer header — some Dremio deployments reject the
latter on the Flight endpoint even though REST accepts it. So `username=` is
required to actually call `datacopy`/`datamove` (a clear
`CatalogOperationError` otherwise) and optional if you never do.

Constructing a `Catalog` connects nothing: `FlightSqlExecutor` opens its gRPC
channel and performs the handshake lazily, on first real use, so a `Catalog`
that never copies data pays nothing for the Flight stack.

```python
with Catalog(DREMIO_URL, DREMIO_TOKEN, username=DREMIO_USERNAME) as catalog:
    catalog.table2view("bwd.consumer", "bwd.draft.bw_assessment",
                       idempotency_key="bwd-v2025_1")
```

See [Lifecycle and cleanup](#lifecycle-and-cleanup) for why `close()` matters.

## `idempotency_key` and the retry contract

**Every operation takes `idempotency_key=` and it is keyword-only and
required.** It exists because of one specific failure mode:

> A cold Dremio engine can take minutes to answer. That is a *stall*, not a
> failure. Rather than blocking a caller for that long, the executors raise
> `EngineStartingError`, the operation records the attempt to disk under your
> key, and you pick it up later from a fresh cell or a separate process.

```python
try:
    catalog.table2view("bwd.consumer", "bwd.draft.bw_assessment",
                       idempotency_key="bwd-v2025_1")
except EngineStartingError:
    ...                                   # later, possibly a new process:
    catalog.retry_pending("bwd-v2025_1")
```

- On `EngineStartingError` the attempt is recorded in
  [`retry_state`](#retry_state) with the operation, target, arguments, attempt
  count and *which step* it reached, then re-raised.
- On full success the remembered attempt is cleared.
- `retry_pending(key)` looks the record up and dispatches back to the same
  function with the same arguments. `KeyError` if nothing is pending under that
  key. `Catalog.retry_pending` routes the retry back over **Flight** for
  `datacopy`/`datamove` and REST for everything else.
- Nothing sensitive is persisted: executors, clients and credentials are never
  written to the state file. They are re-supplied at retry time and matched to
  the operation's parameters **by name** — some operations (`getwikifrom`, …)
  take no `executor` at all, so it is only passed to those that declare it.

**Multi-step operations are safe to retry from the start**, because every
statement is either idempotent (`IF EXISTS`) or fails loudly rather than
silently duplicating. Each operation submits **one** SQL statement per
`execute()` call — Dremio's SQL job API and Flight SQL endpoint both expect a
single statement — so a multi-step operation runs as a short sequence.

> ⚠ **The one real gap.** If `datamove`'s `CREATE` succeeds but the following
> `DROP` then stalls on a starting engine, a blind retry's `CREATE` fails with
> "already exists". The recorded error message says which step it reached, but
> resolving that specific case is left to you: check the catalog, then call
> `retry_pending` or finish the `DROP` by hand.

## Operation reference

Each is a method on `Catalog` and also a module-level function in
`catalog.operations` taking an executor and/or a `CatalogRestClient` explicitly.
All take `*, idempotency_key: str`.

### Table / view lifecycle

**`table2view(view_path, source_path, *, create_target_folder=True)`**
The one-time `DROP TABLE` + `CREATE VIEW` transition for a location that started
as a physical table (straight off ingest) and is moving to being a view over
some other table. Checks `source_path` exists first, so a typo fails with a
clear message rather than a confusing `CREATE VIEW` error. With
`create_target_folder=True` (the default) it also ensures `view_path`'s
containing folder exists, creating missing levels — that needs a
`catalog_rest`; pass `False` if you know the folder is there. The `DROP` is
`IF EXISTS`, so the whole thing is safe to re-run from the start.

**One table to one view — folders are not accepted.** Both paths name a single
catalog entry. `source_path` is checked against `INFORMATION_SCHEMA."TABLES"`,
which lists tables and views only, so passing a folder raises
`CatalogOperationError: source '…' does not exist` before anything is dropped or
created. Nothing in the library fans an operation out over a folder's contents —
to convert a whole folder, loop over `gettablesfrom`:
[recipe](#turn-every-table-in-a-folder-into-views-in-another-folder).

**`createviewas(view_path, sql, *, overwrite=False, create_target_folder=True)`**
— `table2view`'s general form: a view over any query body you hand it, not just
`SELECT * FROM <source>`. `sql` is the body only (no `CREATE VIEW` prefix) and is
sent through unchanged — quoting and correctness are yours, since the text
usually comes back out of Dremio itself. `overwrite=True` drops whatever is
actually at `view_path` first, detecting whether it is a table or a view.

**`deleteview(view_path)`** — `DROP VIEW IF EXISTS`. Idempotent. If `view_path`
is actually a table, this fails loudly rather than dropping the wrong kind of
entry.

**`deletetable(table_path)`** — `DROP TABLE IF EXISTS`; `deleteview`'s
counterpart, failing just as loudly on the wrong kind of entry.

**`draft2version(draft_path, version_path)`** and
**`publishversion(consumer_view_path, version_path)`** — **not implemented**;
both raise `NotImplementedError`. See [Not implemented yet](#not-implemented-yet).

### Copying and moving data — always Arrow Flight

**`datacopy(source_path, target_path, *, overwrite=False, create_target_folder=False)`**
`CREATE TABLE … AS SELECT`.

**`datamove(source_path, target_path, *, entry_type=None, overwrite=False, create_target_folder=False)`**
Copy, then drop the source. Implemented as copy-then-drop rather than a native
Dremio catalog rename because this deployment is Community Edition; a SQL-only
implementation was chosen over assuming a specific REST rename endpoint.

Shared behaviour:

- **Source is checked first.** `datamove` auto-detects `entry_type`
  (`"TABLE"`/`"VIEW"`) from `INFORMATION_SCHEMA.TABLE_TYPE` when you do not pass
  it, and that query doubles as the existence check. Getting `entry_type` wrong
  yourself fails the `DROP` outright with "is not a VIEW"/"is not a TABLE" —
  auto-detection sidesteps that. Re-querying on a retry is safe: the source is
  untouched until the final `DROP`. Pass it explicitly only to skip the lookup
  or override a detection you distrust.
- **`cp source dest/` semantics.** If `target_path` is an existing *folder*
  rather than a specific table/view path, the source's own name is appended to
  it, landing the copy inside that folder. Needs a `catalog_rest`; without one,
  `target_path` is used exactly as given.
- **`overwrite=False` (the default)** checks the (possibly folder-adjusted)
  target explicitly and raises `CatalogOperationError` if it exists — so you
  never silently overwrite something, and you get a better message than a bare
  CTAS failure. **`overwrite=True`** detects what is *actually* there (it might
  be a view, not a table, from an earlier operation) and drops it with the
  matching verb.
- **`create_target_folder=True`** ensures the target's containing folder exists
  first; requires a `catalog_rest`.
- **`datamove` verifies afterwards.** Neither `CREATE` nor `DROP` failing to
  raise is proof either happened server-side — Flight SQL's completion semantics
  for a no-result-rows DDL statement are unverified here — so it checks
  `target_path` actually exists in the catalog before declaring success.

### Whole folders — `catalog.foldertree`

`folderscan` / `foldercopy` / `foldermove` (also methods on `Catalog`). These
are orchestration over the operations above rather than one statement each, so
they live in their own module — see its docstring for the assumptions they rest
on, and the [recipe](#move-a-whole-folder-somewhere-else) for how to drive them.

**`folderscan(catalog, source_path, target_path) -> FolderTree`** — read-only.
Every sub-folder, table and view under `source_path`, mapped to where it would
land, with each view's SQL already rewritten and the views in dependency order.
Raises if the two paths are the same, or if either is inside the other.

```python
@dataclass(frozen=True, slots=True)
class TreeEntry:
    source_path: str
    target_path: str
    kind: Literal["FOLDER", "TABLE", "VIEW"]
    view_sql: str | None              # views: as Dremio stores it
    view_context: tuple[str, ...]     # views: its sqlContext, if known
    remapped_sql: str | None          # views: what will actually be created
    depends_on: tuple[str, ...]       # views: what it selects from, in this subtree
    notes: tuple[str, ...]
```

**`foldercopy(catalog, source_path, target_path, *, dry_run=False,
overwrite=False, skip_existing=True, verify_views=True, copy_metadata=True,
qualify_external=True, tree=None, on_progress=None) -> FolderTransferReport`**
— reproduces the subtree and proves it. Folders first (empty ones included),
then tables via `datacopy` with `COUNT(*)` compared on both sides, then views
recreated from their own SQL with references into the subtree remapped. A
per-entry failure is recorded, not raised — the sweep finishes so you see the
whole picture.

`skip_existing=True` is what makes a re-run cheap: a target table already there
**with the source's row count** is reported `skipped`. One with a *different*
count fails that entry rather than being silently accepted.

**`foldermove(catalog, source_path, target_path, *, delete_source_root=True,
…)`** — `foldercopy`, then removal of the source **only if `report.verified`**:
views in reverse dependency order, then tables, then folders deepest-first.
`deletefolder(cascade=True)` is deliberately not used — a folder that will not
delete because something unexpected is inside it is a fact worth surfacing.

**`FolderTransferReport`** — `folders` / `tables` / `views` are lists of
`EntryResult(entry, status, source_rows, target_rows, sql, error, notes)`, where
`status` is `planned` / `created` / `copied` / `skipped` / `failed`.
`report.verified` is the green light, `report.failures` is what to fix,
`report.deleted` and `report.delete_errors` say what came off the source, and
`report.summary()` prints the lot.

**`remap_view_sql(sql, *, source_path, target_path, entries, context=(),
external_exists=None) -> RemapResult`** — the view rewriter on its own. Textual,
never a parse of Dremio's dialect: it skips string literals and comments, and
rewrites only identifier chains it can resolve — against a known entry, against
the source root, or against the view's own `sqlContext`. Its docstring lists the
four rules in order, and the one corner it can get wrong.


### Discovery

**`gettablesfrom(schema_path) -> list[str]`** — every table and view under
`schema_path` at any depth, as full dot-separated paths. One query covers the
whole subtree (`TABLE_SCHEMA` equal to `schema_path` or `LIKE schema_path.%`,
since SQL `LIKE`'s `%` matches across dots), with `_`/`%` in real folder names
escaped so an underscore is not read as a wildcard. Full paths rather than bare
names because two subfolders can each hold a table of the same name.

**`getentriesfrom(schema_path) -> list[CatalogEntry]`** — the same sweep, with
`TABLE_TYPE` selected as well, so each path carries whether it is a `"TABLE"` or
a `"VIEW"`. Sorted by path. This is what a folder-level operation needs: copying
a view with `CREATE TABLE … AS SELECT` silently materialises it into a table.

**`entryexists(path) -> bool`** — whether `path` is a table or view Dremio knows
about, via `INFORMATION_SCHEMA` (so REST or Flight both answer). Fetches no rows,
so it costs nothing on a large table. Unlike `CatalogRestClient.exists`, it does
not see folders, spaces or sources.

**`getviewsql(view_path) -> ViewDefinition`** — the SQL a view is defined by.

```python
@dataclass(frozen=True, slots=True)
class ViewDefinition:
    sql: str                      # the query body, as Dremio stores it
    context: tuple[str, ...]      # the sqlContext it was written against; () = unknown
```

Two sources, better one first: Dremio's own catalog entity (which carries the
`sqlContext` as well as the text), then `INFORMATION_SCHEMA."VIEWS"` (text only,
so `context` comes back empty — meaning *unknown*, not *none*). Raises if
neither answers, rather than letting a caller replace a view with a guess.

**`getfoldersfrom(path, *, recursive=True) -> list[str]`** — the folders under
`path`, shallowest first (the order they have to be created in on the other
side). REST-only: folders are not in `INFORMATION_SCHEMA`, so an **empty** folder
is invisible to `gettablesfrom`/`getentriesfrom` and can only be found this way.

**`gettableitemsfrom(table_path) -> TableInfo`** — one table or view's schema and
row count, **without fetching any rows**.

```python
@dataclass(frozen=True, slots=True)
class TableInfo:
    schema: dict[str, str]     # column name -> Dremio DATA_TYPE, in ordinal order
    row_count: int             # SELECT COUNT(*) — the true count
```

`CatalogOperationError` if the path does not exist — checked via
`INFORMATION_SCHEMA."COLUMNS"`, which also supplies the schema, so the existence
check pays for itself.

### Wiki and tags — REST only

Dremio's wiki is plain markdown with no structured-metadata concept of its own,
and Dremio's tags/labels exist **only on tables and views, not folders**. The
library therefore offers two non-overlapping schemes, gated to the level each
belongs to:

| Level | Use | Gate |
|---|---|---|
| tables / views | `gettagsfrom` · `settagsto` (Dremio's real tags) | raises `CatalogOperationError` on a folder |
| folders | `setmeta2wiki` · `getmetafromwiki` (tags embedded in the wiki text) | raises `CatalogOperationError` on a table/view |

**`getwikifrom(path) -> str`** — the wiki text. Raises if `path` does not exist
*or* has no wiki at all (the message distinguishes which).

**`setwikito(path, text, *, tags=None)`** — create or overwrite the wiki text.
Raises if `path` does not exist. `tags`, if given, is a list of
`{"tag_name", "tag_value", "tag_title"}` dicts (a missing key raises
immediately) rendered into a `# Meta Data` section appended to `text`: one
human-readable `title : value` line per tag, plus the same data machine-readable
as `<meta><tag name=… value=… title=…/>…</meta>`. The rendered text — metadata
included — is what a retry remembers and re-sends; `tags` itself is never
persisted.

**`deletewiki(path)`** — idempotent: a missing `path`, or one with no wiki at
all, is a no-op rather than an error. Dremio's collaboration API has no
delete-wiki endpoint, so this clears the text to empty.

**`setmeta2wiki(path, *, tags=None, overwrite=True)`** — **folders only**.
Updates just the `# Meta Data` section of the wiki already at `path`, keeping
whatever text comes before it untouched. `overwrite=True` (the default) replaces
the whole section with one built fresh from `tags`; `overwrite=False` merges
`tags` in after the tags already there (parsed back out of the existing `<meta>`
block), **with no deduplication** — a repeated `tag_name` appears twice. If
`path` has no wiki yet, starts from empty base text rather than raising. On
retry, this re-runs from scratch — re-fetching and re-merging against whatever
the wiki looks like by then — rather than resending a stale precomputed string.

**`getmetafromwiki(path, tag_name=None, field=None)`** — **folders only**. Reads
those tags back. Without `tag_name` — or with one that matches nothing — returns
**every** tag as a list of dicts. With a matching `tag_name`, returns a single
dict: both `tag_value` and `tag_title` if `field` is omitted, or `tag_name` plus
just the one `field` (`"tag_value"` / `"tag_title"`) you asked for. A wiki
hand-edited outside this format simply yields no tags, not an error.

**`gettagsfrom(path) -> list[str]`** — **tables/views only**. Zero tags is a
normal state and returns `[]`; only a missing or wrong-kind path raises.

**`settagsto(path, tags)`** — **tables/views only**. **Replaces** the tag set —
pass the union yourself (via `gettagsfrom` first) if you want to keep the
existing ones.

**`deletetags(path, tags)`** — removes just the given tags, leaving the rest
intact (fetch-remove-set, since there is no delete-tags endpoint). Raises if
`path` does not exist. *Unlike `gettagsfrom`/`settagsto`, this one does not gate
on the entity being a table or view.*

### Folders — REST only, idempotent

**`createfolder(path, *, create_parents=False)`** — an already-there folder is
left alone, not an error. `create_parents=False` (the default) raises if the
parent is missing, rather than silently creating a deep new path;
`create_parents=True` creates every missing level. The first (leftmost) segment
— the Dremio space or source itself — is **never** created automatically, only
folders under it.

**`deletefolder(path, *, cascade=False)`** — an already-gone folder is not an
error. `cascade=False` (the default) raises if the folder still has contents
(`rmdir` vs `rm -r`); `cascade=True` deletes every table/view and subfolder
inside first, depth-first, then the folder itself.

### Retrying

**`retry_pending(idempotency_key)`** — re-attempt whatever last stalled under
that key, with the arguments it was originally called with. `KeyError` if
nothing is pending.

## Executors (`catalog.sql`)

Both executors implement the same tiny protocol, so an operation never cares
which it got:

```python
class SqlExecutor(Protocol):
    def execute(self, sql, *, idempotency_key=None) -> SqlResult: ...
    def fetch_all(self, sql, *, idempotency_key=None) -> list[dict[str, Any]]: ...

@dataclass(frozen=True, slots=True)
class SqlResult:
    row_count: int | None
    job_id: str | None = None
```

Neither executor knows about idempotency keys or retry bookkeeping — that lives
in `operations`, which is what decides whether and how to retry.

**`RestSqlExecutor(base_url, token, *, timeout=900.0, poll_interval=2.0, http_client=None)`**
Submits to `POST /api/v3/sql`, then polls `GET /api/v3/job/{id}` until
`COMPLETED` / `FAILED` / `CANCELED`. `timeout` bounds the **whole
submit-and-poll cycle**, not any single HTTP call — a cold engine shows up as
many fast polls returning a non-terminal state, not one slow call, so the budget
has to span the loop (the per-request timeout is a deliberate 30s).
`fetch_all` pages results 500 rows at a time.

**`FlightSqlExecutor(location, username, token, *, timeout=900.0, flight_client=None)`**
Authenticates via Flight's HTTP-basic handshake
(`authenticate_basic_token`, username + PAT as the password), once, caching the
resulting session token for the executor's lifetime. Holds **one** persistent
gRPC channel for its whole lifetime rather than reconnecting per statement —
`datacopy`/`datamove` issue several statements each (existence check, folder
creation, the CTAS/DROP), and a fresh handshake per step would be pure overhead.
**Call `close()` when done, or that channel leaks.**

**`resolve_executor(base_url, token, *, username=None, flight_location=None, timeout=900.0)`**
REST unless `EEA_CATALOG_TRANSPORT=flight`. For callers driving `operations.*`
themselves — `Catalog` does not use it.

**`resolve_flight_location(base_url, flight_location=None)`** — explicit
argument, else `DREMIO_FLIGHT_LOCATION`, else a best-effort default derived from
`base_url`'s host: an `https` base URL yields `grpc+tls`, plain `http` yields
`grpc`, on port `32010`. (Connecting with plain `grpc` to a TLS-only endpoint
fails with a misleading "Socket closed" rather than a clear protocol error,
hence borrowing the TLS-ness.) **The port is Dremio's documented default and has
not been verified against this project's deployment** — pass `flight_location`
explicitly, or set `DREMIO_FLIGHT_LOCATION`, if either guess is wrong for you.

Timeouts on either transport raise `EngineStartingError`; a rejected submission,
a failed or cancelled job, or an unpollable one raise `CatalogOperationError`.

## `CatalogRestClient` (`catalog.rest`)

Dremio's own `/api/v3/catalog`, for the things with no SQL or Flight equivalent.
Same `base_url`/`token` as the SQL executors — Dremio's catalog API and its SQL
Jobs API are both under the one REST root.

| Method | Does |
|---|---|
| `exists(path)` | whether `path` is any entity |
| `is_folder(path)` | whether it is specifically a folder |
| `is_table_or_view(path)` | whether it is a Dremio "dataset" |
| `get_wiki(path)` / `set_wiki(path, text)` | the entity's wiki text |
| `get_tags(path)` / `set_tags(path, tags)` | the entity's tags (`set_tags` **replaces**) |
| `create_folder(path)` | returns whether it was *newly* created (`False` on 409 — already there) |
| `ensure_folder_path(path)` | creates every missing level, returns the ones actually created |
| `delete_folder(path, *, cascade=False)` | idempotent; cascade deletes contents depth-first |
| `close()` | closes the `httpx.Client` if this object created it |

Two behaviours worth knowing:

- **`is_folder` / `is_table_or_view` never raise.** Some Dremio source types
  (its internal Arctic/Nessie-backed catalog sources) reject by-path lookups
  into nested items outright — a `400`, not a `404` — rather than answering "not
  found". These convenience checks cannot tell that apart from "does not exist",
  so any lookup failure is treated as "not a match".
- **`ensure_folder_path` creates without checking first**, tolerating the `409`,
  for the same reason: create-and-tolerate-already-there sidesteps needing a
  working existence check at all against those sources. It costs one extra POST
  per level that already existed — worth it, since the alternative is failing
  outright where the check itself is broken.
- **Wiki/tag writes try create-then-update.** Dremio's collaboration API is
  optimistic-concurrency-controlled by a `version` field, but only once a record
  exists: sending `version: 0` when there is none is rejected ("Tried to update
  version 0, found no tag"). The catch — also confirmed against a real call — is
  that `GET .../wiki` can answer `200` with default content even when nothing
  was ever set, so a GET beforehand cannot reliably tell "real record" from
  "nothing yet". So the simple create is attempted first; only if rejected does
  it fetch the actual version and retry once as an update.

## `retry_state`

Deliberately a flat JSON file, not a database — one developer's local retry
bookkeeping, not shared state. Default path
`~/.cache/eea_datalakehouse/catalog_retry_state.json`, overridable with
`EEA_CATALOG_RETRY_STATE`. Written atomically via a temp file + `os.replace`, so
a crash mid-write cannot corrupt it; a corrupt or unreadable file reads as empty
rather than failing a catalog operation.

```python
from eea_datalakehouse.catalog import retry_state

retry_state.list_pending()          # -> list[PendingOperation]
retry_state.get(key)                # -> PendingOperation | None
retry_state.clear(key)              # forget one (operations do this on success)
```

```python
@dataclass
class PendingOperation:
    idempotency_key: str
    operation: str          # "table2view", "datacopy", …
    target: str
    attempts: int
    first_attempted_at: str # ISO-8601 UTC
    last_attempted_at: str
    last_error: str         # includes which step it reached, e.g. "step 3/4: …"
    params: dict[str, Any]  # the original arguments — JSON-serialisable only
```

## Catalog errors

**`CatalogOperationError`** — the operation was rejected or errored for a real
reason: bad SQL, missing path, permission denied, wrong entity kind, a target
that already exists, a missing `catalog_rest` where one was needed.

**`EngineStartingError`** — a SQL call stalled or timed out, most likely because
a Dremio engine is cold-starting. **Not a failure.** It carries the
`idempotency_key`, the attempt has been recorded, and the right response is to
retry later rather than to treat the operation as lost. See
[the retry contract](#idempotency_key-and-the-retry-contract).

---

# Cross-cutting

## Secret handling

- `DremioCreds.__repr__`/`__str__` redact the PAT as `***`.
- `IngestClient.__repr__` omits its `Authorization` header — deliberately, so
  printing or logging the client (or hitting it in a debugger or an unhandled
  traceback) never renders the bearer PAT.
- `RestSqlExecutor`, `CatalogRestClient` and `Catalog` do the same in their own
  `__repr__`.
- The DDS bearer token is applied to DDS API calls **only** — never to a
  presigned S3 upload, which carries its own auth in the URL.
- `retry_state` persists only JSON-serialisable operation arguments. Executors,
  clients and credentials are never written to that file and are re-supplied at
  retry time.

## Lifecycle and cleanup

Both halves own HTTP resources and both follow the same rule: **an injected
client is never closed by the library; one the library built is.**

```python
with FolderIngest(...) as job:        ...    # job.close()
with Catalog(...) as catalog:         ...    # catalog.close()
with IngestClient(...) as client:     ...    # client.close()
```

`Catalog.close()` is idempotent and disposes the REST executor, the Flight
executor and the catalog REST client. It matters most for Flight, which holds a
persistent gRPC channel that leaks if merely dropped.

For the case a caller forgets — a debug script that just exits, a container
receiving `SIGTERM` — every `Catalog` registers itself for best-effort cleanup
at interpreter exit **and** on `SIGTERM`/`SIGINT`. The signal handlers are
installed once globally (not per instance, so instances do not stack handlers),
only from the main thread (`signal.signal` requires it), and they chain to
whatever handler was previously installed — e.g. Jupyter's own `SIGINT`
handling. Off the main thread the install is silently skipped and `atexit` still
covers a normal shutdown. One instance failing to close never stops the rest
from being disposed.

## Unverified assumptions

The code flags these itself; they are repeated here so nobody builds on them
unknowingly. Check against a real Dremio deployment before relying on them.

| Where | Assumption |
|---|---|
| `catalog/rest.py` (module) | The v3 catalog API's by-path lookup, folder creation, wiki/tag collaboration and folder-deletion request/response shapes are inferred from Dremio's documented API, not confirmed against this project's instance. |
| `CatalogRestClient.delete_folder` | The cascade path assumes a container child's JSON carries `type="CONTAINER"` / `containerType="FOLDER"` as the docs describe. |
| `catalog/sql.py` | `DEFAULT_FLIGHT_PORT = 32010` is Dremio's documented default, unverified here. |
| `datamove` | Flight SQL's completion semantics for a no-result-rows DDL statement are unverified — which is exactly why `datamove` verifies the target exists afterwards instead of trusting the executor's silence. |
| `gettablesfrom` | `INFORMATION_SCHEMA."TABLES"` covering both tables and views is the ANSI convention, unverified against this project's Dremio version. |
| `setwikito` / `settagsto` | The exact semantics of the collaboration API's `version` field are unverified (hence the create-then-update fallback). |

## Not implemented yet

- **`draft2version(draft_path, version_path)`** — promoting a draft table into a
  permanent version. Raises `NotImplementedError`.
- **`publishversion(consumer_view_path, version_path)`** — repointing a
  consumer-facing view at a version. Raises `NotImplementedError`.

Both are already wired through `Catalog`, `operations._OPERATIONS` and the
package exports, so only the bodies are missing. Note the intended division of
labour: `table2view` is the **one-time** transition for a location that started
as a physical table; `publishversion` is the **ongoing, idempotent** repoint once
a location is already a view.

---

## Development

```bash
pip install -e ".[dev]"
pytest                 # testpaths = tests, addopts = -q
ruff check .           # E, F, I, UP, B, C4, SIM — line length 100, target py311
mypy src               # strict, minus warn_return_any
```

Tests live under `tests/catalog/` and `tests/dds_ingestion/`; the ingest suite
mocks httpx with **respx**, so no test makes a real network call.

`debugger/debug_run.py` is a VS Code debug entrypoint that exercises the ingest
path fully mocked (DDS API + presigned S3 endpoints) against a temp folder of
fake parquet files — safe to re-run as often as you like.

Release process — including the floating `main`/`staging` tags and the fact that
version tags are **not permanent** — is documented in
[`../README.md`](../README.md#releasing-a-new-version).
