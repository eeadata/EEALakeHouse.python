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

[![Latest release](https://img.shields.io/github/v/release/eeadata/EEALakeHouse.python?label=latest%20release)](https://github.com/eeadata/EEALakeHouse.python/releases/latest)

The badge above always shows the current latest release tag — substitute it for `v0.1.5` below
if it's moved on since this was written (or check the [Releases page](https://github.com/eeadata/EEALakeHouse.python/releases/latest) directly).

```bash
# latest release (currently v0.1.5) — recommended: stable, pinned to a tag
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.5"

# latest main — bleeding edge, whatever's currently merged, not pinned to a release
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@main"
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
- `draft2version(draft_path, version_path)` / `publishversion(consumer_view_path, version_path)`
  — **not implemented yet** (both raise `NotImplementedError`); promoting a draft table into a
  permanent version and repointing a consumer-facing view at it.
- `deleteview(view_path)` — `DROP VIEW IF EXISTS`.

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

**Discovery**
- `gettablesfrom(schema_path)` — every table/view under `schema_path`, recursing into
  subfolders; returns full dot-separated paths.
- `gettableitemsfrom(table_path)` — one table/view's schema (`{column: DATA_TYPE}`) and row
  count, without fetching any actual rows.

**Wiki & tags** (Dremio's catalog collaboration API — REST-only, no SQL/Flight equivalent):
- `getwikifrom(path)` / `assignwikito(path, text)` — read, or create/overwrite, the wiki text on
  a catalog entity.
- `gettagsfrom(path)` / `assigntagsto(path, tags)` / `deletetags(path, tags)` — read the full tag
  list; replace it wholesale; or remove just the given tags, leaving the rest untouched.

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

## Layout

| Path | Purpose |
|---|---|
| `dds_ingestion/credentials.py` | env-var creds + redacted `DremioCreds` |
| `dds_ingestion/models.py` | typed request/response models for the DDS ingest contract |
| `dds_ingestion/client.py` | thin, unit-testable HTTP client (`IngestClient`) |
| `dds_ingestion/progress.py` | tqdm progress bar with graceful fallback |
| `dds_ingestion/folder.py` | `FolderIngest` orchestration (scan/parallel/resume) |
| `catalog/client.py` | `Catalog` — two connections (REST + Flight), every operation as a method |
| `catalog/operations.py` | the operations themselves (table2view, datacopy, createfolder, ...), as functions taking an executor and/or a `CatalogRestClient` |
| `catalog/sql.py` | `SqlExecutor` protocol + REST/Flight implementations, `resolve_executor()` (transport env var) |
| `catalog/rest.py` | `CatalogRestClient` — Dremio's native `/api/v3/catalog` for folders, wiki, and tags (none of which have a SQL/Flight equivalent) |
| `catalog/retry_state.py` | persisted memory of stalled attempts, for `retry_pending()` |
| `catalog/errors.py` | `CatalogOperationError`, `EngineStartingError` |

## Development

```bash
pip install -e ".[dev]"
pytest
```

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

## Install in JupyterLab

Run this in a notebook cell — see the badge under [Install](#install) for the current latest
release tag (`v0.1.5` as of this writing):

```python
# latest release (currently v0.1.5) — recommended
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.5"

# latest main — bleeding edge, not pinned to a release
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@main"
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
%pip install /path/to/EEADataLakehouse-0.1.5-py3-none-any.whl
```
