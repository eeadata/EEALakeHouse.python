# EEADataLakehouse

Two main class domains for the EEA data lakehouse:

- `eea_datalakehouse.dds_ingestion` — notebook-side client for the Dremio Document
  Service (DDS) Ingest API (DI-8.4/8.5). `FolderIngest` transfers a folder of
  data files to S3 (via DDS-issued presigned URLs only — never an S3 SDK or
  S3 credentials) and registers it as a Dremio table, coordinated entirely
  through the DDS REST API.
- `eea_datalakehouse.catalog` — Dremio catalog operations once data has
  landed: promoting a draft table to a version, publishing a version to a
  consumer view, moving/copying/deleting catalog entries, and listing what's
  there. Runs over Dremio's REST SQL Jobs API by default, or Arrow Flight SQL
  opt-in — and treats a stalled call as a Dremio engine cold-starting rather
  than a failure, so it can be remembered and retried later instead of
  blocking.

## Install

Install directly from GitHub, pinned to a released tag:

```bash
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.0"
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

# base_url/token are Dremio's own (not DDS) — REST by default; set
# EEA_CATALOG_TRANSPORT=flight in the environment to use Arrow Flight SQL instead.
catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN)

try:
    catalog.draft2version(
        "bwd.draft.bw_assessment", "bwd.versions.v2025_1", idempotency_key="bwd-v2025_1"
    )
    catalog.publishversion(
        "bwd.consumer", "bwd.versions.v2025_1", idempotency_key="bwd-v2025_1-publish"
    )
except EngineStartingError:
    # A cold Dremio engine looks like a stalled call, not a failure — the
    # attempt is remembered under its idempotency_key; call it again later:
    catalog.retry_pending("bwd-v2025_1-publish")

info = catalog.gettableitemsfrom("bwd.versions.v2025_1.assessments", idempotency_key="check-1")
print(info.schema, info.row_count)             # schema + row count, no rows fetched

catalog.gettablesfrom("bwd", idempotency_key="list-1")   # recurses into every subfolder
```

- `table2view(view_path, source_path)` — the one-time `DROP TABLE` + `CREATE VIEW` transition
  for a location that started as a physical table (straight off ingest) and is moving to being
  a view; checks `source_path` exists first, and (by default) creates `view_path`'s containing
  folder if it's missing.
- `draft2version(draft_path, version_path)` — promotes a draft table into a permanent version
  (CTAS; the draft itself is untouched).
- `publishversion(consumer_view_path, version_path)` — repoints a consumer-facing view at a
  version (`CREATE OR REPLACE VIEW`, idempotent).
- `datacopy(source_path, target_path, overwrite=False)` / `datamove(...)` — copy or move a
  table/view between catalog paths, over Arrow Flight. `overwrite=False` (the default) raises if
  `target_path` already exists; `overwrite=True` replaces it.
- `deleteview(view_path)` — `DROP VIEW IF EXISTS`.
- `gettablesfrom(schema_path)` / `gettableitemsfrom(table_path)` — list every table/view under
  a path (recursively), or get one table's schema and row count without fetching any rows.
- `retry_pending(idempotency_key)` — re-attempt whatever last stalled on an `EngineStartingError`,
  using the same arguments it was originally called with.

## Layout

| Path | Purpose |
|---|---|
| `dds_ingestion/credentials.py` | env-var creds + redacted `DremioCreds` |
| `dds_ingestion/models.py` | typed request/response models for the DDS ingest contract |
| `dds_ingestion/client.py` | thin, unit-testable HTTP client (`IngestClient`) |
| `dds_ingestion/progress.py` | tqdm progress bar with graceful fallback |
| `dds_ingestion/folder.py` | `FolderIngest` orchestration (scan/parallel/resume) |
| `catalog/client.py` | `Catalog` — one connection, every operation as a method |
| `catalog/operations.py` | the operations themselves (table2view, draft2version, ...), as functions taking an executor |
| `catalog/sql.py` | `SqlExecutor` protocol + REST/Flight implementations, `resolve_executor()` (transport env var) |
| `catalog/rest.py` | `CatalogRestClient` — Dremio's native `/api/v3/catalog` for folder existence/creation (no SQL equivalent) |
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

## Install in JupyterLab

Run this in a notebook cell, pinned to the release tag you want:

```python
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.0"
```

Use the `%pip` magic rather than `!pip` — it installs into the kernel the
notebook is actually running on, instead of whatever `pip` happens to be
first on `PATH`. After installing, restart the kernel (**Kernel > Restart
Kernel...**) so the import below picks up the newly installed package:

```python
from eea_datalakehouse.dds_ingestion import FolderIngest
```

Alternatively, download the wheel attached to the GitHub Release page for
that tag and install the local file instead of pulling from git:

```python
%pip install /path/to/EEADataLakehouse-0.1.0-py3-none-any.whl
```
