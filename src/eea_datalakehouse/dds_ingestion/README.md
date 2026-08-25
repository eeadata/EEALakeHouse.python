# eea_datalakehouse.dds_ingestion

Notebook-side client for the **Dremio Document Service (DDS) Ingest API** (DI-8.4 / DI-8.5).

A generated Jupyter notebook imports `FolderIngest` to transfer a folder of data
files to S3 and register it as a Dremio table — coordinated entirely through the
DDS REST API. The **only** object-storage access is the presigned URLs issued by
DDS; this package never uses an S3 SDK or S3 credentials.

## Install

This is a subpackage of the **EEADataLakehouse** library, not a distribution of
its own — install the library from the repository root:

```bash
pip install -e ".[dev]"   # dev extra adds ruff / mypy / pytest / respx
```

In a notebook kernel, pinned to a ref that carries this subpackage:

```python
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@development"
```

## Usage (inside a notebook kernel)

Dremio credentials are read from the injected kernel env vars `_DREMIO_USER` /
`_DREMIO_PWD`, and the service URL from `DDS_BASE_URL`. None of these are ever
logged or printed.

```python
from eea_datalakehouse.dds_ingestion import FolderIngest

outcome = FolderIngest(
    folder="./my_data",
    target_catalog_path="biodiversity.uploads",
    data_format="parquet",      # one of parquet | csv | json
    intent="read_only",         # or "editable" — see below
    conflict_mode="fail",
    parallelism=4,              # concurrent uploads (default 4)
).run()

print(outcome.commit.table_path, outcome.commit.record_count)
print(outcome.commit.storage_path)   # where the files are, when they are kept
```

`intent` decides what the transfer leaves behind:

| | `read_only` | `editable` |
|---|---|---|
| your uploaded files | kept — they **are** the table | copied in, then deleted |
| the table | the folder, registered, with a view at the catalog path | an Iceberg table |
| good for | published data, data that accumulates | a table you will write to |

(Storing read-only files permanently is a server setting. Where it is not
enabled, both intents stage and load as before and only the table's shape
differs — `storage_path` is then `None`.)

`run()` performs `begin → upload(all files) → commit`, and leaves the session
handle available afterwards.

## Read-only data that grows

`intent="read_only"` on a server configured for permanent storage keeps your
files as uploaded and registers them as the table — nothing is copied into the
catalog. `sub_path` files each upload under a named sub-folder, so one table can
accumulate:

```python
FolderIngest(
    folder="./bw_2026",                       # a flat folder of parquet files
    target_catalog_path="water_management_resources/bathing_water/bwd/reference",
    data_format="parquet",
    intent="read_only",
    table_name="water_temperature",
    sub_path="2026",                          # → .../water_temperature/2026/
    conflict_mode="fail",                     # refuses if 2026 is already there
).run()
```

Next year, the same call with `sub_path="2027"` adds to the same table;
`conflict_mode="replace"` with a `sub_path` re-does **that year only** and leaves
the others standing. If your local folder already has the structure
(`2026/*.parquet`), you do not need `sub_path` — the layout is preserved as-is.
The name is normalised server-side to the EEA convention (lowercase,
underscores), so `"Q1 2026"` becomes `q1_2026`.

## Managing the transfer

A transfer is a server-side session, so it can be inspected and resumed:

```python
with FolderIngest(folder="./my_data", target_catalog_path="catalog/theme/sub",
                  data_format="parquet") as job:
    try:
        job.run()
    except Exception:
        if job.status().is_resumable:
            job.retry()      # re-runs only the step that failed — no re-upload
```

`job.session_id` · `status()` · `estimate()` · `retry()` · `cancel()` ·
`close()` · `FolderIngest.attach(session_id, ...)` to pick a transfer up in a
later kernel.

**Full class reference:** [`docs/python-client-guide.md`](../../docs/python-client-guide.md).

## Layout

| Path | Purpose |
|---|---|
| `dds_ingestion/credentials.py` | env-var creds + redacted `DremioCreds` |
| `dds_ingestion/models.py` | typed request/response models for the contract |
| `dds_ingestion/client.py` | thin, unit-testable HTTP client (`IngestClient`) |
| `dds_ingestion/progress.py` | tqdm progress bar with graceful fallback |
| `dds_ingestion/folder.py` | `FolderIngest` orchestration (scan/parallel/session) |
