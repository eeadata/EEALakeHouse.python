# EEADataLakehouse

Two main areas of concern and class domains for the EEA data lakehouse:

- `eea_datalakehouse.data_preparation` — data acquisition, vocabulary
  acquisition, data exploration, and transformation to parquet. Each stage is
  an abstract base class; concrete per-dataset pipelines subclass them since
  the actual logic varies by dataset and data flow.
- `eea_datalakehouse.dds_ingestion` — notebook-side client for the Dremio Document
  Service (DDS) Ingest API (DI-8.4/8.5). `FolderIngest` transfers a folder of
  data files to S3 (via DDS-issued presigned URLs only — never an S3 SDK or
  S3 credentials) and registers it as a Dremio table, coordinated entirely
  through the DDS REST API.

## Install

Install directly from GitHub, pinned to a released tag:

```bash
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.0"
```

## Usage

```python
from pathlib import Path

from eea_datalakehouse.data_preparation import DataValidationError, ParquetTransformer


class MyDatasetTransformer(ParquetTransformer):
    def to_parquet(self, records, destination: Path) -> Path:
        ...  # dataset-specific parquet write


transformer = MyDatasetTransformer(required_fields=["id", "value"])

raw_source = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
records = transformer.prepare(raw_source)

try:
    transformer.validate(records)
except DataValidationError as exc:
    print(f"Invalid data: {exc}")
```

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

- `prepare(source)` normalizes an iterable of raw records into a list of plain dicts.
- `validate(records)` checks that every record contains the configured `required_fields`,
  raising `DataValidationError` on the first invalid record.
- `to_parquet(records, destination)` is where a concrete subclass writes its own dataset's
  schema out to parquet.

## Layout

| Path | Purpose |
|---|---|
| `data_preparation/acquisition.py` | `DataAcquirer` — fetch a dataset's raw source data |
| `data_preparation/vocabulary.py` | `VocabularyLoader` — load the controlled vocabulary to validate against |
| `data_preparation/exploration.py` | `Explorer` — inspect acquired data before transformation |
| `data_preparation/transformation.py` | `ParquetTransformer` — prepare/validate/write to parquet |
| `dds_ingestion/credentials.py` | env-var creds + redacted `DremioCreds` |
| `dds_ingestion/models.py` | typed request/response models for the DDS ingest contract |
| `dds_ingestion/client.py` | thin, unit-testable HTTP client (`IngestClient`) |
| `dds_ingestion/progress.py` | tqdm progress bar with graceful fallback |
| `dds_ingestion/folder.py` | `FolderIngest` orchestration (scan/parallel/resume) |

## Development

```bash
pip install -e ".[dev]"
pytest
```

## Releasing a new version

1. Update `version` in `pyproject.toml` (e.g. `0.2.0`).
2. Commit and push to `main`.
3. Tag the commit and push the tag:
   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```
4. The `Release` GitHub Actions workflow builds the package and publishes a
   GitHub Release for the tag, with notes auto-generated from merged PRs since
   the previous release (categorized via `.github/release.yml`).

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
from eea_datalakehouse.data_preparation import ParquetTransformer
```

Alternatively, download the wheel attached to the GitHub Release page for
that tag and install the local file instead of pulling from git:

```python
%pip install /path/to/EEADataLakehouse-0.1.0-py3-none-any.whl
```
