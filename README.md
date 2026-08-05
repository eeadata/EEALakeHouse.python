# EEADataLakehouseIngestion

Data preparation and validation utilities for the EEA data lakehouse ingestion pipeline.

## Install

Install directly from GitHub, pinned to a released tag:

```bash
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.0"
```

## Usage

```python
from eea_datalakehouse_ingestion import IngestionPipeline, IngestionValidationError

pipeline = IngestionPipeline(required_fields=["id", "value"])

raw_source = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
records = pipeline.prepare(raw_source)

try:
    pipeline.validate(records)
except IngestionValidationError as exc:
    print(f"Invalid data: {exc}")
```

- `prepare(source)` normalizes an iterable of raw records into a list of plain dicts.
- `validate(records)` checks that every record contains the configured `required_fields`,
  raising `IngestionValidationError` on the first invalid record.

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
from eea_datalakehouse_ingestion import IngestionPipeline
```

Alternatively, download the wheel attached to the GitHub Release page for
that tag and install the local file instead of pulling from git:

```python
%pip install /path/to/EEADataLakehouseIngestion-0.1.0-py3-none-any.whl
```
