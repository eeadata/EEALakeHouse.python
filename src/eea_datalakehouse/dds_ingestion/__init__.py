"""eea_datalakehouse.dds_ingestion — notebook-side client for the Dremio
Document Service Ingest API.

A generated Jupyter notebook imports :class:`FolderIngest` to transfer a folder
of data files to S3 (via DDS-issued presigned URLs only) and register it as a
Dremio table, coordinated entirely through the DDS REST API.

Typical use inside a notebook (creds + base URL come from the kernel env)::

    from eea_datalakehouse.dds_ingestion import FolderIngest

    outcome = FolderIngest(
        folder="./my_data",
        target_catalog_path="biodiversity.uploads",
        data_format="parquet",
        intent="read_only",   # keep the files as the table | "editable" → Iceberg
        parallelism=4,
    ).run()
    print(outcome.commit.table_path, outcome.commit.record_count)
    print(outcome.commit.storage_path)   # where the files are, if kept

``intent`` decides what the transfer leaves behind. On a server configured for
permanent read-only storage, ``"read_only"`` keeps the uploaded files where the
table lives and registers them; ``"editable"`` loads them into an Iceberg table
and deletes the upload. See :class:`FolderIngest` for the full rule.
"""

from __future__ import annotations

from .client import IngestApiError, IngestClient
from .credentials import (
    DremioCreds,
    MissingCredentialsError,
    load_base_url,
    load_creds,
)
from .folder import (
    DEFAULT_PARALLELISM,
    FolderIngest,
    IngestOutcome,
    IngestStateError,
    ingest_folder,
    scan_folder,
)
from .models import (
    BeginResult,
    CommitResult,
    DataFormat,
    EstimateResult,
    FileSpec,
    Intent,
    Progress,
    S3Plan,
    StageResult,
    StatusResult,
    UploadPart,
    UploadTarget,
)

__all__ = [
    "DEFAULT_PARALLELISM",
    "BeginResult",
    "CommitResult",
    "DataFormat",
    "DremioCreds",
    "EstimateResult",
    "FileSpec",
    "FolderIngest",
    "IngestApiError",
    "IngestClient",
    "IngestOutcome",
    "IngestStateError",
    "Intent",
    "MissingCredentialsError",
    "Progress",
    "S3Plan",
    "StageResult",
    "StatusResult",
    "UploadPart",
    "UploadTarget",
    "ingest_folder",
    "load_base_url",
    "load_creds",
    "scan_folder",
]
