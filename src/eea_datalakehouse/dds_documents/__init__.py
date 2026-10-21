"""eea_datalakehouse.dds_documents — store plain files (documents) in DDS folders.

Unlike :mod:`eea_datalakehouse.dds_ingestion`, nothing here creates a dataset:
a document is uploaded as-is and never becomes a Dremio table::

    from eea_datalakehouse.dds_documents import DdsController, DdsFolder

    with DdsController.from_env() as dds:
        dds.push("catalog.water.bwd", DdsFolder.DELIVERABLES, ["report.pdf"])
        dds.list("catalog.water.bwd", DdsFolder.DELIVERABLES)
        report = dds.get("catalog.water.bwd", DdsFolder.DELIVERABLES, "report.pdf")

:class:`DocumentsClient` is the HTTP layer underneath, one method per DDS
endpoint; :class:`DdsSession` is what the `%dds` magic dispatches onto.
"""

from __future__ import annotations

from .client import DocumentExistsError, DocumentsApiError, DocumentsClient
from .controller import (
    DdsController,
    DdsFile,
    DdsFileNotFoundError,
    DdsFilesExistError,
    DdsFolder,
    DdsListing,
    DdsPushError,
    PushReport,
    folder_name,
    valid_folders_hint,
)
from .paths import path_segments, to_catalog_path, to_storage_path
from .session import DdsDownload, DdsSession, DdsSessionError

__all__ = [
    "DdsController",
    "DdsDownload",
    "DdsFile",
    "DdsFileNotFoundError",
    "DdsFilesExistError",
    "DdsFolder",
    "DdsListing",
    "DdsPushError",
    "DdsSession",
    "DdsSessionError",
    "DocumentExistsError",
    "DocumentsApiError",
    "DocumentsClient",
    "PushReport",
    "folder_name",
    "path_segments",
    "to_catalog_path",
    "to_storage_path",
    "valid_folders_hint",
]
