"""eea_datalakehouse.dds_documents — store plain files (documents) in DDS folders.

Unlike :mod:`eea_datalakehouse.dds_ingestion`, nothing here creates a dataset:
a document is uploaded as-is and never becomes a Dremio table::

    from eea_datalakehouse.dds_documents import DocumentsClient

    with DocumentsClient.from_env() as docs:
        docs.put("a/b/metadata/x.xml", xml_bytes, content_type="application/xml")
"""

from __future__ import annotations

from .client import DocumentExistsError, DocumentsApiError, DocumentsClient

__all__ = ["DocumentExistsError", "DocumentsApiError", "DocumentsClient"]
