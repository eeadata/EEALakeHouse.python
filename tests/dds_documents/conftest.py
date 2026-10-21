from __future__ import annotations

import httpx
import pytest

from eea_datalakehouse.dds_documents import DocumentsClient
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds

DDS_URL = "https://dds.example.test"
DATASET = "catalog.water.bwd"
TARGET = "catalog/water/bwd/deliverables"


def file_url(path: str, base: str = DDS_URL) -> str:
    return f"{base}/api/v1/files/{path}"


def list_url(folder: str, base: str = DDS_URL) -> str:
    """The folder's listing: its path with a trailing slash."""
    return f"{base}/api/v1/files/{folder}/"


def not_found() -> httpx.Response:
    return httpx.Response(404, json={"error": "not_found", "message": "no such file"})


@pytest.fixture
def documents() -> DocumentsClient:
    return DocumentsClient(DDS_URL, DremioCreds(username="alice", _password="s3cr3t-pwd"))
