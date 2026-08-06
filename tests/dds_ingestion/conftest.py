from __future__ import annotations

from pathlib import Path

import pytest
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds

BASE_URL = "https://dds.example.test"


@pytest.fixture
def creds() -> DremioCreds:
    return DremioCreds(username="alice", _password="s3cr3t-pwd")


@pytest.fixture
def data_folder(tmp_path: Path) -> Path:
    """A folder with two parquet files plus noise of other formats."""

    (tmp_path / "a.parquet").write_bytes(b"PAR1-a")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.parquet").write_bytes(b"PAR1-bb")
    # noise that the single-format scan must ignore:
    (tmp_path / "notes.csv").write_text("x,y\n1,2\n")
    (tmp_path / "meta.json").write_text("{}")
    (tmp_path / "readme.txt").write_text("ignore me")
    return tmp_path
