from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
import shapely

DATASET = "catalog.water.bwd"


class FakeSource:
    """A `DatasetSource` serving fixed Arrow batches; records the SQL it ran
    and which streams were closed. Thread-safe, like the real one."""

    def __init__(
        self,
        tables: dict[str, list[pa.RecordBatch]],
        kinds: dict[str, str] | None = None,
        columns: dict[str, list[tuple[str, str]]] | None = None,
        fail: dict[str, Exception] | None = None,
    ) -> None:
        self.tables = tables
        self.kinds = kinds or {}
        self.column_types = columns or {}
        self.fail = fail or {}
        self.sql: list[str] = []
        self.closed: list[str] = []
        self.threads: set[str] = set()
        self._lock = threading.Lock()

    def datasets(self, path: str) -> list[tuple[str, str]]:
        return sorted(
            (p, self.kinds.get(p, "TABLE"))
            for p in self.tables
            if p == path or p.startswith(f"{path}.")
        )

    def batches(self, sql: str) -> Iterator[pa.RecordBatch]:
        with self._lock:
            self.sql.append(sql)
            self.threads.add(threading.current_thread().name)
        quoted = sql.removeprefix("SELECT * FROM ").split(" LIMIT ")[0]
        path = ".".join(part.strip('"') for part in quoted.split('"."'))
        limit = int(sql.split(" LIMIT ")[1]) if " LIMIT " in sql else None
        left = limit
        try:
            for i, batch in enumerate(self.tables[path]):
                if path in self.fail and i == 1:
                    raise self.fail[path]
                if left is not None:
                    if left <= 0:
                        return
                    batch = batch.slice(0, left)
                    left -= batch.num_rows
                yield batch
        finally:
            with self._lock:
                self.closed.append(path)

    def columns(self, path: str) -> list[tuple[str, str]]:
        return self.column_types.get(path, [])


def read_gpkg(path: Path, sql: str, *params: object) -> list[tuple[object, ...]]:
    with sqlite3.connect(path) as db:
        return db.execute(sql, params).fetchall()


def geometry(blob: bytes) -> shapely.Geometry:
    """Decode a GeoPackage geometry blob: header (8 bytes + envelope), then WKB."""
    flags = blob[3]
    envelope = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[(flags >> 1) & 0b111]
    return shapely.from_wkb(blob[8 + envelope :])


@pytest.fixture
def out(tmp_path: Path) -> Path:
    return tmp_path / "out.gpkg"
