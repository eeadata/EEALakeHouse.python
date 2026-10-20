"""Write an OGC GeoPackage (1.4) with nothing but :mod:`sqlite3`.

A GeoPackage is a SQLite file with three metadata tables
(``gpkg_spatial_ref_sys``, ``gpkg_contents``, ``gpkg_geometry_columns``) and
one table per layer. Feature tables hold geometries as GeoPackage binary: a
short header (``GP``, flags, SRS id, envelope) followed by ISO WKB, which
:func:`shapely.to_wkb` writes. No GDAL is involved.

Built for bulk loading: :func:`encode_geometries` and :func:`batch_rows` turn
an Arrow batch into ready-to-insert values and are safe to call from worker
threads, so the one thread that owns the SQLite connection only inserts.
The file is built under a temporary name with no journal and no fsync, in a
single transaction, and moved into place on :meth:`~GeoPackageWriter.close`
— a failed export never leaves a half-written ``.gpkg`` where a good one was.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import os
import sqlite3
import struct
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import shapely

from . import crs

APPLICATION_ID = 0x47504B47  # "GPKG"
USER_VERSION = 10400  # GeoPackage 1.4.0
UNDEFINED_CARTESIAN = -1
UNDEFINED_GEOGRAPHIC = 0
FID_COLUMN = "fid"

# A bulk build into a temporary file that is renamed only on success: no
# rollback journal or fsync is needed, and one transaction holds it all.
_BULK_PRAGMAS = (
    "PRAGMA journal_mode = OFF",
    "PRAGMA synchronous = OFF",
    "PRAGMA temp_store = MEMORY",
    "PRAGMA cache_size = -131072",  # 128 MiB
    "PRAGMA locking_mode = EXCLUSIVE",
)

_METADATA_DDL = (
    """CREATE TABLE gpkg_spatial_ref_sys (
  srs_name TEXT NOT NULL,
  srs_id INTEGER NOT NULL PRIMARY KEY,
  organization TEXT NOT NULL,
  organization_coordsys_id INTEGER NOT NULL,
  definition TEXT NOT NULL,
  description TEXT
)""",
    """CREATE TABLE gpkg_contents (
  table_name TEXT NOT NULL PRIMARY KEY,
  data_type TEXT NOT NULL,
  identifier TEXT UNIQUE,
  description TEXT DEFAULT '',
  last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  min_x DOUBLE,
  min_y DOUBLE,
  max_x DOUBLE,
  max_y DOUBLE,
  srs_id INTEGER,
  CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
)""",
    """CREATE TABLE gpkg_geometry_columns (
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL,
  geometry_type_name TEXT NOT NULL,
  srs_id INTEGER NOT NULL,
  z TINYINT NOT NULL,
  m TINYINT NOT NULL,
  CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
  CONSTRAINT uk_gc_table_name UNIQUE (table_name),
  CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
  CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys (srs_id)
)""",
)

# shapely.get_type_id -> GeoPackage geometry type name.
_GEOMETRY_TYPES = {
    0: "POINT",
    1: "LINESTRING",
    2: "LINESTRING",  # a LinearRing is written as a LineString
    3: "POLYGON",
    4: "MULTIPOINT",
    5: "MULTILINESTRING",
    6: "MULTIPOLYGON",
    7: "GEOMETRYCOLLECTION",
}


def quote_identifier(name: str) -> str:
    """``name`` as a SQLite identifier, double-quoted."""
    return '"' + name.replace('"', '""') + '"'


def gpkg_type(arrow_type: pa.DataType) -> str:
    """The GeoPackage column type for an Arrow type."""
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    if pa.types.is_int8(arrow_type):
        return "TINYINT"
    if pa.types.is_int16(arrow_type) or pa.types.is_uint8(arrow_type):
        return "SMALLINT"
    if pa.types.is_int32(arrow_type) or pa.types.is_uint16(arrow_type):
        return "MEDIUMINT"
    if pa.types.is_integer(arrow_type):
        return "INTEGER"
    if pa.types.is_float32(arrow_type) or pa.types.is_float16(arrow_type):
        return "FLOAT"
    if pa.types.is_floating(arrow_type) or pa.types.is_decimal(arrow_type):
        return "DOUBLE"
    if (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    ):
        return "BLOB"
    if pa.types.is_date(arrow_type):
        return "DATE"
    if pa.types.is_timestamp(arrow_type):
        return "DATETIME"
    return "TEXT"


def gpkg_value(value: Any) -> Any:
    """``value`` (as Arrow's ``to_pylist`` gives it) in the form SQLite and the
    GeoPackage column types expect: dates as ISO text, timestamps as UTC
    ``...Z`` text, decimals as floats, nested values as JSON text."""
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.UTC).replace(tzinfo=None)
        return value.isoformat(timespec="milliseconds") + "Z"
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (dt.time, dt.timedelta)):
        return str(value)
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    return json.dumps(value, default=str)


def _is_plain(arrow_type: pa.DataType) -> bool:
    """Types whose Python values SQLite takes as they are."""
    return (
        pa.types.is_boolean(arrow_type)
        or pa.types.is_integer(arrow_type)
        or pa.types.is_floating(arrow_type)
        or pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_null(arrow_type)
    )


def column_values(array: pa.Array | pa.ChunkedArray) -> list[Any]:
    """One column's values ready for SQLite, converted a column at a time
    (Arrow does the casting) rather than value by value where it can."""
    arrow_type = array.type
    if _is_plain(arrow_type):
        return array.to_pylist()
    if pa.types.is_decimal(arrow_type):
        return pc.cast(array, pa.float64()).to_pylist()
    if pa.types.is_date(arrow_type):
        return pc.cast(array, pa.string()).to_pylist()  # YYYY-MM-DD
    return [gpkg_value(v) for v in array.to_pylist()]


def batch_rows(batch: pa.RecordBatch) -> list[tuple[Any, ...]]:
    """The batch's rows as tuples ready to insert, in its column order."""
    if not batch.num_columns:
        return [()] * batch.num_rows  # a features table whose only column is the geometry
    return list(zip(*(column_values(column) for column in batch.columns), strict=True))


@dataclass(frozen=True)
class EncodedGeometries:
    """One batch's geometries as GeoPackage binary, plus what the layer's
    metadata needs from them."""

    blobs: list[bytes | None]
    bounds: tuple[float, float, float, float] | None  # min_x, min_y, max_x, max_y
    types: frozenset[str]
    z_values: frozenset[bool]


def encode_geometries(geometries: np.ndarray, srs_id: int) -> EncodedGeometries:
    """GeoPackage binary for each geometry (``None`` stays ``None``).

    Header: ``GP``, version 0, flags (little-endian; an XY envelope, or the
    empty bit and no envelope for an empty geometry), the SRS id, then ISO
    WKB. Vectorised shapely calls release the GIL, so this runs well in
    worker threads.
    """
    wkb = shapely.to_wkb(geometries, byte_order=1, flavor="iso", output_dimension=3)
    all_bounds = shapely.bounds(geometries)
    empty = shapely.is_empty(geometries)
    srs = struct.pack("<i", srs_id)
    with_envelope = b"GP\x00\x03" + srs
    empty_header = b"GP\x00\x11" + srs
    # Every envelope packed at once (min_x, max_x, min_y, max_y per row), then
    # sliced: no per-row struct.pack.
    envelopes = np.ascontiguousarray(all_bounds[:, [0, 2, 1, 3]], dtype="<f8").tobytes()
    blobs: list[bytes | None] = [
        None
        if body is None
        else empty_header + body
        if is_empty
        else with_envelope + envelopes[i * 32 : i * 32 + 32] + body
        for i, (body, is_empty) in enumerate(zip(wkb, empty, strict=True))
    ]

    present = geometries[~shapely.is_missing(geometries)]
    if not len(present):
        return EncodedGeometries(blobs, None, frozenset(), frozenset())
    types = frozenset(
        _GEOMETRY_TYPES.get(int(t), "GEOMETRY") for t in np.unique(shapely.get_type_id(present))
    )
    z_values = frozenset(bool(z) for z in np.unique(shapely.has_z(present)))
    non_empty = present[~shapely.is_empty(present)]
    bounds = None
    if len(non_empty):
        min_x, min_y, max_x, max_y = (float(v) for v in shapely.total_bounds(non_empty))
        bounds = (min_x, min_y, max_x, max_y)
    return EncodedGeometries(blobs, bounds, types, z_values)


def geometry_type_name(types: Iterable[str]) -> str:
    """The one type a layer's geometries have, else ``GEOMETRY``."""
    kinds = set(types)
    return next(iter(kinds)) if len(kinds) == 1 else "GEOMETRY"


@dataclass
class _Layer:
    """What a feature table has seen so far, for its metadata rows."""

    name: str
    srs_id: int
    geometry_column: str
    bounds: list[float] = field(default_factory=lambda: [np.inf, np.inf, -np.inf, -np.inf])
    types: set[str] = field(default_factory=set)
    z_values: set[bool] = field(default_factory=set)

    def add(self, encoded: EncodedGeometries) -> None:
        self.types |= encoded.types
        self.z_values |= encoded.z_values
        if encoded.bounds is not None:
            b, (min_x, min_y, max_x, max_y) = self.bounds, encoded.bounds
            self.bounds = [min(b[0], min_x), min(b[1], min_y), max(b[2], max_x), max(b[3], max_y)]


class GeoPackageWriter:
    """Builds one ``.gpkg`` file; use as a context manager::

        with GeoPackageWriter("out.gpkg") as gpkg:
            gpkg.create_table("stations", columns, geometry_column="geom", srs_id=3035)
            gpkg.insert("stations", rows, geometries)

    The file appears at ``path`` only when the block exits without an error.
    One thread uses a writer; workers prepare values with
    :func:`batch_rows` / :func:`encode_geometries` and hand them to
    :meth:`insert_encoded`.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._tmp = self.path.with_name(f".{self.path.name}.partial")
        self._tmp.unlink(missing_ok=True)
        self._db = sqlite3.connect(self._tmp, isolation_level=None)
        for pragma in _BULK_PRAGMAS:
            self._db.execute(pragma)
        self._db.execute(f"PRAGMA application_id = {APPLICATION_ID}")
        self._db.execute(f"PRAGMA user_version = {USER_VERSION}")
        self._db.execute("BEGIN")
        for ddl in _METADATA_DDL:
            self._db.execute(ddl)
        self._srs: set[int] = set()
        self._layers: dict[str, _Layer] = {}
        self._insert_sql: dict[str, str] = {}
        self.add_srs(UNDEFINED_CARTESIAN)
        self.add_srs(UNDEFINED_GEOGRAPHIC)
        self.add_srs(4326)

    # -- lifecycle --------------------------------------------------------

    def __enter__(self) -> GeoPackageWriter:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if exc_type is None:
            self.close()
        else:
            self.abort()

    def close(self) -> None:
        """Write the layers' metadata, commit, and move the file into place."""
        for layer in self._layers.values():
            self._finish_layer(layer)
        self._db.execute("COMMIT")
        self._db.close()
        os.replace(self._tmp, self.path)

    def abort(self) -> None:
        """Drop the partial file; ``path`` is left as it was."""
        self._db.close()
        self._tmp.unlink(missing_ok=True)

    # -- building ---------------------------------------------------------

    def add_srs(self, srs_id: int) -> None:
        """Register ``srs_id`` (an EPSG code, or -1 / 0 for undefined) once."""
        if srs_id in self._srs:
            return
        if srs_id == UNDEFINED_CARTESIAN:
            row = ("Undefined cartesian SRS", srs_id, "NONE", srs_id, "undefined", None)
        elif srs_id == UNDEFINED_GEOGRAPHIC:
            row = ("Undefined geographic SRS", srs_id, "NONE", srs_id, "undefined", None)
        else:
            name, definition = crs.srs_row(srs_id)  # from epsg_definitions.json first
            row = (name, srs_id, "EPSG", srs_id, definition, None)
        self._db.execute("INSERT INTO gpkg_spatial_ref_sys VALUES (?, ?, ?, ?, ?, ?)", row)
        self._srs.add(srs_id)

    def create_table(
        self,
        name: str,
        columns: Sequence[tuple[str, str]],
        *,
        geometry_column: str | None = None,
        srs_id: int = UNDEFINED_CARTESIAN,
        description: str = "",
    ) -> None:
        """Create a layer: ``columns`` are ``(name, GeoPackage type)`` pairs,
        written after an ``fid`` primary key. With ``geometry_column`` it is a
        feature table in ``srs_id``; without, an attributes table."""
        defs = [f"{quote_identifier(FID_COLUMN)} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL"]
        if geometry_column is not None:
            defs.append(f"{quote_identifier(geometry_column)} GEOMETRY")
        defs += [f"{quote_identifier(col)} {kind}" for col, kind in columns]
        self._db.execute(f"CREATE TABLE {quote_identifier(name)} ({', '.join(defs)})")
        if geometry_column is not None:
            self.add_srs(srs_id)
            self._db.execute(
                "INSERT INTO gpkg_contents (table_name, data_type, identifier, description, "
                "srs_id) VALUES (?, 'features', ?, ?, ?)",
                (name, name, description, srs_id),
            )
            self._layers[name] = _Layer(name, srs_id, geometry_column)
            cols = [geometry_column, *(col for col, _ in columns)]
        else:
            self._db.execute(
                "INSERT INTO gpkg_contents (table_name, data_type, identifier, description) "
                "VALUES (?, 'attributes', ?, ?)",
                (name, name, description),
            )
            cols = [col for col, _ in columns]
        self._insert_sql[name] = (
            f"INSERT INTO {quote_identifier(name)} "
            f"({', '.join(quote_identifier(c) for c in cols)}) "
            f"VALUES ({', '.join('?' for _ in cols)})"
        )

    def insert(
        self,
        name: str,
        rows: Iterable[Sequence[Any]],
        geometries: np.ndarray | None = None,
    ) -> None:
        """Append ``rows`` (values in ``create_table``'s column order) to a
        layer; for a feature table, ``geometries`` holds one shapely geometry
        (or ``None``) per row."""
        values = [[gpkg_value(v) for v in row] for row in rows]
        layer = self._layers.get(name)
        encoded = None
        if layer is not None:
            if geometries is None:
                raise ValueError(f"{name} is a feature table: geometries are required")
            encoded = encode_geometries(geometries, layer.srs_id)
        self.insert_encoded(name, values, encoded)

    def insert_encoded(
        self,
        name: str,
        rows: Sequence[Sequence[Any]],
        encoded: EncodedGeometries | None = None,
    ) -> None:
        """Append rows already prepared by :func:`batch_rows` and, for a
        feature table, their geometries by :func:`encode_geometries`."""
        layer = self._layers.get(name)
        if layer is not None:
            if encoded is None:
                raise ValueError(f"{name} is a feature table: geometries are required")
            layer.add(encoded)
            rows = [(blob, *row) for blob, row in zip(encoded.blobs, rows, strict=True)]
        if rows:
            self._db.executemany(self._insert_sql[name], rows)

    def geometry_type(self, name: str) -> str | None:
        """The geometry type a feature table's ``gpkg_geometry_columns`` row
        will name: the one type its rows have, else ``GEOMETRY``."""
        layer = self._layers.get(name)
        return None if layer is None else geometry_type_name(layer.types)

    # -- internals --------------------------------------------------------

    def _finish_layer(self, layer: _Layer) -> None:
        z = 2 if len(layer.z_values) > 1 else int(True in layer.z_values)
        self._db.execute(
            "INSERT INTO gpkg_geometry_columns VALUES (?, ?, ?, ?, ?, 0)",
            (layer.name, layer.geometry_column, geometry_type_name(layer.types), layer.srs_id, z),
        )
        if np.isfinite(layer.bounds).all():
            min_x, min_y, max_x, max_y = layer.bounds
            self._db.execute(
                "UPDATE gpkg_contents SET min_x = ?, min_y = ?, max_x = ?, max_y = ? "
                "WHERE table_name = ?",
                (min_x, min_y, max_x, max_y, layer.name),
            )
