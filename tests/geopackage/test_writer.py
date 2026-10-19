from __future__ import annotations

import datetime as dt
import decimal
import struct
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
import shapely

from eea_datalakehouse.geopackage.writer import (
    GeoPackageWriter,
    batch_rows,
    encode_geometries,
    gpkg_type,
    gpkg_value,
)

from .conftest import geometry, read_gpkg


def test_an_empty_package_has_the_geopackage_header_and_tables(out: Path) -> None:
    with GeoPackageWriter(out):
        pass

    assert read_gpkg(out, "PRAGMA application_id") == [(0x47504B47,)]
    assert read_gpkg(out, "PRAGMA user_version") == [(10400,)]
    assert read_gpkg(out, "SELECT srs_id FROM gpkg_spatial_ref_sys ORDER BY srs_id") == [
        (-1,),
        (0,),
        (4326,),
    ]
    assert read_gpkg(out, "PRAGMA integrity_check") == [("ok",)]
    assert read_gpkg(out, "PRAGMA journal_mode") == [("delete",)]  # a normal file afterwards


def test_a_feature_table_gets_its_metadata_rows(out: Path) -> None:
    with GeoPackageWriter(out) as gpkg:
        gpkg.create_table("pts", [("name", "TEXT")], geometry_column="geom", srs_id=3035)
        gpkg.insert(
            "pts",
            [("a",), ("b",), ("c",)],
            np.array([shapely.Point(1, 2), shapely.Point(5, 7), None], dtype=object),
        )

    assert read_gpkg(out, "SELECT * FROM gpkg_geometry_columns") == [
        ("pts", "geom", "POINT", 3035, 0, 0)
    ]
    assert read_gpkg(
        out, "SELECT data_type, srs_id, min_x, min_y, max_x, max_y FROM gpkg_contents"
    ) == [("features", 3035, 1.0, 2.0, 5.0, 7.0)]
    rows = read_gpkg(out, "SELECT fid, geom, name FROM pts ORDER BY fid")
    assert [(fid, name) for fid, _, name in rows] == [(1, "a"), (2, "b"), (3, "c")]
    assert geometry(rows[0][1]).equals(shapely.Point(1, 2))
    assert rows[2][1] is None
    assert read_gpkg(out, "PRAGMA foreign_key_check") == []


def test_insert_encoded_takes_what_workers_prepared(out: Path) -> None:
    batch = pa.record_batch({"name": ["a", "b"]})
    encoded = encode_geometries(np.array([shapely.Point(1, 2), shapely.Point(3, 4)]), 4326)
    with GeoPackageWriter(out) as gpkg:
        gpkg.create_table("pts", [("name", "TEXT")], geometry_column="geom", srs_id=4326)
        gpkg.insert_encoded("pts", batch_rows(batch), encoded)
        with pytest.raises(ValueError, match="geometries are required"):
            gpkg.insert_encoded("pts", batch_rows(batch))

    assert read_gpkg(out, "SELECT name FROM pts") == [("a",), ("b",)]
    assert read_gpkg(out, "SELECT min_x, max_y FROM gpkg_contents") == [(1.0, 4.0)]


def test_mixed_types_and_z_are_recorded(out: Path) -> None:
    with GeoPackageWriter(out) as gpkg:
        gpkg.create_table("g", [], geometry_column="geom", srs_id=4326)
        gpkg.insert("g", [(), ()], np.array([shapely.Point(1, 2, 3), shapely.box(0, 0, 1, 1)]))

    assert read_gpkg(out, "SELECT geometry_type_name, z FROM gpkg_geometry_columns") == [
        ("GEOMETRY", 2)
    ]


def test_geometry_blob_header() -> None:
    encoded = encode_geometries(np.array([shapely.box(1, 2, 3, 4)]), 3035)
    (blob,) = encoded.blobs

    assert blob is not None
    assert blob[:2] == b"GP" and blob[2] == 0
    assert blob[3] == 0b00000011  # little-endian, XY envelope
    assert struct.unpack("<i", blob[4:8]) == (3035,)
    assert struct.unpack("<4d", blob[8:40]) == (1.0, 3.0, 2.0, 4.0)  # minx maxx miny maxy
    assert shapely.from_wkb(blob[40:]).equals(shapely.box(1, 2, 3, 4))
    assert encoded.bounds == (1.0, 2.0, 3.0, 4.0)
    assert encoded.types == {"POLYGON"} and encoded.z_values == {False}


def test_an_empty_geometry_sets_the_empty_flag_and_no_envelope() -> None:
    (blob,) = encode_geometries(np.array([shapely.from_wkt("POINT EMPTY")]), 4326).blobs

    assert blob is not None and blob[3] == 0b00010001
    assert shapely.from_wkb(blob[8:]).is_empty


def test_z_geometries_are_written_as_iso_wkb_and_2d_stay_2d() -> None:
    blob3d, blob2d = encode_geometries(
        np.array([shapely.Point(1, 2, 3), shapely.Point(1, 2)]), 4326
    ).blobs

    assert blob3d is not None and blob2d is not None
    assert struct.unpack("<I", blob3d[41:45]) == (1001,)  # ISO Point Z
    assert struct.unpack("<I", blob2d[41:45]) == (1,)


def test_the_file_appears_only_when_the_block_succeeds(out: Path) -> None:
    out.write_bytes(b"previous")
    with pytest.raises(RuntimeError), GeoPackageWriter(out) as gpkg:
        gpkg.create_table("t", [("a", "TEXT")])
        raise RuntimeError("boom")

    assert out.read_bytes() == b"previous"
    assert list(out.parent.iterdir()) == [out]


@pytest.mark.parametrize(
    ("arrow_type", "expected"),
    [
        (pa.bool_(), "BOOLEAN"),
        (pa.int8(), "TINYINT"),
        (pa.int16(), "SMALLINT"),
        (pa.int32(), "MEDIUMINT"),
        (pa.int64(), "INTEGER"),
        (pa.float32(), "FLOAT"),
        (pa.float64(), "DOUBLE"),
        (pa.decimal128(10, 2), "DOUBLE"),
        (pa.string(), "TEXT"),
        (pa.binary(), "BLOB"),
        (pa.date32(), "DATE"),
        (pa.timestamp("ms"), "DATETIME"),
        (pa.list_(pa.int32()), "TEXT"),
    ],
)
def test_gpkg_type(arrow_type: pa.DataType, expected: str) -> None:
    assert gpkg_type(arrow_type) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        (3, 3),
        (decimal.Decimal("1.5"), 1.5),
        (dt.date(2026, 1, 2), "2026-01-02"),
        (dt.datetime(2026, 1, 2, 3, 4, 5), "2026-01-02T03:04:05.000Z"),
        (
            dt.datetime(2026, 1, 2, 5, 4, 5, tzinfo=dt.timezone(dt.timedelta(hours=2))),
            "2026-01-02T03:04:05.000Z",
        ),
        ([1, 2], "[1, 2]"),
        ({"a": 1}, '{"a": 1}'),
    ],
)
def test_gpkg_value(value: object, expected: object) -> None:
    assert gpkg_value(value) == expected


def test_batch_rows_converts_a_column_at_a_time() -> None:
    batch = pa.record_batch(
        {
            "n": pa.array([1, None]),
            "d": pa.array([dt.date(2026, 1, 2), None]),
            "x": pa.array([decimal.Decimal("1.25"), decimal.Decimal("2")], pa.decimal128(5, 2)),
            "t": pa.array([dt.datetime(2026, 1, 2, 3, 4, 5), None], pa.timestamp("us")),
            "l": pa.array([[1], None]),
        }
    )

    assert batch_rows(batch) == [
        (1, "2026-01-02", 1.25, "2026-01-02T03:04:05.000Z", "[1]"),
        (None, None, 2.0, None, None),
    ]
    assert batch_rows(pa.record_batch({"a": [1, 2]}).select([])) == [(), ()]
