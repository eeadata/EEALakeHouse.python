from __future__ import annotations

import datetime as dt
import threading
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest
import shapely

from eea_datalakehouse.catalog.operations import TableInfo
from eea_datalakehouse.geopackage import DremioSource, GpkgController
from eea_datalakehouse.geopackage.controller import layer_names, select_datasets

from .conftest import DATASET, FakeSource, geometry, read_gpkg

STATIONS = pa.record_batch(
    {
        "id": [1, 2, 3],
        "geom": ["POINT (10 52)", "POINT (11 51)", None],
        "crs": ["EPSG:4326", "4326", "4326"],
        "measured": [dt.datetime(2026, 1, 1, 12), None, dt.datetime(2026, 2, 1)],
    }
)
CODES = pa.record_batch({"code": ["X1", "X2"], "label": ["one", "two"]})


def _source(**kwargs: Any) -> FakeSource:
    return FakeSource(
        {
            f"{DATASET}.stations": [STATIONS],
            f"{DATASET}.codes": [CODES],
            f"{DATASET}.sub.samples": [
                pa.record_batch({"site": ["a"], "lat": [50.0], "lon": [4.0]}),
                pa.record_batch({"site": ["b"], "lat": [51.0], "lon": [5.0]}),
            ],
            f"{DATASET}.sub.empty": [],
        },
        kinds={f"{DATASET}.codes": "VIEW"},
        columns={f"{DATASET}.sub.empty": [("id", "BIGINT"), ("name", "CHARACTER VARYING")]},
        **kwargs,
    )


def test_every_table_and_view_becomes_a_layer(out: Path) -> None:
    report = GpkgController(_source()).export(DATASET, out)

    assert [(layer.layer, layer.kind, layer.rows) for layer in report.layers] == [
        ("codes", "attributes", 2),
        ("stations", "features", 3),
        ("empty", "attributes", 0),
        ("samples", "features", 2),
    ]
    assert read_gpkg(out, "SELECT table_name, data_type, srs_id FROM gpkg_contents ORDER BY 1") == [
        ("codes", "attributes", None),
        ("empty", "attributes", None),
        ("samples", "features", 4326),
        ("stations", "features", 4326),
    ]
    assert read_gpkg(out, "SELECT code, label FROM codes") == [("X1", "one"), ("X2", "two")]
    assert [c[1] for c in read_gpkg(out, "PRAGMA table_info(empty)")] == ["fid", "id", "name"]
    assert read_gpkg(out, "PRAGMA integrity_check") == [("ok",)]
    assert read_gpkg(out, "PRAGMA foreign_key_check") == []


def test_a_spatial_table_keeps_its_attributes_and_replaces_the_encoded_column(
    out: Path,
) -> None:
    GpkgController(_source()).export(DATASET, out, tables=["stations"])

    columns = [c[1] for c in read_gpkg(out, "PRAGMA table_info(stations)")]
    assert columns == ["fid", "geom", "id", "crs", "measured"]
    rows = read_gpkg(out, "SELECT id, geom, measured FROM stations ORDER BY id")
    assert geometry(rows[0][1]).equals(shapely.Point(10, 52))
    assert rows[0][2] == "2026-01-01T12:00:00.000Z"
    assert rows[2][1] is None
    assert read_gpkg(out, "SELECT * FROM gpkg_geometry_columns") == [
        ("stations", "geom", "POINT", 4326, 0, 0)
    ]


def test_srid_reprojects_every_layer(out: Path) -> None:
    report = GpkgController(_source()).export(DATASET, out, srid="EPSG:3035")

    assert {layer.srs_id for layer in report.layers if layer.kind == "features"} == {3035}
    (blob,) = read_gpkg(out, "SELECT geom FROM stations WHERE id = 1")[0]
    assert geometry(blob).equals_exact(shapely.Point(4321000, 3210000), tolerance=0.01)


def test_limit_caps_the_rows_of_every_table(out: Path) -> None:
    source = _source()

    report = GpkgController(source).export(DATASET, out, limit=1)

    assert {layer.layer: layer.rows for layer in report.layers} == {
        "codes": 1,
        "empty": 0,
        "samples": 1,
        "stations": 1,
    }
    assert all(sql.endswith(" LIMIT 1") for sql in source.sql)


def test_a_table_with_no_crs_gets_the_undefined_srs_and_a_note(out: Path) -> None:
    source = FakeSource({f"{DATASET}.plain": [pa.record_batch({"wkt": ["POINT (1 2)"]})]})

    (layer,) = GpkgController(source).export(DATASET, out, srid=3035).layers

    assert layer.srs_id == -1
    assert "SRS left undefined, not reprojected to 3035" in layer.notes[0]


def test_rows_in_another_crs_are_reprojected_to_the_layers(out: Path) -> None:
    batch = pa.record_batch(
        {"wkt": ["SRID=4326;POINT (10 52)", "SRID=3035;POINT (4321000 3210000)"]}
    )
    source = FakeSource({f"{DATASET}.mixed": [batch]})

    (layer,) = GpkgController(source).export(DATASET, out).layers

    assert layer.srs_id == 4326  # the first rows' CRS
    blobs = [row[0] for row in read_gpkg(out, "SELECT geom FROM mixed ORDER BY fid")]
    assert geometry(blobs[1]).equals_exact(shapely.Point(10, 52), tolerance=1e-6)


def test_undecodable_values_are_counted(out: Path) -> None:
    first = pa.record_batch({"wkt": ["POINT (1 2)"], "srid": [4326]})
    later = pa.record_batch({"wkt": ["POINT (broken"], "srid": [4326]})
    source = FakeSource({f"{DATASET}.t": [first, later]})

    (layer,) = GpkgController(source).export(DATASET, out).layers

    assert layer.rows == 2
    assert "1 value(s) in wkt did not decode" in layer.notes[0]


def test_a_source_fid_column_is_renamed(out: Path) -> None:
    source = FakeSource({f"{DATASET}.t": [pa.record_batch({"fid": [7], "v": ["a"]})]})

    GpkgController(source).export(DATASET, out)

    assert read_gpkg(out, "SELECT fid, fid_src, v FROM t") == [(1, 7, "a")]


def test_the_path_can_be_a_single_view(out: Path) -> None:
    report = GpkgController(_source()).export(f"{DATASET}.codes", out)

    assert [layer.layer for layer in report.layers] == ["codes"]


def test_an_existing_file_is_kept_without_overwrite(out: Path) -> None:
    out.write_bytes(b"keep me")

    with pytest.raises(FileExistsError, match="overwrite=True"):
        GpkgController(_source()).export(DATASET, out, overwrite=False)
    assert out.read_bytes() == b"keep me"


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_a_bad_limit_is_refused(out: Path, limit: object) -> None:
    with pytest.raises(ValueError, match="limit must be a positive whole number"):
        GpkgController(_source()).export(DATASET, out, limit=limit)  # type: ignore[arg-type]


@pytest.mark.parametrize("workers", [0, True, 2.0])
def test_bad_workers_are_refused(out: Path, workers: object) -> None:
    with pytest.raises(ValueError, match="workers must be a positive whole number"):
        GpkgController(_source()).export(DATASET, out, workers=workers)  # type: ignore[arg-type]


def test_a_missing_output_folder_is_refused(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no such folder"):
        GpkgController(_source()).export(DATASET, tmp_path / "nope" / "x.gpkg")


# -- the streaming pipeline ----------------------------------------------------


def _big_source(tables: int = 6, batches: int = 20, rows: int = 50) -> FakeSource:
    def batch(t: int, b: int) -> pa.RecordBatch:
        ids = list(range(b * rows, (b + 1) * rows))
        return pa.record_batch(
            {
                "id": ids,
                "wkt": [f"SRID=4326;POINT ({t} {i % 80})" for i in ids],
                "label": [f"t{t}-{i}" for i in ids],
            }
        )

    return FakeSource(
        {f"{DATASET}.t{t}": [batch(t, b) for b in range(batches)] for t in range(tables)}
    )


@pytest.mark.parametrize("workers", [1, 3, 8])
def test_every_row_arrives_whatever_the_number_of_workers(out: Path, workers: int) -> None:
    source = _big_source()

    report = GpkgController(source).export(DATASET, out, workers=workers)

    assert [layer.rows for layer in report.layers] == [1000] * 6
    for t in range(6):
        ids = [row[0] for row in read_gpkg(out, f"SELECT id FROM t{t} ORDER BY fid")]
        assert ids == list(range(1000))  # each table's rows in stream order
    assert read_gpkg(out, "PRAGMA integrity_check") == [("ok",)]
    assert sorted(source.closed) == sorted(source.tables)


def test_tables_are_read_in_parallel(out: Path) -> None:
    source = _big_source(tables=4)

    GpkgController(source).export(DATASET, out, workers=4)

    assert len(source.threads) > 1


def test_a_failing_table_stops_the_export_and_leaves_no_file(out: Path) -> None:
    source = _big_source()
    source.fail = {f"{DATASET}.t2": RuntimeError("stream broke")}
    out.write_bytes(b"previous")

    with pytest.raises(RuntimeError, match="stream broke"):
        GpkgController(source).export(DATASET, out, workers=3)

    assert out.read_bytes() == b"previous"
    assert [p.name for p in out.parent.iterdir()] == [out.name]
    assert not [t for t in threading.enumerate() if t.name.startswith("gpkg")]


# -- choosing and naming the layers --------------------------------------------

AVAILABLE = [
    (f"{DATASET}.a.stations", "TABLE"),
    (f"{DATASET}.b.stations", "VIEW"),
    (f"{DATASET}.codes", "TABLE"),
]


@pytest.mark.parametrize(
    ("tables", "expected"),
    [
        (None, AVAILABLE),
        ([], AVAILABLE),
        (["codes"], [AVAILABLE[2]]),
        (["a.stations"], [AVAILABLE[0]]),
        (["a/stations"], [AVAILABLE[0]]),
        (["stations"], AVAILABLE[:2]),
        ([f"{DATASET}.codes", "codes"], [AVAILABLE[2]]),
        ("codes", [AVAILABLE[2]]),
    ],
)
def test_select_datasets(tables: list[str] | None, expected: list[tuple[str, str]]) -> None:
    assert select_datasets(AVAILABLE, DATASET, tables) == expected


def test_select_datasets_names_what_is_missing() -> None:
    with pytest.raises(ValueError, match=r"no table or view 'nope' under .*there are: a.stations"):
        select_datasets(AVAILABLE, DATASET, ["codes", "nope"])
    with pytest.raises(ValueError, match="there are no tables or views"):
        select_datasets([], DATASET, None)


def test_layer_names_use_the_relative_path_only_when_names_clash() -> None:
    paths = [p for p, _ in AVAILABLE] + [f"{DATASET}.gpkg_things"]

    assert layer_names(DATASET, paths) == ["a_stations", "b_stations", "codes", "t_gpkg_things"]


# -- DremioSource: a Flight channel per stream ----------------------------------


class _FakeChannel:
    def __init__(self, n: int) -> None:
        self.n = n
        self.closed = False

    def stream_batches(self, sql: str, *, idempotency_key: str) -> Any:
        yield pa.record_batch({"channel": [self.n]})

    def close(self) -> None:
        self.closed = True


def _catalog() -> Any:
    opened: list[_FakeChannel] = []

    def open_flight_channel() -> _FakeChannel:
        opened.append(_FakeChannel(len(opened)))
        return opened[-1]

    return SimpleNamespace(
        opened=opened,
        open_flight_channel=open_flight_channel,
        _catalog_rest=SimpleNamespace(
            list_dataset_kinds=lambda segments: [(".".join(segments), "TABLE")]
        ),
        gettableitemsfrom=lambda path, idempotency_key: TableInfo({"id": "BIGINT"}, 0),
    )


def test_dremio_source_gives_each_open_stream_its_own_channel_and_reuses_idle_ones() -> None:
    catalog = _catalog()
    with DremioSource(catalog) as source:
        first, second = source.batches("SELECT 1"), source.batches("SELECT 2")
        assert [next(first).column(0)[0].as_py(), next(second).column(0)[0].as_py()] == [0, 1]
        first.close()
        second.close()
        third = source.batches("SELECT 3")
        assert next(third).column(0)[0].as_py() in (0, 1)  # an idle channel, not a new one
        third.close()
        assert len(catalog.opened) == 2
        assert source.datasets("catalog.water") == [("catalog.water", "TABLE")]
        assert source.columns("a.b") == [("id", "BIGINT")]

    assert all(channel.closed for channel in catalog.opened)
