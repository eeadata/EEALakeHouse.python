from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import pytest
import shapely

from eea_datalakehouse.geopackage.spatial import (
    SpatialPlan,
    epsg_of,
    geometries_for,
    plan_table,
    reproject,
    resolve_srid,
)

EWKB = shapely.to_wkb(shapely.set_srid(shapely.Point(4321000, 3210000), 3035), include_srid=True)


def _plan(columns: dict[str, list[object]]) -> tuple[SpatialPlan | None, pa.RecordBatch]:
    batch = pa.record_batch(columns)
    return plan_table(batch.schema, batch), batch


@pytest.mark.parametrize(
    ("column", "values", "encoding"),
    [
        ("geom", ["POINT (1 2)", "LINESTRING (0 0, 1 1)"], "wkt"),
        ("geom", ["SRID=3035;POINT (1 2)", None], "wkt"),
        ("geom", [shapely.to_wkb(shapely.Point(1, 2))], "wkb"),
        ("geom", [EWKB], "wkb"),
        ("geom", [shapely.to_wkb(shapely.Point(1, 2), hex=True)], "hexwkb"),
        ("geom", ['{"type": "Point", "coordinates": [1, 2]}'], "geojson"),
        ("whatever", ["POLYGON ((0 0, 1 0, 1 1, 0 0))"], "wkt"),  # any name, by content
    ],
)
def test_a_geometry_column_is_found_by_decoding_it(
    column: str, values: list[object], encoding: str
) -> None:
    plan, _ = _plan({"id": list(range(len(values))), column: values})

    assert plan is not None and plan.kind == "geometry"
    assert (plan.geometry_column, plan.encoding) == (column, encoding)


def test_a_column_named_like_a_geometry_wins_over_others() -> None:
    plan, _ = _plan({"note": ["POINT (9 9)"], "shape": ["POINT (1 2)"]})

    assert plan is not None and plan.geometry_column == "shape"


@pytest.mark.parametrize(
    "columns",
    [
        {"name": ["a", "b"], "value": [1, 2]},
        {"geom": ["POINT (1 2)", "not a geometry"]},  # one value fails: not a geometry column
        {"code": ["DEADBEEF"]},  # too short to be hex WKB
        {"lat": ["50"], "lon": ["4"]},  # latitude/longitude must be numeric
    ],
)
def test_a_tabular_table_has_no_plan(columns: dict[str, list[object]]) -> None:
    plan, _ = _plan(columns)

    assert plan is None


def test_latitude_and_longitude_make_points_in_4326() -> None:
    plan, batch = _plan({"Latitude": [50.5, None], "Longitude": [4.25, 5.0]})

    assert plan is not None and plan.kind == "latlon"
    geometries, srids = geometries_for(plan, batch)
    assert geometries[0].equals(shapely.Point(4.25, 50.5)) and geometries[1] is None
    assert srids.tolist() == [4326, 0]


def test_the_srid_inside_the_geometry_wins_over_the_crs_column() -> None:
    plan, batch = _plan(
        {
            "wkt": ["SRID=3035;POINT (1 2)", "POINT (3 4)", "POINT (5 6)"],
            "crs": ["4326", "EPSG:3857", None],
        }
    )

    assert plan is not None and plan.crs_column == "crs"
    _, srids = geometries_for(plan, batch)
    assert srids.tolist() == [3035, 3857, 0]


def test_ewkb_keeps_its_srid() -> None:
    plan, batch = _plan({"geom": [EWKB]})

    assert plan is not None
    _, srids = geometries_for(plan, batch)
    assert srids.tolist() == [3035]


def test_a_crs_column_overrides_the_latlon_default() -> None:
    plan, batch = _plan({"lat": [50.0], "lon": [4.0], "srid": [4258]})

    assert plan is not None
    _, srids = geometries_for(plan, batch)
    assert srids.tolist() == [4258]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (3035, 3035),
        ("3035", 3035),
        ("EPSG:3035", 3035),
        ("urn:ogc:def:crs:EPSG::3035", 3035),
        ("ETRS89-extended / LAEA Europe", 3035),
        (None, 0),
        ("", 0),
        ("nonsense", 0),
        (-1, 0),
        (True, 0),
    ],
)
def test_epsg_of(value: object, expected: int) -> None:
    assert epsg_of(value) == expected


def test_resolve_srid_refuses_what_is_not_an_epsg_crs() -> None:
    assert resolve_srid("EPSG:4326") == 4326
    with pytest.raises(ValueError, match="not an EPSG coordinate reference system"):
        resolve_srid("nonsense")
    with pytest.raises(ValueError, match="not an EPSG"):
        resolve_srid(999999)


def test_reproject_transforms_each_row_from_its_own_crs() -> None:
    geometries = np.array(
        [shapely.Point(10, 52), shapely.Point(4321000, 3210000), shapely.Point(1, 1), None],
        dtype=object,
    )
    srids = np.array([4326, 3035, 0, 4326])

    out = reproject(geometries, srids, 3035)

    assert out[0].equals_exact(shapely.Point(4321000, 3210000), tolerance=0.01)  # LAEA origin
    assert out[1].equals(geometries[1])  # already in 3035
    assert out[2].equals(geometries[2])  # unknown CRS: untouched
    assert out[3] is None


def test_reproject_keeps_two_dimensional_geometries_two_dimensional() -> None:
    out = reproject(np.array([shapely.Point(10, 52)]), np.array([4326]), 3035)

    assert not shapely.has_z(out[0])
    out3d = reproject(np.array([shapely.Point(10, 52, 7)]), np.array([4326]), 3035)
    assert shapely.has_z(out3d[0]) and out3d[0].z == pytest.approx(7)


@pytest.mark.parametrize("target", [3857, 3035, 25832, 4258])
def test_reproject_mixes_2d_and_3d_without_losing_coordinates(target: int) -> None:
    geometries = np.array(
        [shapely.Point(10, 52), shapely.Point(10, 52, 7), shapely.box(9, 51, 10, 52)]
    )

    out = reproject(geometries, np.array([4326, 4326, 4326]), target)

    assert not shapely.has_z(out[0]) and shapely.has_z(out[1]) and not shapely.has_z(out[2])
    assert np.isfinite(shapely.get_coordinates(out, include_z=False)).all()
    assert out[1].z == pytest.approx(7, abs=1)


def test_reproject_is_safe_from_several_threads() -> None:
    points = np.array([shapely.Point(10, 52)] * 200)
    srids = np.full(200, 4326)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: reproject(points, srids, 3035), range(16)))

    for out in results:
        assert all(p.equals_exact(shapely.Point(4321000, 3210000), tolerance=0.01) for p in out)
