from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
import shapely

from eea_datalakehouse.geopackage import GpkgController, crs, spatial
from eea_datalakehouse.geopackage.spatial import epsg_of, reproject, resolve_srid

from .conftest import DATASET, FakeSource, geometry, read_gpkg

# EPSG:3035 with its false easting moved by 1,000 km: only a CRS built from
# the definitions file puts the LAEA origin at x = 5321000.
SHIFTED_3035 = (
    'PROJCS["Shifted LAEA",GEOGCS["ETRS89",DATUM["European_Terrestrial_Reference_System_1989",'
    'SPHEROID["GRS 1980",6378137,298.257222101]],PRIMEM["Greenwich",0],'
    'UNIT["degree",0.0174532925199433]],PROJECTION["Lambert_Azimuthal_Equal_Area"],'
    'PARAMETER["latitude_of_center",52],PARAMETER["longitude_of_center",10],'
    'PARAMETER["false_easting",5321000],PARAMETER["false_northing",3210000],UNIT["metre",1]]'
)
WGS84 = (
    'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],AUTHORITY["EPSG","4326"]]'
)


def _reset() -> None:
    crs._loaded = None
    crs.crs_for.cache_clear()
    crs._codes_by_name.cache_clear()
    spatial._epsg_from_text.cache_clear()
    spatial._local.__dict__.clear()


@pytest.fixture
def definitions(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    path = tmp_path / "epsg_definitions.json"
    path.write_text(
        json.dumps(
            {
                "3035": {"name": "Shifted LAEA", "wkt": SHIFTED_3035},
                "4326": {"name": "WGS 84", "wkt": WGS84},
                "900913": {"name": "Google Maps Global Mercator", "wkt": WGS84},
            }
        )
    )
    monkeypatch.setattr(crs, "DEFINITIONS_FILE", path)
    _reset()
    yield path
    _reset()


def test_the_shipped_file_is_read() -> None:
    _reset()
    assert crs.DEFINITIONS_FILE.name == "epsg_definitions.json"
    assert len(crs.definitions()) > 7000
    name, wkt = crs.definition(3035) or ("", "")
    assert "LAEA" in name and wkt.startswith("PROJCS[")


@pytest.mark.usefixtures("definitions")
def test_reprojection_builds_both_crs_from_the_file() -> None:
    (out,) = reproject(np.array([shapely.Point(10, 52)]), np.array([4326]), 3035)

    assert out.equals_exact(shapely.Point(5321000, 3210000), tolerance=0.01)


@pytest.mark.usefixtures("definitions")
def test_a_code_the_file_lacks_falls_back_to_pyproj() -> None:
    (out,) = reproject(np.array([shapely.Point(10, 52)]), np.array([4326]), 3857)

    assert out.x == pytest.approx(1113194.9, abs=1)  # Web Mercator, from pyproj's database
    assert crs.definition(3857) is None


@pytest.mark.usefixtures("definitions")
def test_the_srs_rows_are_written_from_the_file(out: Path) -> None:
    source = FakeSource({f"{DATASET}.t": [pa.record_batch({"wkt": ["SRID=4326;POINT (10 52)"]})]})

    GpkgController(source).export(DATASET, out, srid=3035)

    assert read_gpkg(
        out, "SELECT srs_name, definition FROM gpkg_spatial_ref_sys WHERE srs_id = 3035"
    ) == [("Shifted LAEA", SHIFTED_3035)]
    (blob,) = read_gpkg(out, "SELECT geom FROM t")[0]
    assert geometry(blob).equals_exact(shapely.Point(5321000, 3210000), tolerance=0.01)


@pytest.mark.usefixtures("definitions")
def test_codes_and_names_from_the_file_are_accepted() -> None:
    assert resolve_srid("EPSG:900913") == 900913  # not an EPSG code pyproj knows
    assert resolve_srid("urn:ogc:def:crs:EPSG::900913") == 900913
    assert epsg_of("google maps global mercator") == 900913
    assert epsg_of("Shifted LAEA") == 3035
    with pytest.raises(ValueError, match="not an EPSG"):
        resolve_srid(999999)
