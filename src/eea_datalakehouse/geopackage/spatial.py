"""Tell spatial tables from tabular ones, decode their geometries, find their CRS.

Dremio has no geometry type, so a table is spatial when one of its columns
*holds* geometries — found by decoding a sample of its values:

- binary values as WKB or EWKB (an SRID inside);
- text values as WKT, EWKT (``SRID=3035;POINT(...)``), hex WKB/EWKB, or GeoJSON.

Columns named like a geometry (``geom``, ``wkt``, ``shape``, ...) are tried
first, then every other text or binary column. A table with none, but with a
latitude and a longitude column, is spatial too: its rows become points in
EPSG:4326.

The CRS of each row, in order: the SRID inside the geometry (EWKB / EWKT),
else the value of a CRS column (``srid``, ``sr_id``, ``epsg``, ``crs``, ...:
``3035``, ``"EPSG:3035"``, an EPSG URN or a CRS name), else EPSG:4326 for
latitude/longitude points, else unknown. Reprojection builds both CRSs from
``epsg_definitions.json`` (see :mod:`.crs`).

Everything here is safe to call from several threads at once.
"""

from __future__ import annotations

import re
import threading
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from typing import Literal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyproj
import shapely

from . import crs

SAMPLE_SIZE = 50

GEOMETRY_NAMES = (
    "geom",
    "geometry",
    "the_geom",
    "wkb_geometry",
    "geom_wkb",
    "geom_wkt",
    "geometry_wkt",
    "wkt",
    "wkb",
    "ewkt",
    "ewkb",
    "shape",
    "geojson",
    "geo_shape",
    "geo_point",
    "location",
    "footprint",
)
CRS_NAMES = (
    "srid",
    "sr_id",
    "srs_id",
    "srsid",
    "epsg",
    "epsg_code",
    "crs",
    "crs_code",
    "crs_epsg",
    "spatial_ref",
    "spatial_reference",
    "coordinate_system",
    "coord_sys",
    "projection",
)
LATITUDE_NAMES = ("lat", "latitude", "y_lat", "lat_dd", "decimal_latitude")
LONGITUDE_NAMES = ("lon", "lng", "long", "longitude", "x_lon", "lon_dd", "decimal_longitude")

Encoding = Literal["wkb", "hexwkb", "wkt", "geojson"]

_HEX = re.compile(r"[0-9A-Fa-f]+")
_EPSG_REFERENCE = re.compile(r"\bEPSG:+(?:[\d.]*:)?(\d+)\s*$", re.IGNORECASE)
_EWKT = re.compile(r"\s*SRID=(\d+);(.*)", re.DOTALL | re.IGNORECASE)
_WKT_START = re.compile(
    r"\s*(SRID=\d+;)?\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|"
    r"GEOMETRYCOLLECTION|LINEARRING)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SpatialPlan:
    """How a table's rows become geometries; ``None`` from :func:`plan_table`
    means the table is tabular."""

    kind: Literal["geometry", "latlon"]
    geometry_column: str | None = None  # the column holding encoded geometries
    encoding: Encoding | None = None
    latitude: str | None = None
    longitude: str | None = None
    crs_column: str | None = None


def _norm(name: str) -> str:
    return name.strip().lower()


def find_column(names: Sequence[str], wanted: Sequence[str]) -> str | None:
    """The first of ``names`` whose lowercase form is in ``wanted``."""
    by_norm = {_norm(n): n for n in names}
    return next((by_norm[w] for w in wanted if w in by_norm), None)


def _encoding_of(value: object) -> Encoding | None:
    """How ``value`` encodes a geometry, if it plausibly does."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "wkb"
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.startswith("{") and '"type"' in text:
        return "geojson"
    if _WKT_START.match(text):
        return "wkt"
    if len(text) >= 18 and len(text) % 2 == 0 and _HEX.fullmatch(text):
        return "hexwkb"
    return None


def decode(values: Sequence[object], encoding: Encoding) -> tuple[np.ndarray, np.ndarray]:
    """``(geometries, srids)`` for ``values``: a shapely geometry per value
    (``None`` where it is null or does not decode) and the SRID found inside
    it (0 where there is none)."""
    n = len(values)
    srids = np.zeros(n, dtype=np.int64)
    if encoding == "wkt":
        texts = np.array([v if isinstance(v, str) else None for v in values], dtype=object)
        # EWKT is rare: the regex runs only on values that carry an SRID prefix.
        for i in np.flatnonzero([isinstance(v, str) and "SRID=" in v[:16].upper() for v in values]):
            match = _EWKT.match(texts[i])
            if match:
                srids[i] = int(match.group(1))
                texts[i] = match.group(2)
        return shapely.from_wkt(texts, on_invalid="ignore"), srids
    if encoding == "geojson":
        array = np.array([v if isinstance(v, str) else None for v in values], dtype=object)
        return shapely.from_geojson(array, on_invalid="ignore"), srids
    # wkb / hexwkb: shapely reads EWKB too, keeping its SRID.
    items: list[str | bytes | None]
    if encoding == "hexwkb":
        items = [v.strip() if isinstance(v, str) else None for v in values]
    else:
        items = [
            bytes(v) if isinstance(v, (bytes, bytearray, memoryview)) else None for v in values
        ]
    geometries = shapely.from_wkb(np.array(items, dtype=object), on_invalid="ignore")
    present = ~shapely.is_missing(geometries)
    srids[present] = shapely.get_srid(geometries[present])
    return geometries, srids


def _decodes(values: Sequence[object], encoding: Encoding) -> bool:
    geometries, _ = decode(values, encoding)
    return bool(len(values)) and not shapely.is_missing(geometries).any()


def plan_table(schema: pa.Schema, sample: pa.RecordBatch | pa.Table) -> SpatialPlan | None:
    """How to turn this table's rows into geometries, or ``None`` if it is tabular.

    ``sample`` is the table's first rows. A column qualifies when every
    non-null sampled value decodes with one encoding.
    """
    names = list(schema.names)
    crs_column = find_column(names, CRS_NAMES)
    by_name = {_norm(n) for n in GEOMETRY_NAMES}
    candidates = [n for n in names if _norm(n) in by_name]
    candidates += [
        n
        for n in names
        if n not in candidates
        and n != crs_column
        and (
            pa.types.is_binary(schema.field(n).type)
            or pa.types.is_large_binary(schema.field(n).type)
            or pa.types.is_string(schema.field(n).type)
            or pa.types.is_large_string(schema.field(n).type)
        )
    ]
    for column in candidates:
        values = [v for v in sample.column(column).to_pylist() if v is not None][:SAMPLE_SIZE]
        encodings = {_encoding_of(v) for v in values}
        if len(encodings) != 1 or None in encodings:
            continue
        (encoding,) = encodings
        if encoding is not None and _decodes(values, encoding):
            return SpatialPlan("geometry", column, encoding, crs_column=crs_column)
    latitude = find_column(names, LATITUDE_NAMES)
    longitude = find_column(names, LONGITUDE_NAMES)
    if latitude and longitude:
        numeric = all(
            pa.types.is_integer(schema.field(c).type)
            or pa.types.is_floating(schema.field(c).type)
            or pa.types.is_decimal(schema.field(c).type)
            for c in (latitude, longitude)
        )
        if numeric:
            return SpatialPlan(
                "latlon", latitude=latitude, longitude=longitude, crs_column=crs_column
            )
    return None


def geometries_for(plan: SpatialPlan, batch: pa.RecordBatch) -> tuple[np.ndarray, np.ndarray]:
    """``(geometries, srids)`` for one batch: srids are the CRS each row is in
    (0 where unknown), from the geometry itself, else the CRS column, else
    EPSG:4326 for latitude/longitude points."""
    if plan.kind == "latlon":
        assert plan.latitude is not None and plan.longitude is not None
        lat_col, lon_col = batch.column(plan.latitude), batch.column(plan.longitude)
        present = pc.and_(pc.is_valid(lat_col), pc.is_valid(lon_col)).to_numpy(zero_copy_only=False)
        lat = pc.cast(lat_col, pa.float64()).fill_null(0).to_numpy(zero_copy_only=False)
        lon = pc.cast(lon_col, pa.float64()).fill_null(0).to_numpy(zero_copy_only=False)
        geometries = np.full(batch.num_rows, None, dtype=object)
        if present.any():
            geometries[present] = shapely.points(lon[present], lat[present])
        srids = np.zeros(batch.num_rows, dtype=np.int64)
        srids[present] = 4326
    else:
        assert plan.geometry_column is not None and plan.encoding is not None
        geometries, srids = decode(batch.column(plan.geometry_column).to_pylist(), plan.encoding)
    if plan.crs_column is not None:
        from_column = epsg_column(batch.column(plan.crs_column))
        embedded = srids > 0
        if plan.kind == "latlon":
            embedded = np.zeros(len(srids), dtype=bool)  # a CRS column beats the 4326 default
        srids = np.where(embedded | (from_column == 0), srids, from_column)
    return geometries, srids


def epsg_column(column: pa.Array | pa.ChunkedArray) -> np.ndarray:
    """:func:`epsg_of` for a whole column, each distinct value parsed once
    (a CRS column holds a handful of values over many rows)."""
    encoded = pc.dictionary_encode(column)
    if isinstance(encoded, pa.ChunkedArray):
        encoded = encoded.combine_chunks()
    codes = np.array([epsg_of(v) for v in encoded.dictionary.to_pylist()] + [0], dtype=np.int64)
    indices = encoded.indices.fill_null(len(codes) - 1).to_numpy(zero_copy_only=False)
    return codes[indices]


def epsg_of(value: object) -> int:
    """The EPSG code ``value`` names (``3035``, ``"EPSG:3035"``, a URN, a CRS
    name...), or 0 if it names none."""
    if value is None or isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return int(value) if float(value).is_integer() and value > 0 else 0
    return _epsg_from_text(str(value).strip())


@lru_cache(maxsize=256)
def _epsg_from_text(text: str) -> int:
    if not text:
        return 0
    if text.isdigit():
        return int(text)
    reference = _EPSG_REFERENCE.search(text)  # EPSG:3035, urn:ogc:def:crs:EPSG::3035, ...
    if reference:
        return int(reference.group(1))
    by_name = crs.code_for_name(text)  # a CRS name as epsg_definitions.json spells it
    if by_name:
        return by_name
    try:
        return pyproj.CRS.from_user_input(text).to_epsg(min_confidence=70) or 0
    except pyproj.exceptions.CRSError:
        return 0


def resolve_srid(value: int | str) -> int:
    """``value`` (``3035``, ``"EPSG:3035"``, a URN, a CRS name) as an EPSG
    code; ``ValueError`` if it names no EPSG CRS."""
    if isinstance(value, bool):
        raise TypeError("srid must be an EPSG code (int) or a CRS name (str), not bool")
    code = epsg_of(value)
    if code and not crs.is_known(code):
        code = 0
    if not code:
        raise ValueError(f"srid {value!r} is not an EPSG coordinate reference system")
    return code


# A pyproj Transformer must not be shared between threads: one per thread.
_local = threading.local()


def _transformer(source: int, target: int) -> pyproj.Transformer:
    cache: dict[tuple[int, int], pyproj.Transformer] = getattr(_local, "transformers", {})
    _local.transformers = cache
    key = (source, target)
    if key not in cache:
        # Both ends from epsg_definitions.json (see .crs), the definitions the
        # GeoPackage's SRS rows are written from too.
        cache[key] = pyproj.Transformer.from_crs(
            crs.crs_for(source), crs.crs_for(target), always_xy=True
        )
    return cache[key]


def reproject(geometries: np.ndarray, srids: np.ndarray, target: int) -> np.ndarray:
    """``geometries`` in ``target``: each row is transformed from its own
    ``srids`` value; rows with an unknown CRS (0) are left as they are."""
    out = geometries.copy()
    for source in np.unique(srids):
        if source in (0, target):
            continue
        rows = (srids == source) & ~shapely.is_missing(geometries)
        if not rows.any():
            continue
        transformer = _transformer(int(source), target)

        def transform(
            coords: np.ndarray, transformer: pyproj.Transformer = transformer
        ) -> np.ndarray:
            if coords.shape[1] == 3:
                return np.column_stack(
                    transformer.transform(coords[:, 0], coords[:, 1], coords[:, 2])
                )
            return np.column_stack(transformer.transform(coords[:, 0], coords[:, 1]))

        # Z only where a geometry has one: a 2D geometry passed with include_z
        # gets NaN for Z, and some transformations (to Web Mercator, ...) then
        # return NaN for X and Y too.
        has_z = np.zeros(len(geometries), dtype=bool)
        has_z[rows] = shapely.has_z(geometries[rows])
        for subset, include_z in ((rows & has_z, True), (rows & ~has_z, False)):
            if subset.any():
                out[subset] = shapely.transform(geometries[subset], transform, include_z=include_z)
    return out
