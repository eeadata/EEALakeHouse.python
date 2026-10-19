"""CRS definitions for reprojection and the GeoPackage's ``gpkg_spatial_ref_sys``.

They come from ``epsg_definitions.json``, next to this module: one entry per
code, ``{"3035": {"name": "...", "wkt": "PROJCS[...]"}, ...}`` (WKT1). Every
CRS this package builds — the source and target of a reprojection, and the
definition written into the file — comes from there, so the coordinates and
the SRS row that describes them always agree. A code the file does not have
falls back to pyproj's own EPSG database.

The file is read once, on first use, and kept for the life of the process.
"""

from __future__ import annotations

import json
import threading
from functools import lru_cache
from pathlib import Path

import pyproj

DEFINITIONS_FILE = Path(__file__).with_name("epsg_definitions.json")

_lock = threading.Lock()
_loaded: dict[int, tuple[str, str]] | None = None


def definitions() -> dict[int, tuple[str, str]]:
    """``{code: (name, wkt)}`` from :data:`DEFINITIONS_FILE`."""
    global _loaded
    if _loaded is None:
        with _lock:
            if _loaded is None:
                raw = json.loads(DEFINITIONS_FILE.read_text(encoding="utf-8"))
                _loaded = {
                    int(code): (str(entry["name"]), str(entry["wkt"]))
                    for code, entry in raw.items()
                    if str(code).isdigit() and entry.get("wkt")
                }
    return _loaded


def definition(code: int) -> tuple[str, str] | None:
    """``(name, wkt)`` for ``code`` from the file, or ``None`` if it has none."""
    return definitions().get(code)


@lru_cache(maxsize=256)
def crs_for(code: int) -> pyproj.CRS:
    """The CRS ``code`` names: built from the file's WKT, else pyproj's EPSG
    database (``CRSError`` if neither knows it). Immutable, so shared freely."""
    found = definition(code)
    if found is not None:
        return pyproj.CRS.from_wkt(found[1])
    return pyproj.CRS.from_epsg(code)


def srs_row(code: int) -> tuple[str, str]:
    """``(name, definition)`` for ``code``'s ``gpkg_spatial_ref_sys`` row."""
    found = definition(code)
    if found is not None:
        return found
    crs = pyproj.CRS.from_epsg(code)
    try:
        return crs.name, crs.to_wkt("WKT1_GDAL")
    except pyproj.exceptions.CRSError:
        return crs.name, crs.to_wkt()


def is_known(code: int) -> bool:
    """Whether ``code`` is in the file or in pyproj's EPSG database."""
    if definition(code) is not None:
        return True
    try:
        pyproj.CRS.from_epsg(code)
    except pyproj.exceptions.CRSError:
        return False
    return True


@lru_cache(maxsize=1)
def _codes_by_name() -> dict[str, int]:
    by_name: dict[str, int] = {}
    for code, (name, _) in sorted(definitions().items()):
        by_name.setdefault(name.strip().lower(), code)  # the lowest code wins a shared name
    return by_name


def code_for_name(name: str) -> int:
    """The code the file gives the CRS named ``name`` (any case), or 0."""
    return _codes_by_name().get(name.strip().lower(), 0)
