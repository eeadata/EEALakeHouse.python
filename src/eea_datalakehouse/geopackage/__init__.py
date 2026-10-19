"""eea_datalakehouse.geopackage — Dremio tables and views to a GeoPackage file.

::

    from eea_datalakehouse.catalog import Catalog
    from eea_datalakehouse.geopackage import DremioSource, GpkgController

    with Catalog(base_url, token, username=user) as catalog, DremioSource(catalog) as source:
        report = GpkgController(source).export(
            "catalog.water_management_resources.bathing_water.bwd", "bwd.gpkg", srid=3035
        )

Spatial tables become feature layers, the rest attribute tables (see
:mod:`.spatial`). Rows are streamed over Arrow Flight, several tables at once
on their own channels (see :mod:`.controller`). Written with :mod:`sqlite3`,
``shapely`` and ``pyproj``; no GDAL. :class:`GpkgSession` is what the `%gpkg`
magic dispatches onto.
"""

from __future__ import annotations

from .controller import DatasetSource, DremioSource, GpkgController, GpkgLayer, GpkgReport
from .session import GpkgSession, GpkgSessionError
from .spatial import SpatialPlan, plan_table, resolve_srid
from .writer import GeoPackageWriter

__all__ = [
    "DatasetSource",
    "DremioSource",
    "GeoPackageWriter",
    "GpkgController",
    "GpkgLayer",
    "GpkgReport",
    "GpkgSession",
    "GpkgSessionError",
    "SpatialPlan",
    "plan_table",
    "resolve_srid",
]
