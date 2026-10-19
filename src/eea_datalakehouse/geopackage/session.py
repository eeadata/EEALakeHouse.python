"""Notebook-facing session behind the `%gpkg` magic::

    gpkg = GpkgSession(resolve_path=..., catalog=...)   # %gpkg — writes .gpkg files
    gpkg.export()                                       # the %catalog context, every table
    gpkg.export(tables=["stations"], limit=1000, srid=3035, filename="stations.gpkg")

- **The source is the `%catalog` context.** `export()` reads the dataset
  folder `%catalog use(...)` last set; `path=` picks another one, absolute or
  relative to the context, exactly as `%catalog` resolves paths.
- **The file goes next to the notebook**, named after the dataset folder
  (`bwd.gpkg` for `catalog.water.bwd`), unless `output_dir=` / `filename=`
  say otherwise.
- **Streamed**: `workers` tables are read at once, each over its own Arrow
  Flight channel, a batch at a time (see the controller module).
- **Every failure is a `GpkgSessionError`**, so the magic can print it as one
  short line instead of a traceback (the original error is ``__cause__``).

Reading uses `%catalog`'s own connection (``DREMIO_BASE_URL`` /
``DREMIO_TOKEN``).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from pathlib import Path

import pyproj

from ..catalog.client import Catalog
from ..catalog.errors import CatalogOperationError, EngineStartingError
from ..dds_documents.paths import path_segments
from ..session_utils import check_arg_types, friendly_errors
from .controller import DEFAULT_WORKERS, DremioSource, GpkgController, GpkgReport

GPKG_SUFFIX = ".gpkg"

# Failures a notebook user can act on; anything else is a bug and keeps its traceback.
_EXPECTED = (
    CatalogOperationError,
    EngineStartingError,
    pyproj.exceptions.CRSError,
    ValueError,
    TypeError,
    OSError,
    RuntimeError,
)


class GpkgSessionError(RuntimeError):
    """Any failure of a `GpkgSession` call; the original error is ``__cause__``."""


_friendly = friendly_errors(GpkgSessionError, _EXPECTED)


class GpkgSession:
    """`export` for a notebook — what `%gpkg` dispatches onto.

    ``resolve_path(path)`` turns the ``path`` argument into a catalog path:
    ``None`` means the current `%catalog` context. ``catalog()`` returns the
    :class:`~eea_datalakehouse.catalog.Catalog` to read with; ``controller``
    replaces the one built over it for each export (tests).
    """

    def __init__(
        self,
        *,
        resolve_path: Callable[[str | None], str | None] | None = None,
        catalog: Callable[[], Catalog] | None = None,
        controller: GpkgController | None = None,
    ) -> None:
        self._resolve_path_fn = resolve_path or (lambda path: path)
        self._catalog = catalog
        self._controller = controller

    def __repr__(self) -> str:
        return "GpkgSession()"

    @_friendly
    def export(
        self,
        path: str | None = None,
        tables: Sequence[str] | None = None,
        limit: int | None = None,
        srid: int | str | None = None,
        filename: str | None = None,
        output_dir: str | None = None,
        overwrite: bool = True,
        workers: int = DEFAULT_WORKERS,
    ) -> GpkgReport:
        """Write the tables and views under `path` (default: the `%catalog`
        context) to `output_dir/filename` (default: the notebook's folder and
        `<dataset>.gpkg`): spatial ones as feature layers, the rest as tables.
        `workers` tables are streamed at once, each on its own Flight channel."""
        check_arg_types(
            "export",
            ("path", path, str, True),
            ("tables", tables, (list, tuple, str), True),
            ("limit", limit, int, True),
            ("srid", srid, (int, str), True),
            ("filename", filename, str, True),
            ("output_dir", output_dir, str, True),
            ("overwrite", overwrite, bool, False),
            ("workers", workers, int, False),
        )
        catalog_path = self._catalog_path(path)
        output = output_file(catalog_path, filename, output_dir)
        options = {
            "tables": [tables] if isinstance(tables, str) else tables,
            "limit": limit,
            "srid": srid,
            "overwrite": overwrite,
            "workers": workers,
        }
        if self._controller is not None:
            return self._controller.export(catalog_path, output, **options)  # type: ignore[arg-type]
        if self._catalog is None:
            raise RuntimeError("no Dremio catalog to read from")
        # One pool of Flight channels per export, closed when it ends.
        with DremioSource(self._catalog()) as source:
            return GpkgController(source).export(catalog_path, output, **options)  # type: ignore[arg-type]

    # -- internals --------------------------------------------------------

    def _catalog_path(self, path: str | None) -> str:
        resolved = self._resolve_path_fn(path)
        if not resolved:
            raise ValueError(
                "nothing to export: pick a dataset folder with %catalog use(...) or pass path="
            )
        if len(path_segments(resolved)) < 2:
            raise ValueError(
                f"{resolved!r} is the catalog root, not a dataset folder; "
                "pick one with %catalog use(...) or pass its path"
            )
        return resolved


def output_file(catalog_path: str, filename: str | None, output_dir: str | None) -> Path:
    """``output_dir/filename``: by default the working directory (the
    notebook's folder) and the dataset folder's name; ``.gpkg`` is added to a
    name without it."""
    name = (filename or path_segments(catalog_path)[-1]).strip()
    if not name or "/" in name or "\\" in name or name in (".", ".."):
        raise ValueError(
            f"filename must be a file name, not {filename!r}; use output_dir= for the folder"
        )
    if not name.lower().endswith(GPKG_SUFFIX):
        name += GPKG_SUFFIX
    folder = Path(output_dir).expanduser() if output_dir else Path.cwd()
    return folder / name
