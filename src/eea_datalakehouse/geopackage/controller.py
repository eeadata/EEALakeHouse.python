"""Export the tables and views under a Dremio catalog path to a GeoPackage.

::

    with Catalog(base_url, token) as catalog, DremioSource(catalog) as source:
        report = GpkgController(source).export(
            "catalog.water_management_resources.bathing_water.bwd",
            output="bwd.gpkg",
            tables=["stations", "assessments"],   # default: every table and view
            limit=1000,                           # default: every row
            srid=3035,                            # default: keep each table's CRS
            workers=4,                            # tables read at once
        )

Each table or view becomes one layer: a **features** table when it is spatial
(see :mod:`.spatial` for how that is decided and where its CRS comes from), an
**attributes** table otherwise.

**Streaming, end to end.** Nothing is read whole. ``workers`` tables are read
at once, each over its own Arrow Flight gRPC channel
(:meth:`DremioSource.batches`), a batch at a time. Each worker turns its
batches into ready-to-insert rows — geometries decoded, reprojected and
encoded as GeoPackage binary (shapely releases the GIL for that) — and hands
them over a bounded queue to the one thread that owns the SQLite file, which
only inserts. The queue holds at most a few batches per worker: a slow disk
slows the readers down instead of filling memory.

The layer's CRS is ``srid`` when given — every row is reprojected to it from
its own CRS — else the CRS of the table's first rows; rows in another CRS are
reprojected to it. Rows whose CRS is unknown are written as they are, and a
table with no known CRS at all gets GeoPackage's "undefined" SRS (-1).
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import TracebackType
from typing import Any, Protocol

import pyarrow as pa
import shapely

from ..catalog.client import Catalog
from ..catalog.sql import FlightSqlExecutor
from .spatial import SpatialPlan, geometries_for, plan_table, reproject, resolve_srid
from .writer import (
    FID_COLUMN,
    UNDEFINED_CARTESIAN,
    EncodedGeometries,
    GeoPackageWriter,
    batch_rows,
    encode_geometries,
    geometry_type_name,
    gpkg_type,
)

logger = logging.getLogger("eea_datalakehouse.geopackage")

GEOMETRY_COLUMN = "geom"
DEFAULT_WORKERS = 4
# Batches a worker may have waiting for the writer before it pauses reading.
BATCHES_AHEAD = 3
_RESERVED_PREFIXES = ("gpkg_", "rtree_", "sqlite_")
# Dremio DATA_TYPE -> GeoPackage type, for a table with no rows to learn from.
_SQL_TYPES = {
    "BOOLEAN": "BOOLEAN",
    "TINYINT": "TINYINT",
    "SMALLINT": "SMALLINT",
    "INTEGER": "MEDIUMINT",
    "BIGINT": "INTEGER",
    "FLOAT": "FLOAT",
    "REAL": "FLOAT",
    "DOUBLE": "DOUBLE",
    "DECIMAL": "DOUBLE",
    "DATE": "DATE",
    "TIMESTAMP": "DATETIME",
    "BINARY VARYING": "BLOB",
    "VARBINARY": "BLOB",
}


class DatasetSource(Protocol):
    """Where :class:`GpkgController` reads from (a Dremio catalog, or a fake in
    tests). ``batches`` is called from several threads at once."""

    def datasets(self, path: str) -> list[tuple[str, str]]:
        """``(full path, "TABLE" | "VIEW")`` for every dataset under ``path``."""
        ...

    def batches(self, sql: str) -> Iterator[pa.RecordBatch]:
        """The rows of a SELECT, as Arrow batches, streamed."""
        ...

    def columns(self, path: str) -> list[tuple[str, str]]:
        """``(column, SQL type)`` for ``path``."""
        ...


class DremioSource:
    """A :class:`DatasetSource` over a :class:`~eea_datalakehouse.catalog.Catalog`.

    The listing uses the REST catalog API (as `%catalog list()`); rows come
    over Arrow Flight. Every stream gets a gRPC channel of its own from a pool
    (:meth:`Catalog.open_flight_channel`, logged in once), so tables read at
    once don't share one connection. :meth:`close` closes the pool's channels.
    """

    def __init__(self, catalog: Catalog) -> None:
        self._catalog = catalog
        self._idle: queue.SimpleQueue[FlightSqlExecutor] = queue.SimpleQueue()
        self._opened: list[FlightSqlExecutor] = []
        self._lock = threading.Lock()

    def __enter__(self) -> DremioSource:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        with self._lock:
            channels, self._opened = self._opened, []
        for channel in channels:
            channel.close()

    def datasets(self, path: str) -> list[tuple[str, str]]:
        rest = self._catalog._catalog_rest  # noqa: SLF001 — the listing %catalog list uses
        return rest.list_dataset_kinds(path.split("."))

    def batches(self, sql: str) -> Iterator[pa.RecordBatch]:
        channel = self._channel()
        try:
            yield from channel.stream_batches(sql, idempotency_key=f"gpkg-{uuid.uuid4()}")
        finally:
            self._idle.put(channel)

    def columns(self, path: str) -> list[tuple[str, str]]:
        info = self._catalog.gettableitemsfrom(path, idempotency_key=f"gpkg-{uuid.uuid4()}")
        return list(info.schema.items())

    def _channel(self) -> FlightSqlExecutor:
        try:
            return self._idle.get_nowait()
        except queue.Empty:
            channel = self._catalog.open_flight_channel()
            with self._lock:
                self._opened.append(channel)
            return channel


@dataclass(frozen=True)
class GpkgLayer:
    """One table or view as it was written."""

    source: str
    layer: str
    kind: str  # "features" or "attributes"
    rows: int
    geometry_from: str | None = None  # the source column(s) the geometry came from
    geometry_type: str | None = None
    srs_id: int | None = None
    notes: tuple[str, ...] = ()
    seconds: float = 0.0  # from the first query to the last row handed to the writer


@dataclass(frozen=True)
class GpkgReport:
    """What :meth:`GpkgController.export` wrote.

    It is also the written file's path (``os.PathLike``), so it can be handed
    to anything that takes one — `%dds push` included::

        report = %gpkg export()
        %dds push(DELIVERABLES, report)
    """

    path: Path
    layers: tuple[GpkgLayer, ...] = field(default_factory=tuple)

    def __fspath__(self) -> str:
        return os.fspath(self.path)

    def __repr__(self) -> str:
        lines = [f"wrote {self.path} — {len(self.layers)} layer(s):"]
        for layer in self.layers:
            if layer.kind == "features":
                what = (
                    f"features, {layer.geometry_type or 'GEOMETRY'}, "
                    f"SRS {layer.srs_id}, geometry from {layer.geometry_from}"
                )
            else:
                what = "attributes (no geometry)"
            lines.append(f"  {layer.layer}  ←  {layer.source}  [{what}; {layer.rows} rows]")
            lines += [f"      note: {note}" for note in layer.notes]
        return "\n".join(lines)


# -- messages from the workers to the writer -----------------------------------


@dataclass(frozen=True)
class _Create:
    name: str
    columns: list[tuple[str, str]]
    geometry_column: str | None
    srs_id: int
    description: str


@dataclass(frozen=True)
class _Rows:
    name: str
    rows: list[tuple[Any, ...]]
    encoded: EncodedGeometries | None


@dataclass(frozen=True)
class _Done:
    index: int
    layer: GpkgLayer


class _Stopped(Exception):
    """The export is being abandoned: a worker stops quietly."""


class GpkgController:
    """Writes Dremio tables and views to GeoPackage files."""

    def __init__(self, source: DatasetSource) -> None:
        self._source = source

    def export(
        self,
        catalog_path: str,
        output: str | Path,
        *,
        tables: Sequence[str] | None = None,
        limit: int | None = None,
        srid: int | str | None = None,
        overwrite: bool = True,
        workers: int = DEFAULT_WORKERS,
        progress: Callable[[str], None] | None = None,
    ) -> GpkgReport:
        """Write the tables and views under ``catalog_path`` to ``output``.

        - ``tables``: which ones, by name relative to ``catalog_path``
          (``"stations"``, ``"sub.stations"``) or full path; default all, at
          any depth. A name that matches nothing is an error.
        - ``limit``: at most this many rows per table; default all.
        - ``srid``: the layers' CRS (``3035``, ``"EPSG:3035"``); every
          geometry is reprojected to it. Default: each table keeps its own.
        - ``overwrite``: replace an existing ``output`` (default); with
          ``False`` an existing file is an error.
        - ``workers``: how many tables are read from Dremio at once, each over
          its own Flight channel.
        - ``progress``: called with one line of text as the export starts, as
          each table or view is written, if one fails (naming it), and at the
          end — ``print`` in a notebook. Default: log only.
        """
        if limit is not None and (
            isinstance(limit, bool) or not isinstance(limit, int) or limit < 1
        ):
            raise ValueError(f"limit must be a positive whole number, not {limit!r}")
        if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
            raise ValueError(f"workers must be a positive whole number, not {workers!r}")
        target = resolve_srid(srid) if srid is not None else None
        output = Path(output)
        if output.exists() and not overwrite:
            raise FileExistsError(f"{output} already exists; pass overwrite=True to replace it")
        if not output.parent.is_dir():
            raise FileNotFoundError(f"no such folder: {output.parent}")
        datasets = select_datasets(self._source.datasets(catalog_path), catalog_path, tables)
        names = layer_names(catalog_path, [path for path, _ in datasets])
        jobs = [
            _Job(i, path, kind, name, limit, target)
            for i, ((path, kind), name) in enumerate(zip(datasets, names, strict=True))
        ]
        workers = min(workers, len(jobs))
        say = _Progress(progress)
        say(
            f"exporting {len(jobs)} table(s)/view(s) from {catalog_path} to {output}, "
            f"{workers} at a time"
        )
        started = time.perf_counter()
        layers = self._run(jobs, output, workers, say)
        rows = sum(layer.rows for layer in layers)
        say(
            f"done: {output} — {len(layers)} layer(s), {rows:,} rows "
            f"in {time.perf_counter() - started:.1f}s"
        )
        return GpkgReport(output, tuple(layers))

    # -- the pipeline -----------------------------------------------------

    def _run(self, jobs: list[_Job], output: Path, workers: int, say: _Progress) -> list[GpkgLayer]:
        stop = threading.Event()
        inbox: queue.Queue[Any] = queue.Queue(maxsize=BATCHES_AHEAD * workers)
        errors: list[tuple[_Job, BaseException]] = []  # a failed worker's, for the writer

        def send(message: Any) -> None:
            while True:
                if stop.is_set():
                    raise _Stopped
                try:
                    inbox.put(message, timeout=0.1)
                    return
                except queue.Full:
                    continue

        def work(job: _Job) -> None:
            started = time.perf_counter()

            def send_timed(message: Any) -> None:
                if isinstance(message, _Done):
                    seconds = round(time.perf_counter() - started, 2)
                    message = _Done(message.index, replace(message.layer, seconds=seconds))
                send(message)

            try:
                _produce(self._source, job, send_timed, stop)
            except _Stopped:
                pass
            except BaseException as exc:  # noqa: BLE001 — handed to the writer thread
                errors.append((job, exc))
                stop.set()

        results: dict[int, GpkgLayer] = {}
        by_name = {job.name: job for job in jobs}
        writing: _Job | None = None  # the table the writer is busy with, for an error message
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="gpkg") as pool:
            futures = [pool.submit(work, job) for job in jobs]
            try:
                with GeoPackageWriter(output) as gpkg:
                    while len(results) < len(jobs):
                        if errors:
                            failed, error = errors[0]
                            say.failed(failed, error, output)
                            raise error
                        try:
                            message = inbox.get(timeout=0.1)
                        except queue.Empty:
                            continue
                        writing = by_name.get(getattr(message, "name", ""))
                        if isinstance(message, _Create):
                            gpkg.create_table(
                                message.name,
                                message.columns,
                                geometry_column=message.geometry_column,
                                srs_id=message.srs_id,
                                description=message.description,
                            )
                        elif isinstance(message, _Rows):
                            gpkg.insert_encoded(message.name, message.rows, message.encoded)
                        else:
                            results[message.index] = message.layer
                            say.done(message.layer, len(results), len(jobs))
            except BaseException as exc:
                if not errors and writing is not None:
                    say.failed(writing, exc, output)  # SQLite refused what a worker sent
                raise
            finally:
                stop.set()
                _drain(inbox)  # unblock any worker still waiting to send
                for future in futures:
                    future.result()
        return [results[job.index] for job in jobs]


class _Progress:
    """The ``progress`` callback (and the log), one line per event."""

    def __init__(self, callback: Callable[[str], None] | None) -> None:
        self._callback = callback

    def __call__(self, line: str) -> None:
        logger.info("%s", line)
        if self._callback is not None:
            self._callback(line)

    def done(self, layer: GpkgLayer, count: int, total: int) -> None:
        if layer.kind == "features":
            what = f"features, {layer.geometry_type}, SRS {layer.srs_id}"
        else:
            what = "attributes"
        line = (
            f"✓ {count}/{total} {layer.layer} ← {layer.source}: {what}, "
            f"{layer.rows:,} rows, {layer.seconds:.1f}s"
        )
        self(line + "".join(f"\n    note: {note}" for note in layer.notes))

    def failed(self, job: _Job, error: BaseException, output: Path) -> None:
        self(
            f"✗ {job.name} ← {job.path}: {type(error).__name__}: {error}\n"
            f"    export stopped: the other tables were cancelled and {output} was not written"
        )


@dataclass(frozen=True)
class _Job:
    index: int
    path: str
    kind: str
    name: str
    limit: int | None
    target: int | None


def _drain(inbox: queue.Queue[Any]) -> None:
    while True:
        try:
            inbox.get_nowait()
        except queue.Empty:
            return


def _produce(source: DatasetSource, job: _Job, send: Any, stop: threading.Event) -> None:
    """Read one table and send the writer its create, its rows, then done."""
    sql = f"SELECT * FROM {_quote_path(job.path)}" + (f" LIMIT {job.limit}" if job.limit else "")
    description = f"{job.kind.lower()} {job.path}"
    batches = iter(source.batches(sql))
    try:
        first = next(batches, None)
        if first is None:
            columns = [
                (col, _SQL_TYPES.get(sql_type.upper(), "TEXT"))
                for col, sql_type in source.columns(job.path)
            ]
            send(_Create(job.name, _rename_fid(columns), None, UNDEFINED_CARTESIAN, description))
            send(
                _Done(job.index, GpkgLayer(job.path, job.name, "attributes", 0, notes=("no rows",)))
            )
            return
        plan = plan_table(first.schema, first)
        columns = [(f.name, gpkg_type(f.type)) for f in first.schema]
        if plan is None:
            send(_Create(job.name, _rename_fid(columns), None, UNDEFINED_CARTESIAN, description))
            rows = 0
            for batch in _chain(first, batches):
                if stop.is_set():
                    raise _Stopped
                send(_Rows(job.name, batch_rows(batch), None))
                rows += batch.num_rows
            send(_Done(job.index, GpkgLayer(job.path, job.name, "attributes", rows)))
            return
        send(
            _Done(
                job.index,
                _produce_features(job, plan, first, batches, columns, description, send, stop),
            )
        )
    finally:
        close = getattr(batches, "close", None)
        if close is not None:
            close()  # stops a Flight stream left part-read


def _produce_features(
    job: _Job,
    plan: SpatialPlan,
    first: pa.RecordBatch,
    batches: Iterator[pa.RecordBatch],
    columns: list[tuple[str, str]],
    description: str,
    send: Any,
    stop: threading.Event,
) -> GpkgLayer:
    notes: list[str] = []
    # The encoded geometry column becomes the layer's geometry; every other
    # column (latitude/longitude and the CRS column included) stays.
    kept = [c for c in columns if c[0] != plan.geometry_column]
    lowered = {c.lower() for c, _ in kept}
    geometry_column = next(
        name for name in (GEOMETRY_COLUMN, "geometry", "gpkg_geom") if name not in lowered
    )
    _, first_srids = geometries_for(plan, first)
    known = first_srids[first_srids > 0]
    if not len(known):
        srs_id = UNDEFINED_CARTESIAN
        notes.append(
            "no CRS in the geometries or a CRS column: SRS left undefined"
            + (f", not reprojected to {job.target}" if job.target is not None else "")
        )
    elif job.target is not None:
        srs_id = job.target
    else:
        srs_id = int(Counter(known.tolist()).most_common(1)[0][0])
    send(_Create(job.name, _rename_fid(kept), geometry_column, srs_id, description))

    kept_names = [c for c, _ in kept]
    rows = unknown = undecoded = 0
    types: set[str] = set()
    for batch in _chain(first, batches):
        if stop.is_set():
            raise _Stopped
        geometries, srids = geometries_for(plan, batch)
        present = ~shapely.is_missing(geometries)
        if plan.geometry_column is not None:
            source_present = (
                batch.column(plan.geometry_column).is_valid().to_numpy(zero_copy_only=False)
            )
            undecoded += int((source_present & ~present).sum())
        unknown += int(((srids == 0) & present).sum())
        if srs_id > 0:
            geometries = reproject(geometries, srids, srs_id)
        encoded = encode_geometries(geometries, srs_id)
        types |= encoded.types
        send(_Rows(job.name, batch_rows(batch.select(kept_names)), encoded))
        rows += batch.num_rows
    if undecoded:
        notes.append(f"{undecoded} value(s) in {plan.geometry_column} did not decode: no geometry")
    if unknown and len(known):
        notes.append(f"{unknown} row(s) had no CRS: written untransformed")
    source = (
        str(plan.geometry_column)
        if plan.kind == "geometry"
        else f"{plan.latitude}/{plan.longitude}"
    )
    if plan.crs_column:
        source += f" (CRS from {plan.crs_column} where the geometry has none)"
    return GpkgLayer(
        job.path,
        job.name,
        "features",
        rows,
        geometry_from=source,
        geometry_type=geometry_type_name(types),
        srs_id=srs_id,
        notes=tuple(notes),
    )


def select_datasets(
    available: list[tuple[str, str]], catalog_path: str, tables: Sequence[str] | None
) -> list[tuple[str, str]]:
    """The datasets ``tables`` names (all when ``None`` or empty), in listing order."""
    if not available:
        raise ValueError(f"there are no tables or views under {catalog_path}")
    if not tables:
        return available
    if isinstance(tables, str):
        tables = [tables]
    chosen: list[tuple[str, str]] = []
    missing: list[str] = []
    for wanted in tables:
        wanted = wanted.strip().replace("/", ".")
        match = [
            item
            for item in available
            if item[0] == wanted
            or item[0] == f"{catalog_path}.{wanted}"
            or item[0].endswith(f".{wanted}")
        ]
        if not match:
            missing.append(wanted)
        chosen += [m for m in match if m not in chosen]
    if missing:
        listed = ", ".join(_relative(p, catalog_path) for p, _ in available)
        raise ValueError(
            f"no table or view {', '.join(map(repr, missing))} under {catalog_path}; "
            f"there are: {listed}"
        )
    return [item for item in available if item in chosen]


def layer_names(catalog_path: str, paths: Sequence[str]) -> list[str]:
    """One GeoPackage table name per dataset: its own name, or its path below
    ``catalog_path`` joined with ``_`` when two datasets share a name."""
    leaves = [p.rsplit(".", 1)[-1] for p in paths]
    clashing = {leaf for leaf, n in Counter(leaves).items() if n > 1}
    names = []
    for path, leaf in zip(paths, leaves, strict=True):
        name = _relative(path, catalog_path).replace(".", "_") if leaf in clashing else leaf
        if name.lower().startswith(_RESERVED_PREFIXES):
            name = f"t_{name}"
        names.append(name)
    return names


def _relative(path: str, catalog_path: str) -> str:
    return path[len(catalog_path) + 1 :] if path.startswith(f"{catalog_path}.") else path


def _rename_fid(columns: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """A source column called ``fid`` would clash with the GeoPackage key."""
    return [(f"{c}_src" if c.lower() == FID_COLUMN else c, t) for c, t in columns]


def _chain(first: pa.RecordBatch, rest: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    yield first
    yield from rest


def _quote_path(path: str) -> str:
    return ".".join('"' + segment.replace('"', '""') + '"' for segment in path.split("."))
