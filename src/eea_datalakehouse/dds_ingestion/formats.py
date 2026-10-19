"""The client's one table of ingest file formats (PY-6).

Everything here that differs between Parquet, CSV and JSON — which extensions a
folder scan picks up, and how to read a file's schema locally — lives in one
:class:`ClientFormat` per format. It mirrors the server's format handlers
(``app/core/ingest_formats/`` in the DDS server), which stay the authority:
the local readers only let a script fail, or fill in columns, *before* an
upload, never instead of the server's own checks.

Local schemas use the same Dremio type names the server derives from a Parquet
footer (``app/core/parquet_schema.py::_dremio_type``), and :func:`compare_columns`
applies the server's comparison rules (``app/core/ingest_formats/columns.py``),
so a mismatch found here is the one the server would report.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
import pyarrow.parquet as pq

from .models import DataFormat

Columns = list[tuple[str, str]]


def dremio_type(t: pa.DataType, *, column: str) -> str:
    """The Dremio SQL type DDS uses for an Arrow type (same mapping as the server)."""
    if pa.types.is_dictionary(t):
        return dremio_type(t.value_type, column=column)
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_int64(t) or pa.types.is_uint32(t) or pa.types.is_uint64(t):
        return "BIGINT"
    if pa.types.is_integer(t):
        return "INT"
    if pa.types.is_float16(t) or pa.types.is_float32(t):
        return "FLOAT"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_decimal(t):
        return f"DECIMAL({t.precision}, {t.scale})"
    if pa.types.is_date(t):
        return "DATE"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP"
    if pa.types.is_time(t):
        return "TIME"
    if pa.types.is_binary(t) or pa.types.is_large_binary(t) or pa.types.is_fixed_size_binary(t):
        return "VARBINARY"
    if pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_null(t):
        return "VARCHAR"
    raise ValueError(
        f"column {column!r} has type {t}, which has no Dremio SQL equivalent "
        "(nested and complex columns are not supported)"
    )


def _columns(schema: pa.Schema) -> Columns:
    return [(f.name, dremio_type(f.type, column=f.name)) for f in schema]


def _widen(a: str, b: str) -> str:
    """One type covering both, for a column seen in several files."""
    if a == b:
        return a
    if {a, b} == {"INT", "BIGINT"}:
        return "BIGINT"
    if {a, b} <= {"INT", "BIGINT", "FLOAT", "DOUBLE"}:
        return "DOUBLE"
    return "VARCHAR"


def merge_columns(per_file: Sequence[Columns]) -> Columns:
    """Unify the files' columns in first-seen order, widening where they differ."""
    merged: dict[str, str] = {}
    for columns in per_file:
        for name, sql_type in columns:
            merged[name] = _widen(merged[name], sql_type) if name in merged else sql_type
    return list(merged.items())


def _parquet_schema(path: Path, options: Mapping[str, Any] | None) -> Columns:
    return _columns(pq.read_schema(path))


def _csv_schema(path: Path, options: Mapping[str, Any] | None) -> Columns:
    opts = options or {}
    parse = pa_csv.ParseOptions(
        delimiter=opts.get("delimiter") or ",", quote_char=opts.get("quote") or '"'
    )
    read = pa_csv.ReadOptions(autogenerate_column_names=opts.get("header") is False)
    return _columns(pa_csv.read_csv(path, read_options=read, parse_options=parse).schema)


def _json_schema(path: Path, options: Mapping[str, Any] | None) -> Columns:
    if (options or {}).get("format") == "array":
        raise ValueError("a JSON array file cannot be read locally; only JSON lines can")
    return _columns(pa_json.read_json(path).schema)


@dataclass(frozen=True, slots=True)
class ClientFormat:
    """What the client needs to know about one data format."""

    name: DataFormat
    extensions: tuple[str, ...]
    # True when the files carry their own schema (Parquet's footer): given columns
    # are then CHECKED against it, never needed to create the table.
    has_footer: bool
    read_schema: Callable[[Path, Mapping[str, Any] | None], Columns]


FORMATS: dict[DataFormat, ClientFormat] = {
    "parquet": ClientFormat("parquet", (".parquet",), True, _parquet_schema),
    "csv": ClientFormat("csv", (".csv",), False, _csv_schema),
    "json": ClientFormat("json", (".json", ".ndjson"), False, _json_schema),
}


def local_columns(
    folder: Path, rel_paths: Sequence[str], fmt: DataFormat, options: Mapping[str, Any] | None
) -> Columns:
    """The unified schema of the given files, read locally (every file, not a sample)."""
    reader = FORMATS[fmt].read_schema
    return merge_columns([reader(folder / rel, options) for rel in rel_paths])


def _normalised_type(sql_type: str) -> str:
    t = re.sub(r"\s+", "", sql_type.upper())
    return "VARCHAR" if t.startswith("VARCHAR(") else t


def compare_columns(
    given: Sequence[tuple[str, str]], actual: Sequence[tuple[str, str]]
) -> list[str]:
    """Where ``given`` columns disagree with ``actual`` ones — the server's rules
    (names case-insensitive; types compared after normalising case, spacing and a
    ``VARCHAR`` length; extra ``actual`` columns allowed), the server's wording."""
    by_name = {name.lower(): (name, sql_type) for name, sql_type in actual}
    problems: list[str] = []
    for name, sql_type in given:
        found = by_name.get(name.lower())
        if found is None:
            problems.append(f"{name}: not in the files")
        elif _normalised_type(sql_type) != _normalised_type(found[1]):
            problems.append(f"{name}: given {sql_type}, the files have {found[1]}")
    return problems
