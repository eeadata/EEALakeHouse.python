from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


class DataValidationError(Exception):
    """Raised when a record fails validation before being transformed to parquet."""


class ParquetTransformer(ABC):
    """Base class for a dataset's transform-to-parquet stage.

    ``prepare`` and ``validate`` are dataset-agnostic staging steps shared by
    every flow; ``to_parquet`` is where a concrete subclass encodes the
    schema and write logic specific to its own dataset.

    Parameters
    ----------
    required_fields:
        Field names that every record must contain to pass ``validate``.
    """

    def __init__(self, required_fields: Sequence[str] = ()) -> None:
        self.required_fields = tuple(required_fields)

    def prepare(self, source: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Normalize an iterable of raw records into a list of plain dicts."""
        return [dict(record) for record in source]

    def validate(self, records: Sequence[Mapping[str, Any]]) -> bool:
        """Check that every record contains all required fields.

        Returns ``True`` when every record is valid, otherwise raises
        ``DataValidationError`` describing the first invalid record.
        """
        for index, record in enumerate(records):
            missing = [field for field in self.required_fields if field not in record]
            if missing:
                raise DataValidationError(
                    f"record {index} is missing required fields: {missing}"
                )
        return True

    @abstractmethod
    def to_parquet(self, records: Sequence[Mapping[str, Any]], destination: Path) -> Path:
        """Write validated records to a parquet file at ``destination``."""
