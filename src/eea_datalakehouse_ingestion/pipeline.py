from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence


class IngestionValidationError(Exception):
    """Raised when a record fails validation before ingestion."""


class IngestionPipeline:
    """Stages and validates records before they are loaded into the lakehouse.

    Parameters
    ----------
    required_fields:
        Field names that every record must contain to pass ``validate``.
    """

    def __init__(self, required_fields: Sequence[str] = ()) -> None:
        self.required_fields = tuple(required_fields)

    def hello_world(self) -> str:
        """Return a greeting, useful for a quick install sanity check."""
        return "Hello, world!"

    def prepare(self, source: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Normalize an iterable of raw records into a list of plain dicts."""
        return [dict(record) for record in source]

    def validate(self, records: Sequence[Mapping[str, Any]]) -> bool:
        """Check that every record contains all required fields.

        Returns ``True`` when every record is valid, otherwise raises
        ``IngestionValidationError`` describing the first invalid record.
        """
        for index, record in enumerate(records):
            missing = [field for field in self.required_fields if field not in record]
            if missing:
                raise IngestionValidationError(
                    f"record {index} is missing required fields: {missing}"
                )
        return True
