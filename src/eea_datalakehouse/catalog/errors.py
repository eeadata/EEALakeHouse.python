from __future__ import annotations


class CatalogOperationError(RuntimeError):
    """A catalog operation (table2view, draft2version, ...) failed outright.

    Distinct from :class:`EngineStartingError`: this means the operation was
    rejected or errored for a real reason (bad SQL, missing path, permission
    denied, ...), not because Dremio was warming up an engine.
    """


class EngineStartingError(RuntimeError):
    """A SQL call likely failed/stalled because a Dremio engine is starting.

    A cold engine can take minutes to answer (see dds_ingestion/folder.py's
    own DDS_TIMEOUT reasoning for the same problem on the ingest side). Rather
    than blocking a caller for that long, executors raise this so the caller
    can record the attempt (see retry_state) and retry later — e.g. from a
    fresh notebook cell or a separate process — instead of holding a
    synchronous connection open indefinitely.
    """

    def __init__(self, message: str, *, idempotency_key: str | None = None) -> None:
        super().__init__(message)
        self.idempotency_key = idempotency_key
