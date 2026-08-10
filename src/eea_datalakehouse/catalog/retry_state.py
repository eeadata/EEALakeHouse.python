"""Persisted memory of catalog operations that stalled on a starting engine.

When a SQL call raises :class:`~eea_datalakehouse.catalog.errors.EngineStartingError`,
the caller's idempotency key is recorded here — on disk, so it survives a
notebook restart or a fresh process — along with what was being attempted and
when. A later retry with the same idempotency key picks up the history rather
than silently forgetting the attempt was ever made.

This is deliberately a flat JSON file, not a database: one developer's local
retry bookkeeping, not shared state.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

DEFAULT_STATE_PATH = Path(
    os.environ.get("EEA_CATALOG_RETRY_STATE")
    or (Path.home() / ".cache" / "eea_datalakehouse" / "catalog_retry_state.json")
)


@dataclass
class PendingOperation:
    """One remembered attempt that stalled on (probably) a starting engine."""

    idempotency_key: str
    operation: str
    target: str
    attempts: int
    first_attempted_at: str
    last_attempted_at: str
    last_error: str
    params: dict[str, Any] = field(default_factory=dict)


def _load(state_path: Path) -> dict[str, dict[str, Any]]:
    try:
        return json.loads(state_path.read_text())
    except FileNotFoundError:
        return {}
    except (json.JSONDecodeError, OSError):
        # Corrupt/unreadable state isn't worth failing a catalog operation
        # over — treat it as empty; record() below overwrites it cleanly.
        return {}


def _save(state_path: Path, data: dict[str, dict[str, Any]]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    # Write to a temp file first so a crash mid-write never corrupts the
    # existing state — os.replace is atomic on the same filesystem.
    fd, tmp_name = tempfile.mkstemp(dir=state_path.parent, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp_name, state_path)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def record(
    idempotency_key: str,
    operation: str,
    target: str,
    error: str,
    *,
    params: dict[str, Any] | None = None,
    state_path: Path | None = None,
) -> PendingOperation:
    """Remember that `operation` on `target` stalled, for a later retry."""
    state_path = state_path if state_path is not None else DEFAULT_STATE_PATH
    now = datetime.now(UTC).isoformat()
    data = _load(state_path)
    existing = data.get(idempotency_key)
    pending = PendingOperation(
        idempotency_key=idempotency_key,
        operation=operation,
        target=target,
        attempts=(existing["attempts"] + 1) if existing else 1,
        first_attempted_at=existing["first_attempted_at"] if existing else now,
        last_attempted_at=now,
        last_error=error,
        params=params or {},
    )
    data[idempotency_key] = asdict(pending)
    _save(state_path, data)
    return pending


def get(
    idempotency_key: str, *, state_path: Path | None = None
) -> PendingOperation | None:
    """The remembered pending attempt for `idempotency_key`, if any."""
    state_path = state_path if state_path is not None else DEFAULT_STATE_PATH
    raw = _load(state_path).get(idempotency_key)
    return PendingOperation(**raw) if raw is not None else None


def clear(idempotency_key: str, *, state_path: Path | None = None) -> None:
    """Forget a pending attempt — call this once the operation actually succeeds."""
    state_path = state_path if state_path is not None else DEFAULT_STATE_PATH
    data = _load(state_path)
    if data.pop(idempotency_key, None) is not None:
        _save(state_path, data)


def list_pending(*, state_path: Path | None = None) -> list[PendingOperation]:
    """Every remembered attempt still waiting on a retry."""
    state_path = state_path if state_path is not None else DEFAULT_STATE_PATH
    return [PendingOperation(**raw) for raw in _load(state_path).values()]
