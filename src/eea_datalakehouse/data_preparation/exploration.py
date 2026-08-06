from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class Explorer(ABC):
    """Base class for exploring acquired data before it is transformed."""

    @abstractmethod
    def explore(self, data: Any) -> Any:
        """Inspect ``data`` and return exploration results (profile, schema, summary, ...)."""
