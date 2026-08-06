from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class VocabularyLoader(ABC):
    """Base class for loading the controlled vocabulary a dataset validates against."""

    @abstractmethod
    def load_vocabulary(self) -> Any:
        """Fetch and return the dataset's controlled vocabulary or reference data."""
