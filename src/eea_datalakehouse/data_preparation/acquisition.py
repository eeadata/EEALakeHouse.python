from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DataAcquirer(ABC):
    """Base class for a dataset's data-acquisition stage.

    A concrete subclass knows how to fetch one dataset's raw source data (an
    SDI download, an API, a file share, ...) and hands back whatever raw form
    is most convenient for that dataset's exploration/transformation steps to
    consume next.
    """

    @abstractmethod
    def acquire(self) -> Any:
        """Fetch and return the dataset's raw source data."""
