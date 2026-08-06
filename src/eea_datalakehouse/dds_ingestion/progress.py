"""Progress-bar abstraction that degrades gracefully without ``tqdm``.

If ``tqdm`` is installed a real bar is shown; otherwise we fall back to a tiny
no-frills reporter that prints occasional line updates. Either way the public
surface is the same: ``update(n)`` and ``close()``.
"""

from __future__ import annotations

from typing import Protocol


class ProgressBar(Protocol):
    def update(self, n: int = 1) -> object: ...

    def close(self) -> None: ...


class _NullProgress:
    """Fallback used when tqdm is unavailable; prints sparse text updates."""

    def __init__(self, total: int, desc: str) -> None:
        self._total = total
        self._desc = desc
        self._done = 0
        print(f"{desc}: 0/{total}")

    def update(self, n: int = 1) -> object:
        self._done += n
        print(f"{self._desc}: {self._done}/{self._total}")
        return None

    def close(self) -> None:
        return None


def make_progress_bar(total: int, desc: str = "Uploading") -> ProgressBar:
    """Return a tqdm bar if available, else a minimal text reporter."""

    try:
        from tqdm.auto import tqdm
    except ImportError:
        return _NullProgress(total, desc)
    return tqdm(total=total, desc=desc, unit="file")
