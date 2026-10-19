from __future__ import annotations

from typing import Any

import pytest
from IPython.testing import globalipapp


@pytest.fixture(scope="session")
def shell() -> Any:
    """The one process-wide IPython test shell.

    `globalipapp.start_ipython()` only actually starts (and returns) a shell
    on its very first call anywhere in this process — every later call
    returns `None` (see its own "should only ever run once" guard) — so every
    test in this package shares this one fixture instead of each starting
    their own and getting `None`.
    """
    return globalipapp.start_ipython()
