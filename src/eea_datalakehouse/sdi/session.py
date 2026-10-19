"""Notebook-facing session behind the `%sdi` magic::

    sdi = SdiSession()                         # %sdi — reads the SDI catalogue
    sdi.get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")

- **`SdiSession` remembers the last record** `get_xml` fetched (`last`).
  Pushing it to DDS is `%dds`'s job (`eea_datalakehouse.dds_documents.session`):
  a record is a file `%dds push` accepts, stored as `{uuid}.xml`.
- **Every failure is a `SdiSessionError`**, so the magic can print it as one
  short line instead of a traceback (the original error is chained as
  ``__cause__``).

Connection details come from the kernel environment: ``SDI_API_URL`` (+
optional ``SDI_USERNAME`` / ``SDI_PASSWORD``). It needs no DDS configuration.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

import httpx

from ..session_utils import call_usage, friendly_errors
from .catalogue import SdiCatalogue
from .controller import SdiController, SdiMetadata
from .errors import SdiError

__all__ = ["SdiSession", "SdiSessionError", "call_usage"]

# Failures a notebook user can act on; anything else is a bug and keeps its traceback.
_EXPECTED = (SdiError, ValueError, TypeError, httpx.HTTPError)


class SdiSessionError(RuntimeError):
    """Any failure of an `SdiSession` call; the original error is ``__cause__``."""


_friendly = friendly_errors(SdiSessionError, _EXPECTED)


class SdiSession:
    """`get_xml` / `resolve_series` for a notebook — what `%sdi` dispatches onto.

    Reads the SDI catalogue only; pushing to DDS is `%dds`'s job.
    ``controller`` is built from the kernel environment if not given (see the
    module docstring). One session lives for the whole kernel under `%sdi`.
    """

    def __init__(
        self,
        *,
        controller: SdiController | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        self._env = env
        self._controller = controller
        self._last: SdiMetadata | None = None

    def __repr__(self) -> str:
        last = self._last.uuid if self._last else None
        return f"SdiSession(last={last!r})"

    @property
    def last(self) -> SdiMetadata | None:
        """The record the last `get_xml` fetched, if any."""
        return self._last

    @_friendly
    def get_xml(self, uuid: str) -> SdiMetadata:
        """Download the ISO 19115-3 XML of SDI record `uuid` and remember it."""
        self._last = self._sdi().get_xml(uuid)
        return self._last

    @_friendly
    def resolve_series(self, series_uuid: str) -> str:
        """The UUID of the one release of `series_uuid` that is not superseded."""
        return self._sdi().resolve_series(series_uuid)

    # -- internals --------------------------------------------------------

    def _sdi(self) -> SdiController:
        if self._controller is None:
            env = dict(os.environ if self._env is None else self._env)
            self._controller = SdiController(SdiCatalogue.from_env(env))
        return self._controller
