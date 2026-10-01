"""Notebook-facing sessions behind the `%sdi` and `%metadata` magics.

Two sessions, one per magic, so reading SDI and writing DDS stay separate::

    sdi = SdiSession()                         # %sdi — reads the SDI catalogue
    sdi.get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")

    metadata = MetadataSession(last=lambda: sdi.last)   # %metadata — writes DDS
    metadata.push_to_dds("catalog/water_management_resources/bathing_water/bwd")

- **`SdiSession` remembers the last record** `get_xml` fetched, and
  `MetadataSession.push_to_dds(dds_path)` uploads it by default, so a notebook
  doesn't have to hold it in a variable.
- **Every failure is a `SdiSessionError` / `MetadataSessionError`**, so the
  magic can print it as one short line instead of a traceback (the original
  error is chained as ``__cause__``).

Connection details come from the kernel environment, as `%catalog` /
`%ingest` read theirs:

- `SdiSession`: ``SDI_API_URL`` (+ optional ``SDI_USERNAME`` /
  ``SDI_PASSWORD``). It needs no DDS configuration at all.
- `MetadataSession`: ``DDS_BASE_URL`` and the Dremio identity. The identity is
  ``_DREMIO_USER`` / ``_DREMIO_PWD`` (what `%ingest` reads), falling back to
  ``DREMIO_USERNAME`` / ``DREMIO_TOKEN`` (what `%catalog` reads).

**`DDS_BASE_URL` comes from a `.env` file first.** When a `MetadataSession` is
built, it looks for `.env` in the notebook's working directory, then in each
parent directory, and takes `DDS_BASE_URL` from the first one found. Only when
no `.env` sets it does the kernel environment's ``DDS_BASE_URL`` apply.
Nothing else is read from the file, and it never changes ``os.environ``.
`dds_base_url()` shows which URL is in use and where it came from.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar

import httpx

from ..dds_documents import DocumentsApiError, DocumentsClient
from ..dds_ingestion.credentials import (
    ENV_BASE_URL,
    ENV_PWD,
    ENV_USER,
    DremioCreds,
    MissingCredentialsError,
    load_base_url,
)
from .catalogue import SdiCatalogue
from .controller import DEFAULT_FOLDER, PushResult, SdiController, SdiMetadata
from .errors import SdiError

# What `%catalog` reads (see notebook/magics.py's _build_catalog_session).
ENV_CATALOG_USER = "DREMIO_USERNAME"
ENV_CATALOG_TOKEN = "DREMIO_TOKEN"  # noqa: S105 — env var name, not a secret value
DOTENV_NAME = ".env"

_F = TypeVar("_F", bound=Callable[..., Any])

# Failures a notebook user can act on; anything else is a bug and keeps its traceback.
_EXPECTED = (SdiError, DocumentsApiError, MissingCredentialsError, ValueError, httpx.HTTPError)


class SdiSessionError(RuntimeError):
    """Any failure of an `SdiSession` call; the original error is ``__cause__``."""


class MetadataSessionError(RuntimeError):
    """Any failure of a `MetadataSession` call; the original error is ``__cause__``."""


def _friendly(error: type[RuntimeError]) -> Callable[[_F], _F]:
    """Re-raise every expected failure of the decorated method as ``error``."""

    def decorate(method: _F) -> _F:
        @wraps(method)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return method(*args, **kwargs)
            except error:
                raise
            except _EXPECTED as exc:
                raise error(str(exc) or type(exc).__name__) from exc

        return wrapper  # type: ignore[return-value]

    return decorate


def load_dds_creds(env: Mapping[str, str] | None = None) -> DremioCreds:
    """Dremio identity for the DDS upload: `%ingest`'s variables, else `%catalog`'s."""
    source = os.environ if env is None else env
    for user_var, token_var in ((ENV_USER, ENV_PWD), (ENV_CATALOG_USER, ENV_CATALOG_TOKEN)):
        user, token = source.get(user_var), source.get(token_var)
        if user and token:
            return DremioCreds(username=user, _password=token)
    raise MissingCredentialsError(
        f"pushing to DDS needs a Dremio identity in the kernel environment: "
        f"{ENV_USER}/{ENV_PWD} or {ENV_CATALOG_USER}/{ENV_CATALOG_TOKEN}"
    )


def find_dotenv(start: str | Path | None = None) -> Path | None:
    """The first ``.env`` in ``start`` (default: the working directory) or a parent."""
    here = Path.cwd() if start is None else Path(start)
    for folder in (here, *here.resolve().parents):
        candidate = folder / DOTENV_NAME
        if candidate.is_file():
            return candidate
    return None


def read_dotenv_value(path: Path, key: str) -> str | None:
    """``key``'s value in the ``KEY=VALUE`` file ``path``, or ``None``.

    Blank lines, ``#`` comments and a leading ``export`` are allowed; one pair
    of surrounding quotes is stripped. An empty value counts as unset.
    """
    found: str | None = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.removeprefix("export ").strip()
        name, sep, value = line.partition("=")
        if not sep or name.strip() != key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        found = value or None  # the last assignment wins, as in a shell
    return found


class SdiSession:
    """`get_xml` / `resolve_series` for a notebook — what `%sdi` dispatches onto.

    Reads the SDI catalogue only; pushing to DDS is `MetadataSession`'s job.
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

    @_friendly(SdiSessionError)
    def get_xml(self, uuid: str) -> SdiMetadata:
        """Download the ISO 19115-3 XML of SDI record `uuid` and remember it."""
        self._last = self._sdi().get_xml(uuid)
        return self._last

    @_friendly(SdiSessionError)
    def resolve_series(self, series_uuid: str) -> str:
        """The UUID of the one release of `series_uuid` that is not superseded."""
        return self._sdi().resolve_series(series_uuid)

    # -- internals --------------------------------------------------------

    def _sdi(self) -> SdiController:
        if self._controller is None:
            env = dict(os.environ if self._env is None else self._env)
            self._controller = SdiController(SdiCatalogue.from_env(env))
        return self._controller


class MetadataSession:
    """`push_to_dds` / `dds_base_url` for a notebook — what `%metadata` dispatches onto.

    ``last`` returns the record to push by default — under the magics, the
    `%sdi` session's last `get_xml` result. ``controller`` is built from the
    kernel environment on the first push if not given; ``dotenv_path`` names
    the ``.env`` to read ``DDS_BASE_URL`` from (by default searched for from
    the working directory upwards, once, when the session is built).
    """

    def __init__(
        self,
        *,
        last: Callable[[], SdiMetadata | None] | None = None,
        controller: SdiController | None = None,
        env: Mapping[str, str] | None = None,
        dotenv_path: str | Path | None = None,
    ) -> None:
        self._last = last or (lambda: None)
        self._controller = controller
        self._env = env
        self._dotenv_path = Path(dotenv_path) if dotenv_path is not None else find_dotenv()
        self._dotenv_dds_url = (
            read_dotenv_value(self._dotenv_path, ENV_BASE_URL)
            if self._dotenv_path is not None and self._dotenv_path.is_file()
            else None
        )

    def __repr__(self) -> str:
        return f"MetadataSession(dotenv={str(self._dotenv_path) if self._dotenv_path else None!r})"

    @_friendly(MetadataSessionError)
    def push_to_dds(
        self,
        dds_path: str,
        metadata: SdiMetadata | None = None,
        folder: str = DEFAULT_FOLDER,
        force: bool = False,
    ) -> PushResult:
        """Upload `metadata` (default: the last `%sdi get_xml` result) to
        `{dds_path}/{folder}/{uuid}.xml` in DDS."""
        metadata = metadata or self._last()
        if metadata is None:
            raise MetadataSessionError("nothing to push yet — run %sdi get_xml(uuid) first")
        return self._push_controller().push_to_dds(
            metadata, dds_path, folder=folder, force=force
        )

    @_friendly(MetadataSessionError)
    def dds_base_url(self) -> str:
        """The DDS URL `push_to_dds` uses, and where it came from."""
        url, source = self._resolve_dds_base_url()
        return f"{url}  (from {source})"

    # -- internals --------------------------------------------------------

    def _push_controller(self) -> SdiController:
        if self._controller is None:
            env = dict(os.environ if self._env is None else self._env)
            # push_to_dds never calls SDI, so the catalogue is never used here.
            self._controller = SdiController(
                documents=DocumentsClient(self._resolve_dds_base_url()[0], load_dds_creds(env))
            )
        return self._controller

    def _resolve_dds_base_url(self) -> tuple[str, str]:
        """``(url, source)``: the ``.env`` file's ``DDS_BASE_URL``, else the kernel's."""
        if self._dotenv_dds_url and self._dotenv_path is not None:
            return self._dotenv_dds_url.rstrip("/"), str(self._dotenv_path)
        env = dict(os.environ if self._env is None else self._env)
        try:
            return load_base_url(env), "the kernel environment"
        except MissingCredentialsError:
            where = (
                f"{self._dotenv_path} has none and " if self._dotenv_path else "no .env found and "
            )
            raise MissingCredentialsError(
                f"pushing to DDS needs {ENV_BASE_URL}: {where}it is not set in the kernel "
                "environment"
            ) from None
