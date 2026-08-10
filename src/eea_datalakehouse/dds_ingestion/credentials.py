"""Credential resolution for the ingest client.

Dremio credentials are read from the kernel environment variables injected by
the JupyterLab extension (``_DREMIO_USER`` / ``_DREMIO_PWD``). The DDS base URL
is read from ``DDS_BASE_URL``.

The :class:`DremioCreds` object deliberately hides its secret from ``repr``,
``str`` and logging so credentials never leak into notebook output or logs.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

ENV_USER = "_DREMIO_USER"
ENV_PWD = "_DREMIO_PWD"  # noqa: S105 — env var name, not a secret value
ENV_BASE_URL = "DDS_BASE_URL"


class MissingCredentialsError(RuntimeError):
    """Raised when the required credentials or base URL are absent."""


@dataclass(frozen=True)
class DremioCreds:
    """Dremio username + PAT.

    ``_password`` is the Dremio PAT (the kernel's ``_DREMIO_PWD``); it is sent as
    the ``Authorization: Bearer`` token. It is stored but never rendered:
    ``repr``/``str`` redact it so it cannot reach notebook output, tracebacks or
    log handlers.
    """

    username: str
    _password: str

    @property
    def password(self) -> str:
        return self._password

    def __repr__(self) -> str:
        return f"DremioCreds(username={self.username!r}, password=***)"

    __str__ = __repr__


def load_creds(env: dict[str, str] | None = None) -> DremioCreds:
    """Load Dremio credentials from the kernel environment.

    Parameters
    ----------
    env:
        Mapping to read from; defaults to :data:`os.environ`. Passing an
        explicit mapping keeps this testable without mutating process state.
    """
    source = os.environ if env is None else env
    user = source.get(ENV_USER)
    pwd = source.get(ENV_PWD)
    if not user or not pwd:
        raise MissingCredentialsError(
            f"missing Dremio credentials in environment "
            f"({ENV_USER}/{ENV_PWD} must both be set 11"
        )
    return DremioCreds(username=user, _password=pwd)


def load_base_url(env: dict[str, str] | None = None) -> str:
    """Load the DDS base URL from the environment."""

    source = os.environ if env is None else env
    base = source.get(ENV_BASE_URL)
    if not base:
        raise MissingCredentialsError(f"missing DDS base URL ({ENV_BASE_URL} must be set)")
    return base.rstrip("/")
