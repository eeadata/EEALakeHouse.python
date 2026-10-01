"""SDI connection settings, read from the environment.

``SDI_API_URL`` is the catalogue's base URL (the client appends
``/srv/api/...``); empty or unset means the public EEA catalogue.
``SDI_USERNAME`` / ``SDI_PASSWORD`` are optional and only needed to read
records that are not public. The password is never rendered by ``repr``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import httpx

ENV_API_URL = "SDI_API_URL"
ENV_USERNAME = "SDI_USERNAME"
ENV_PASSWORD = "SDI_PASSWORD"  # noqa: S105 — env var name, not a secret value
DEFAULT_SDI_API_URL = "https://sdi.eea.europa.eu/catalogue"


@dataclass(frozen=True)
class SdiConfig:
    api_url: str = DEFAULT_SDI_API_URL
    username: str | None = None
    _password: str | None = None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> SdiConfig:
        source = os.environ if env is None else env
        url = (source.get(ENV_API_URL) or DEFAULT_SDI_API_URL).rstrip("/")
        return cls(
            api_url=url,
            username=source.get(ENV_USERNAME) or None,
            _password=source.get(ENV_PASSWORD) or None,
        )

    @property
    def auth(self) -> httpx.BasicAuth | None:
        if self.username and self._password:
            return httpx.BasicAuth(self.username, self._password)
        return None

    def __repr__(self) -> str:
        return (
            f"SdiConfig(api_url={self.api_url!r}, username={self.username!r}, "
            f"password={'***' if self._password else None})"
        )

    __str__ = __repr__
