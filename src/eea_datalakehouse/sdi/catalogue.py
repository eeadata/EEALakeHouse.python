"""Read-only client for the SDI (GeoNetwork 4) REST API.

======================================  =====================================================
:meth:`SdiCatalogue.get_xml`            ``GET  {SDI_API_URL}/srv/api/records/{uuid}/formatters/xml``
:meth:`~SdiCatalogue.search_by_uuid`    ``POST {SDI_API_URL}/srv/api/search/records/_search``
:meth:`~SdiCatalogue.site`              ``GET  {SDI_API_URL}/srv/api/site``
======================================  =====================================================
"""

from __future__ import annotations

from types import TracebackType
from typing import Any
from urllib.parse import quote

import httpx

from .config import SdiConfig
from .errors import SdiApiError, SdiAuthError, SdiNotFound, SeriesCandidate

DEFAULT_TIMEOUT = 60.0
_MAX_ERROR_CHARS = 500
# Fields asked of the search index. Its *DateRange fields are shifted to UTC
# (31 December reads …-12-30T23:00Z), so only plain-date fields are used.
_SEARCH_FIELDS = [
    "uuid",
    "resourceTitleObject",
    "resourceEdition",
    "publicationDateForResource",
    "cl_status",
]


class SdiCatalogue:
    """Read-only access to one GeoNetwork catalogue."""

    def __init__(
        self,
        config: SdiConfig | None = None,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._config = config or SdiConfig()
        self._api = f"{self._config.api_url.rstrip('/')}/srv/api"
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(timeout=timeout, follow_redirects=True)

    @classmethod
    def from_env(
        cls, env: dict[str, str] | None = None, *, timeout: float = DEFAULT_TIMEOUT
    ) -> SdiCatalogue:
        return cls(SdiConfig.from_env(env), timeout=timeout)

    def __repr__(self) -> str:
        return f"SdiCatalogue(api_url={self._config.api_url!r})"

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> SdiCatalogue:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- API endpoints ----------------------------------------------------

    def get_xml(self, uuid: str) -> bytes:
        """The record's ISO XML, exactly as SDI serves it."""
        path = f"/records/{quote(uuid, safe='')}/formatters/xml"
        resp = self._http.get(
            f"{self._api}{path}",
            headers={"Accept": "application/xml"},
            **self._auth(),
        )
        self._raise_for_status(resp, f"GET /srv/api{path}", uuid=uuid)
        return resp.content

    def search_by_uuid(self, uuids: list[str]) -> list[SeriesCandidate]:
        """Title, edition, publication date and status of each record in ``uuids``."""
        if not uuids:
            return []
        body = {
            "query": {"terms": {"uuid": uuids}},
            "size": len(uuids),
            "_source": _SEARCH_FIELDS,
        }
        resp = self._http.post(
            f"{self._api}/search/records/_search",
            json=body,
            headers={"Accept": "application/json"},
            **self._auth(),
        )
        self._raise_for_status(resp, "POST /srv/api/search/records/_search")
        hits = resp.json().get("hits", {}).get("hits", [])
        return [_candidate(hit.get("_source", {})) for hit in hits]

    def site(self) -> dict[str, Any]:
        """The catalogue's site description (name, version)."""
        resp = self._http.get(
            f"{self._api}/site", headers={"Accept": "application/json"}, **self._auth()
        )
        self._raise_for_status(resp, "GET /srv/api/site")
        result: dict[str, Any] = resp.json()
        return result

    # -- internals --------------------------------------------------------

    def _auth(self) -> dict[str, Any]:
        """``auth=`` for a request: basic auth when configured, else none."""
        auth = self._config.auth
        return {"auth": auth} if auth is not None else {}

    @staticmethod
    def _raise_for_status(resp: httpx.Response, where: str, *, uuid: str | None = None) -> None:
        if resp.is_success:
            return
        if resp.status_code == 404 and uuid is not None:
            raise SdiNotFound(404, f"no SDI record with UUID {uuid!r}", where=where)
        message = resp.text.strip()
        if len(message) > _MAX_ERROR_CHARS:
            message = message[:_MAX_ERROR_CHARS] + "… (truncated)"
        message = message or f"(empty {resp.status_code} response body)"
        if resp.status_code in (401, 403):
            raise SdiAuthError(
                resp.status_code,
                f"{message} (record not public? set SDI_USERNAME / SDI_PASSWORD)",
                where=where,
            )
        raise SdiApiError(resp.status_code, message, where=where)


def _candidate(source: dict[str, Any]) -> SeriesCandidate:
    title = source.get("resourceTitleObject")
    published = source.get("publicationDateForResource")
    statuses = source.get("cl_status") or []
    if isinstance(statuses, dict):
        statuses = [statuses]
    return SeriesCandidate(
        uuid=str(source.get("uuid", "")),
        title=title.get("default") if isinstance(title, dict) else title,
        edition=source.get("resourceEdition"),
        publication_date=str(published[0]) if isinstance(published, list) and published
        else (str(published) if published else None),
        status=next((s.get("key") for s in statuses if isinstance(s, dict)), None),
    )
