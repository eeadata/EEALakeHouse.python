"""Dremio's own REST Catalog API (v3) — used only for folder existence and
creation, since neither has a SQL or Flight equivalent (this is the one
place in `catalog` that isn't transport-agnostic through SqlExecutor).

Unverified against a real Dremio deployment: the v3 catalog API's by-path
lookup and folder-creation request/response shapes here are inferred from
Dremio's documented API, not confirmed against this project's own instance —
check this against a real call before relying on it.
"""

from __future__ import annotations

import httpx

from .errors import CatalogOperationError, EngineStartingError

DEFAULT_TIMEOUT = 60.0


class CatalogRestClient:
    """Existence checks and folder creation via ``/api/v3/catalog``.

    Uses the same Dremio base_url/token as the SQL executors — Dremio's
    catalog API and its SQL Jobs API are both under the one REST root, just
    different sub-paths.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {token}"}
        self._owns_client = http_client is None
        self._http = http_client or httpx.Client(timeout=timeout)

    def __repr__(self) -> str:
        # No Authorization header, same reasoning as IngestClient/RestSqlExecutor.
        return f"CatalogRestClient(base_url={self._base_url!r})"

    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def exists(self, path: str) -> bool:
        """Whether `path` (dot-separated) is any entity in the catalog."""
        encoded = "/".join(path.split("."))
        try:
            resp = self._http.get(
                f"{self._base_url}/api/v3/catalog/by-path/{encoded}", headers=self._headers
            )
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"catalog lookup for {path!r} timed out") from exc
        if resp.status_code == 404:
            return False
        if resp.status_code >= 400:
            raise CatalogOperationError(
                f"could not look up {path!r} ({resp.status_code}): {resp.text[:300]}"
            )
        return True

    def _create_folder(self, path: str) -> None:
        try:
            resp = self._http.post(
                f"{self._base_url}/api/v3/catalog",
                json={"entityType": "folder", "path": path.split(".")},
                headers=self._headers,
            )
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"creating folder {path!r} timed out") from exc
        # 409: someone else created it first between our exists() check and
        # this call — the post-condition ("it exists now") still holds.
        if resp.status_code not in (200, 201, 409):
            raise CatalogOperationError(
                f"could not create folder {path!r} ({resp.status_code}): {resp.text[:300]}"
            )

    def ensure_folder_path(self, path: str) -> list[str]:
        """Create every missing folder level of `path`. Returns the levels created.

        The first (leftmost) segment — the Dremio space/source itself — is
        required to already exist and is never created here; only the
        folder levels under it are, mirroring dds_ingestion's own
        "taxonomy levels are never invented" convention for the ingest path.
        """
        segments = [s for s in path.split(".") if s]
        if not segments:
            return []

        walked = segments[0]
        if not self.exists(walked):
            raise CatalogOperationError(
                f"{walked!r} does not exist — the space/source itself is never "
                "created automatically, only folders under it"
            )

        created: list[str] = []
        for name in segments[1:]:
            current = f"{walked}.{name}"
            if not self.exists(current):
                self._create_folder(current)
                created.append(current)
            walked = current
        return created
