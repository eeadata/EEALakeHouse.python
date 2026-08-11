"""Dremio's own REST Catalog API (v3) — used for folder existence/creation/
deletion and for reading an entity's wiki/tags, none of which have a SQL or
Flight equivalent (this is the one place in `catalog` that isn't
transport-agnostic through SqlExecutor).

Unverified against a real Dremio deployment: the v3 catalog API's by-path
lookup, folder-creation, wiki/tag-collaboration, and folder-deletion
request/response shapes here are inferred from Dremio's documented API, not
confirmed against this project's own instance — check this against a real
call before relying on it. `delete_folder`'s cascade path in particular
assumes a container child's JSON carries ``type="CONTAINER"`` /
``containerType="FOLDER"`` the way Dremio's docs describe it.
"""

from __future__ import annotations

from typing import Any

import httpx

from .errors import CatalogOperationError, EngineStartingError

DEFAULT_TIMEOUT = 60.0


class CatalogRestClient:
    """Existence checks, folder creation, and wiki/tag reads via ``/api/v3/catalog``.

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

    def _lookup_by_path(self, path: str) -> dict[str, Any] | None:
        """The catalog entity's own JSON (id, type, ...) at `path`, or None if absent."""
        encoded = "/".join(path.split("."))
        try:
            resp = self._http.get(
                f"{self._base_url}/api/v3/catalog/by-path/{encoded}", headers=self._headers
            )
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"catalog lookup for {path!r} timed out") from exc
        if resp.status_code == 404:
            return None
        if resp.status_code >= 400:
            raise CatalogOperationError(
                f"could not look up {path!r} ({resp.status_code}): {resp.text[:300]}"
            )
        return resp.json()

    def exists(self, path: str) -> bool:
        """Whether `path` (dot-separated) is any entity in the catalog."""
        return self._lookup_by_path(path) is not None

    def is_folder(self, path: str) -> bool:
        """Whether `path` names an existing folder — not a table/view/space/source.

        Used by `datacopy`/`datamove` to decide whether a `target_path`
        that names a folder should have the source's own name appended to
        it (`cp source dest/` semantics), rather than being taken literally
        as the new table/view name.

        Best-effort: some Dremio source types (its own internal
        Arctic/Nessie-backed catalog sources, seen in practice as
        "Can not get internal item from non-filesystem source ... of type
        [com.dremio.plugins.dremiocatalog.store.DremioCatalogLocalPlugin]")
        reject by-path lookups into nested items outright — a 400, not a
        404 — rather than answering "not found". This convenience can't
        tell that apart from "doesn't exist", so any lookup failure here
        is treated the same as "not a folder", never raised.
        """
        try:
            entity = self._lookup_by_path(path)
        except CatalogOperationError:
            return False
        return entity is not None and entity.get("entityType") == "folder"

    def get_wiki(self, path: str) -> str:
        """The Dremio wiki text attached to the catalog entity at `path`.

        Looks the entity up by path first (to get its id — the wiki endpoint
        is keyed by id, not path), then reads
        ``/api/v3/catalog/{id}/collaboration/wiki``. Raises
        `CatalogOperationError` if `path` doesn't exist, or if it exists but
        has no wiki at all (both surface as 404 from Dremio; the message
        distinguishes which one happened).
        """
        entity = self._lookup_by_path(path)
        if entity is None:
            raise CatalogOperationError(f"{path!r} does not exist")
        entity_id = entity["id"]
        try:
            resp = self._http.get(
                f"{self._base_url}/api/v3/catalog/{entity_id}/collaboration/wiki",
                headers=self._headers,
            )
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"fetching wiki for {path!r} timed out") from exc
        if resp.status_code == 404:
            raise CatalogOperationError(f"{path!r} has no wiki")
        if resp.status_code >= 400:
            raise CatalogOperationError(
                f"could not fetch wiki for {path!r} ({resp.status_code}): {resp.text[:300]}"
            )
        return resp.json().get("text", "")

    def _post_collaboration(self, url: str, body: dict[str, Any], *, what: str) -> httpx.Response:
        try:
            return self._http.post(url, json=body, headers=self._headers)
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"{what} timed out") from exc

    def set_wiki(self, path: str, text: str) -> None:
        """Create or update the Dremio wiki text on the catalog entity at `path`.

        Dremio's collaboration API is optimistic-concurrency-controlled by a
        ``version`` field — but only when a wiki already exists, and
        confirmed against a real call: sending ``version: 0`` when there is
        no existing wiki gets rejected ("Tried to update version 0, found
        no tag"). The catch — also confirmed — is that ``GET .../wiki`` can
        answer ``200`` with default/empty content even when nothing has
        ever been set, so a GET beforehand can't reliably tell "real
        record" from "nothing yet."

        So this tries the simple create (no ``version``) first. Only if
        that's rejected does it fetch whatever version is actually there
        and retry once as an update — covering the real "a wiki already
        exists" case without needing the GET to be trustworthy up front.
        """
        entity = self._lookup_by_path(path)
        if entity is None:
            raise CatalogOperationError(f"{path!r} does not exist")
        entity_id = entity["id"]
        url = f"{self._base_url}/api/v3/catalog/{entity_id}/collaboration/wiki"

        resp = self._post_collaboration(url, {"text": text}, what=f"setting wiki for {path!r}")
        if resp.status_code < 400:
            return

        try:
            current = self._http.get(url, headers=self._headers)
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"reading current wiki for {path!r} timed out") from exc
        version = current.json().get("version", 0) if current.status_code == 200 else 0

        retry = self._post_collaboration(
            url, {"text": text, "version": version}, what=f"setting wiki for {path!r}"
        )
        if retry.status_code >= 400:
            raise CatalogOperationError(
                f"could not set wiki for {path!r} ({retry.status_code}): {retry.text[:300]}"
            )

    def get_tags(self, path: str) -> list[str]:
        """The Dremio tags attached to the catalog entity at `path`.

        Unlike `get_wiki`, having zero tags is a normal state, not an error
        — this only raises `CatalogOperationError` if `path` itself doesn't
        exist. A 404 from the tag endpoint (no tag record at all yet) is
        treated the same as an empty tag list.
        """
        entity = self._lookup_by_path(path)
        if entity is None:
            raise CatalogOperationError(f"{path!r} does not exist")
        entity_id = entity["id"]
        try:
            resp = self._http.get(
                f"{self._base_url}/api/v3/catalog/{entity_id}/collaboration/tag",
                headers=self._headers,
            )
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"fetching tags for {path!r} timed out") from exc
        if resp.status_code == 404:
            return []
        if resp.status_code >= 400:
            raise CatalogOperationError(
                f"could not fetch tags for {path!r} ({resp.status_code}): {resp.text[:300]}"
            )
        return resp.json().get("tags", [])

    def set_tags(self, path: str, tags: list[str]) -> None:
        """Replace the Dremio tags on the catalog entity at `path` with `tags`.

        Same confirmed create-then-fallback-to-update shape as `set_wiki`
        (see its docstring for why a GET beforehand isn't trustworthy
        enough to decide up front). This *replaces* the tag set, it doesn't
        add to it — pass the union of old and new tags if you want to keep
        the existing ones (e.g. via `get_tags` first).
        """
        entity = self._lookup_by_path(path)
        if entity is None:
            raise CatalogOperationError(f"{path!r} does not exist")
        entity_id = entity["id"]
        url = f"{self._base_url}/api/v3/catalog/{entity_id}/collaboration/tag"

        resp = self._post_collaboration(url, {"tags": tags}, what=f"setting tags for {path!r}")
        if resp.status_code < 400:
            return

        try:
            current = self._http.get(url, headers=self._headers)
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"reading current tags for {path!r} timed out") from exc
        version = current.json().get("version", 0) if current.status_code == 200 else 0

        retry = self._post_collaboration(
            url, {"tags": tags, "version": version}, what=f"setting tags for {path!r}"
        )
        if retry.status_code >= 400:
            raise CatalogOperationError(
                f"could not set tags for {path!r} ({retry.status_code}): {retry.text[:300]}"
            )

    def create_folder(self, path: str) -> bool:
        """POST `path` into existence. Returns whether it was newly created
        (False on 409 — it was already there)."""
        try:
            resp = self._http.post(
                f"{self._base_url}/api/v3/catalog",
                json={"entityType": "folder", "path": path.split(".")},
                headers=self._headers,
            )
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"creating folder {path!r} timed out") from exc
        # 409: it was already there (either from before this call, or a
        # concurrent creation) — the post-condition ("it exists now") still
        # holds either way.
        if resp.status_code not in (200, 201, 409):
            raise CatalogOperationError(
                f"could not create folder {path!r} ({resp.status_code}): {resp.text[:300]}"
            )
        return resp.status_code != 409

    def ensure_folder_path(self, path: str) -> list[str]:
        """Create every missing folder level of `path`. Returns the levels
        actually created (not the ones that were already there).

        Attempts creation directly for every nested level rather than
        checking existence first — some Dremio source types (its own
        internal Arctic/Nessie-backed catalog sources) reject by-path
        *lookups* into nested items outright (400, not 404) even though
        folder *creation* still works there (see `is_folder`'s docstring
        for the exact error). Create-and-tolerate-409-already-there
        sidesteps needing a working existence check at all for these
        levels — it costs one extra POST per level that already existed,
        which is the trade worth making since the alternative is failing
        outright against a source where the check itself is broken.

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
            if self.create_folder(current):
                created.append(current)
            walked = current
        return created

    @staticmethod
    def _child_is_folder(child: dict[str, Any]) -> bool:
        # Unverified against a real deployment (same caveat as the module
        # docstring): a container child is assumed to carry
        # containerType="FOLDER" the way Dremio's documented catalog API
        # describes it.
        return (
            str(child.get("type", "")).upper() == "CONTAINER"
            and str(child.get("containerType", "")).upper() == "FOLDER"
        )

    def _delete_entity(self, entity_id: str, *, what: str) -> None:
        try:
            resp = self._http.delete(
                f"{self._base_url}/api/v3/catalog/{entity_id}", headers=self._headers
            )
        except httpx.TimeoutException as exc:
            raise EngineStartingError(f"deleting {what} timed out") from exc
        # 404: already gone (a concurrent delete, or a retry after a prior
        # attempt whose response we never saw) — the post-condition ("it's
        # gone") still holds either way.
        if resp.status_code not in (200, 204, 404):
            raise CatalogOperationError(
                f"could not delete {what} ({resp.status_code}): {resp.text[:300]}"
            )

    def delete_folder(self, path: str, *, cascade: bool = False) -> None:
        """Delete the folder at `path`. Idempotent — a folder that's
        already gone is not an error, same as `deleteview`'s
        ``DROP VIEW IF EXISTS``.

        `cascade=False` (the default) raises `CatalogOperationError` if the
        folder still has contents — `rmdir` vs `rm -r`. `cascade=True`
        deletes every table/view and subfolder inside first, depth-first
        (each straight through this same catalog API — Dremio supports
        deleting a dataset by id here too, not just via SQL DROP), then the
        folder itself.
        """
        entity = self._lookup_by_path(path)
        if entity is None:
            return
        if entity.get("entityType") != "folder":
            raise CatalogOperationError(f"{path!r} is not a folder")

        children = entity.get("children") or []
        if children and not cascade:
            raise CatalogOperationError(
                f"folder {path!r} is not empty — pass cascade=True to delete its "
                "contents and subfolders too"
            )
        if cascade:
            for child in children:
                child_path = ".".join(child.get("path") or [])
                if not child_path:
                    continue
                if self._child_is_folder(child):
                    self.delete_folder(child_path, cascade=True)
                else:
                    self._delete_entity(child["id"], what=f"{child_path!r}")

        self._delete_entity(entity["id"], what=f"folder {path!r}")
