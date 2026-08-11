from __future__ import annotations

from typing import Any

import pytest

from eea_datalakehouse.catalog.errors import EngineStartingError
from eea_datalakehouse.catalog.sql import SqlResult


class FakeExecutor:
    """In-memory stand-in for SqlExecutor that records every statement.

    `fail_on` maps a 0-based call index (shared between execute() and
    fetch_all() calls, in the order they happen) to an exception to raise
    instead of succeeding — lets a test simulate "the 2nd statement stalls"
    without any real HTTP/Flight machinery.

    `rows` is what fetch_all() returns on every non-failing call — fine when
    an operation only ever calls fetch_all() once. `rows_sequence`, if given,
    instead pops one entry per fetch_all() call in order (falling back to
    `rows` once exhausted) — needed for gettableitemsfrom, which fetches a
    columns query and then an items query and expects different rows back
    from each.
    """

    def __init__(
        self,
        fail_on: dict[int, Exception] | None = None,
        rows: list[dict[str, Any]] | None = None,
        rows_sequence: list[list[dict[str, Any]]] | None = None,
    ) -> None:
        self.statements: list[str] = []
        self._fail_on = fail_on or {}
        self._rows = rows if rows is not None else []
        self._rows_sequence = list(rows_sequence) if rows_sequence is not None else None

    def execute(self, sql: str, *, idempotency_key: str | None = None) -> SqlResult:
        index = len(self.statements)
        self.statements.append(sql)
        if index in self._fail_on:
            raise self._fail_on[index]
        return SqlResult(row_count=1, job_id=f"job-{index}")

    def fetch_all(
        self, sql: str, *, idempotency_key: str | None = None
    ) -> list[dict[str, Any]]:
        index = len(self.statements)
        self.statements.append(sql)
        if index in self._fail_on:
            raise self._fail_on[index]
        if self._rows_sequence:
            return self._rows_sequence.pop(0)
        return self._rows


@pytest.fixture
def executor() -> FakeExecutor:
    return FakeExecutor()


@pytest.fixture
def stalling_executor() -> FakeExecutor:
    return FakeExecutor(fail_on={0: EngineStartingError("engine starting", idempotency_key="k")})


class FakeCatalogRest:
    """In-memory stand-in for CatalogRestClient.

    `existing` seeds which dot-paths already "exist" — include the
    space/source segment (e.g. "a") if a test's ensure_folder_path call
    should succeed rather than raise on a missing space. `folders` seeds
    which of those paths are specifically folders (for `is_folder`) —
    independent of `existing`, since a real entity could be a table/view
    instead.
    """

    def __init__(
        self,
        existing: set[str] | None = None,
        folders: set[str] | None = None,
        wikis: dict[str, str] | None = None,
        tags: dict[str, list[str]] | None = None,
        raise_on_get_wiki: Exception | None = None,
        raise_on_get_tags: Exception | None = None,
        raise_on_set_wiki: Exception | None = None,
        raise_on_set_tags: Exception | None = None,
    ) -> None:
        self.existing = set(existing or set())
        self.folders = set(folders or set())
        self.created: list[str] = []
        self._wikis = dict(wikis or {})
        self._tags = dict(tags or {})
        self._raise_on_get_wiki = raise_on_get_wiki
        self._raise_on_get_tags = raise_on_get_tags
        self._raise_on_set_wiki = raise_on_set_wiki
        self._raise_on_set_tags = raise_on_set_tags

    def exists(self, path: str) -> bool:
        return path in self.existing

    def is_folder(self, path: str) -> bool:
        return path in self.folders

    def get_wiki(self, path: str) -> str:
        from eea_datalakehouse.catalog.errors import CatalogOperationError

        if self._raise_on_get_wiki is not None:
            raise self._raise_on_get_wiki
        if path not in self.existing:
            raise CatalogOperationError(f"{path!r} does not exist")
        if path not in self._wikis:
            raise CatalogOperationError(f"{path!r} has no wiki")
        return self._wikis[path]

    def get_tags(self, path: str) -> list[str]:
        from eea_datalakehouse.catalog.errors import CatalogOperationError

        if self._raise_on_get_tags is not None:
            raise self._raise_on_get_tags
        if path not in self.existing:
            raise CatalogOperationError(f"{path!r} does not exist")
        return self._tags.get(path, [])

    def set_wiki(self, path: str, text: str) -> None:
        from eea_datalakehouse.catalog.errors import CatalogOperationError

        if self._raise_on_set_wiki is not None:
            raise self._raise_on_set_wiki
        if path not in self.existing:
            raise CatalogOperationError(f"{path!r} does not exist")
        self._wikis[path] = text

    def set_tags(self, path: str, tags: list[str]) -> None:
        from eea_datalakehouse.catalog.errors import CatalogOperationError

        if self._raise_on_set_tags is not None:
            raise self._raise_on_set_tags
        if path not in self.existing:
            raise CatalogOperationError(f"{path!r} does not exist")
        self._tags[path] = list(tags)

    def ensure_folder_path(self, path: str) -> list[str]:
        from eea_datalakehouse.catalog.errors import CatalogOperationError

        segments = [s for s in path.split(".") if s]
        if not segments:
            return []
        walked = segments[0]
        if walked not in self.existing:
            raise CatalogOperationError(f"{walked!r} does not exist")
        created: list[str] = []
        for name in segments[1:]:
            current = f"{walked}.{name}"
            if current not in self.existing:
                self.existing.add(current)
                self.folders.add(current)
                self.created.append(current)
                created.append(current)
            walked = current
        return created


@pytest.fixture
def catalog_rest() -> FakeCatalogRest:
    return FakeCatalogRest(existing={"a", "bwd"})
