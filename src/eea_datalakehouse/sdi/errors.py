"""Errors raised by the SDI controller."""

from __future__ import annotations

from dataclasses import dataclass


class SdiError(RuntimeError):
    """Base class for SDI controller errors."""


class SdiApiError(SdiError):
    """The SDI REST API returned a non-success status."""

    def __init__(self, status_code: int, message: str, *, where: str) -> None:
        super().__init__(f"SDI API error {status_code} on {where}: {message}")
        self.status_code = status_code
        self.message = message
        self.where = where


class SdiNotFound(SdiApiError):
    """No SDI record has that UUID (404)."""


class SdiAuthError(SdiApiError):
    """SDI refused the request (401/403): the record is not public.

    Set ``SDI_USERNAME`` / ``SDI_PASSWORD`` for an account that may read it.
    """


class NotIso19115_3(SdiError):  # noqa: N801 — the standard's name
    """The document is not an ISO 19115-3 ``mdb:MD_Metadata`` record."""


class UuidMismatch(SdiError):
    """The XML's ``metadataIdentifier`` is not the UUID that was asked for."""


class CatalogPathNotFound(SdiError):
    """The dataset path to push metadata under does not exist in the Dremio catalog."""


class DdsCopyConflict(SdiError):
    """The copy already in DDS differs and is not older than the SDI record.

    Someone probably edited it in DDS. Extract never silently overwrites that;
    pass ``force=True`` to replace it anyway.
    """


@dataclass(frozen=True)
class SeriesCandidate:
    """One release of a series, as SDI's search index describes it."""

    uuid: str
    title: str | None
    edition: str | None
    publication_date: str | None
    status: str | None


class NotCurrentError(SdiError):
    """A series has zero or several releases that are not ``superseded``.

    ``candidates`` lists every release so the caller can pick a UUID.
    """

    def __init__(self, series_uuid: str, candidates: list[SeriesCandidate]) -> None:
        current = [c for c in candidates if c.status != "superseded"]
        lines = "\n".join(
            f"  {c.uuid}  {c.status or 'current'}  ed. {c.edition or '-'}  "
            f"{c.publication_date or '-'}  {c.title or ''}"
            for c in candidates
        )
        super().__init__(
            f"series {series_uuid!r} has {len(current)} current release(s), expected 1; "
            f"pass the release UUID explicitly:\n{lines}"
        )
        self.series_uuid = series_uuid
        self.candidates = candidates
