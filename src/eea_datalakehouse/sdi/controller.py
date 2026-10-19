"""SDI metadata controller: read ISO 19115-3 records from the SDI catalogue.

=================================  ================================================
:meth:`SdiController.get_xml`      download a record's ISO 19115-3 XML from SDI
:meth:`SdiController.resolve_series`  a series UUID's current release UUID
=================================  ================================================

Pushing a record to DDS is :class:`~eea_datalakehouse.dds_documents.DdsController`'s
job: an :class:`SdiMetadata` has the ``filename`` / ``content`` it takes, so
``dds.push(dds_path, DdsFolder.METADATA, metadata)`` stores it as
``{dds_path}/metadata/{uuid}.xml``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from types import TracebackType

from . import iso
from .catalogue import SdiCatalogue
from .errors import NotCurrentError, NotIso19115_3
from .iso import IsoRecord

XML_CONTENT_TYPE = "application/xml"


@dataclass(frozen=True)
class SdiMetadata:
    """One ISO 19115-3 record as SDI served it.

    ``xml`` holds the exact bytes, so what lands in DDS is byte-for-byte what
    SDI returned. ``record`` is the parsed summary (title, edition, dates).
    """

    uuid: str
    xml: bytes = field(repr=False)
    record: IsoRecord = field(repr=False)

    def __repr__(self) -> str:
        return (
            f"SdiMetadata(uuid={self.uuid!r}, title={self.record.title!r}, "
            f"edition={self.record.edition!r}, xml=<{len(self.xml)} bytes>)"
        )

    @property
    def filename(self) -> str:
        """``{uuid}.xml`` — the name the record gets when pushed to DDS."""
        return f"{self.uuid}.xml"

    @property
    def content(self) -> bytes:
        """The XML bytes, for :class:`~eea_datalakehouse.dds_documents.DdsController`."""
        return self.xml

    @property
    def content_type(self) -> str:
        return XML_CONTENT_TYPE

    def provenance_tags(self) -> list[dict[str, str]]:
        """Tags in :meth:`Catalog.setmeta2wiki`'s shape, recording which SDI record this is.

        For people reading the wiki only; nothing reads them back to pick a
        record.
        """
        stamp = self.record.date_stamp
        values = {
            "sdi_record_uuid": ("SDI record UUID", self.uuid),
            "sdi_edition": ("SDI edition", self.record.edition or ""),
            "sdi_date_stamp": ("SDI metadata date", stamp.isoformat() if stamp else ""),
        }
        return [
            {"tag_name": name, "tag_value": value, "tag_title": title}
            for name, (title, value) in values.items()
        ]


class SdiController:
    """Reads ISO 19115-3 metadata records from the SDI catalogue."""

    def __init__(self, catalogue: SdiCatalogue | None = None) -> None:
        self._catalogue = catalogue or SdiCatalogue()

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> SdiController:
        """SDI settings from ``SDI_API_URL`` (+ optional ``SDI_USERNAME`` / ``SDI_PASSWORD``)."""
        return cls(SdiCatalogue.from_env(env))

    def __repr__(self) -> str:
        return f"SdiController(catalogue={self._catalogue!r})"

    @property
    def catalogue(self) -> SdiCatalogue:
        """The SDI client this controller reads from."""
        return self._catalogue

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        self._catalogue.close()

    def __enter__(self) -> SdiController:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- public API -------------------------------------------------------

    def get_xml(self, uuid: str) -> SdiMetadata:
        """Download the ISO 19115-3 XML of the SDI record ``uuid``.

        Raises :class:`SdiNotFound` if SDI has no such record,
        :class:`SdiAuthError` if it is not public, :class:`SdiApiError` on any
        other failure, :class:`NotIso19115_3` if the response is not an
        ``mdb:MD_Metadata`` document and :class:`UuidMismatch` if its
        identifier is not ``uuid``.
        """
        uuid = _require_uuid(uuid)
        xml = self._catalogue.get_xml(uuid)
        try:
            record = iso.check(xml, uuid)
        except NotIso19115_3 as exc:
            raise NotIso19115_3(f"SDI record {uuid!r}: {exc}") from exc
        return SdiMetadata(uuid=uuid, xml=xml, record=record)

    def resolve_series(self, series_uuid: str) -> str:
        """The UUID of the one release of ``series_uuid`` that is not superseded.

        Raises :class:`NotCurrentError`, listing every release, if there are
        zero or several, rather than guessing by date.
        """
        series_uuid = _require_uuid(series_uuid)
        series = self.get_xml(series_uuid)
        candidates = self._catalogue.search_by_uuid(list(series.record.children))
        current = [c for c in candidates if c.status != "superseded"]
        if len(current) != 1:
            raise NotCurrentError(series_uuid, candidates)
        return current[0].uuid


def _require_uuid(uuid: str) -> str:
    if not isinstance(uuid, str):
        raise TypeError(f"uuid must be a str, not {type(uuid).__name__}")
    uuid = uuid.strip()
    if not uuid:
        raise ValueError("uuid must not be empty")
    if "/" in uuid:
        raise ValueError(f"invalid SDI UUID {uuid!r}")
    return uuid
