"""Read the few ISO 19115-3 fields the SDI controller checks.

Uses :mod:`xml.etree.ElementTree` with explicit namespaces. Python's expat
parser does not fetch external entities, so an SDI response cannot make it
read local files or the network.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import UTC, date, datetime

from .errors import NotIso19115_3, UuidMismatch

NS = {
    "mdb": "http://standards.iso.org/iso/19115/-3/mdb/2.0",
    "mcc": "http://standards.iso.org/iso/19115/-3/mcc/1.0",
    "mri": "http://standards.iso.org/iso/19115/-3/mri/1.0",
    "cit": "http://standards.iso.org/iso/19115/-3/cit/2.0",
    "gco": "http://standards.iso.org/iso/19115/-3/gco/1.0",
    "gcx": "http://standards.iso.org/iso/19115/-3/gcx/1.0",
}
ROOT_TAG = f"{{{NS['mdb']}}}MD_Metadata"

_IDENTIFIER = "mdb:metadataIdentifier/mcc:MD_Identifier/mcc:code/gco:CharacterString"
_SCOPE = "mdb:metadataScope/mdb:MD_MetadataScope/mdb:resourceScope/mcc:MD_ScopeCode"
_CITATION = "mdb:identificationInfo/*/mri:citation/cit:CI_Citation"
_DATE_INFO = "mdb:dateInfo/cit:CI_Date"
_ASSOCIATED = "mdb:identificationInfo/*/mri:associatedResource/mri:MD_AssociatedResource"


@dataclass(frozen=True)
class IsoRecord:
    """What the controller needs to know about one ISO 19115-3 record."""

    uuid: str
    title: str | None
    edition: str | None
    hierarchy_level: str | None
    created: datetime | None
    revised: datetime | None
    children: tuple[str, ...] = ()

    @property
    def date_stamp(self) -> datetime | None:
        """The metadata's last change: its revision date, else its creation date."""
        return self.revised or self.created


def parse(xml: bytes) -> IsoRecord:
    """Parse ``xml``; raise :class:`NotIso19115_3` unless it is ``mdb:MD_Metadata``."""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        raise NotIso19115_3(f"not well-formed XML: {exc}") from exc
    if root.tag != ROOT_TAG:
        raise NotIso19115_3(f"expected mdb:MD_Metadata (ISO 19115-3), got {root.tag!r}")

    citation = root.find(_CITATION, NS)
    scope = root.find(_SCOPE, NS)
    dates: dict[str, datetime] = {}
    for ci_date in root.findall(_DATE_INFO, NS):
        kind = ci_date.find("cit:dateType/cit:CI_DateTypeCode", NS)
        when = _parse_date(
            _text(ci_date, "cit:date/gco:DateTime") or _text(ci_date, "cit:date/gco:Date")
        )
        if kind is not None and when is not None:
            key = kind.get("codeListValue", "")
            dates[key] = max(when, dates[key]) if key in dates else when

    children = tuple(
        ref
        for assoc in root.findall(_ASSOCIATED, NS)
        if _code(assoc, "mri:associationType/mri:DS_AssociationTypeCode") == "isComposedOf"
        and (ref := _attr(assoc, "mri:metadataReference", "uuidref"))
    )

    return IsoRecord(
        uuid=_text(root, _IDENTIFIER) or "",
        title=_text(citation, "cit:title") if citation is not None else None,
        edition=_text(citation, "cit:edition") if citation is not None else None,
        hierarchy_level=scope.get("codeListValue") if scope is not None else None,
        created=dates.get("creation"),
        revised=dates.get("revision"),
        children=children,
    )


def check(xml: bytes, uuid: str) -> IsoRecord:
    """Parse ``xml`` and raise :class:`UuidMismatch` unless it identifies ``uuid``."""
    record = parse(xml)
    if record.uuid != uuid:
        raise UuidMismatch(f"expected SDI record {uuid!r} but the XML identifies {record.uuid!r}")
    return record


def _text(node: ET.Element, path: str) -> str | None:
    """Text of a ``gco:CharacterString`` / ``gcx:Anchor`` (or a plain element)."""
    el = node.find(path, NS)
    if el is None:
        return None
    if el.text is None or not el.text.strip():
        inner = el.find("gco:CharacterString", NS)
        if inner is None:
            inner = el.find("gcx:Anchor", NS)
        el = inner if inner is not None else el
    text = (el.text or "").strip()
    return text or None


def _code(node: ET.Element, path: str) -> str | None:
    el = node.find(path, NS)
    return el.get("codeListValue") if el is not None else None


def _attr(node: ET.Element, path: str, name: str) -> str | None:
    el = node.find(path, NS)
    return el.get(name) if el is not None else None


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        try:
            parsed = datetime.combine(date.fromisoformat(value), datetime.min.time())
        except ValueError:
            return None
    # Dates without a zone are taken as UTC so every value compares.
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
