from __future__ import annotations

from datetime import UTC, datetime

import pytest

from eea_datalakehouse.sdi import NotIso19115_3, UuidMismatch, iso

from .conftest import SERIES_UUID, UUID, iso_xml


def test_parse_reads_identifier_citation_scope_and_dates() -> None:
    record = iso.parse(iso_xml())

    assert record.uuid == UUID
    assert record.title == "Bathing Water Directive - Status of bathing water, 2025 v.1.0"
    assert record.edition == "01.00"
    assert record.hierarchy_level == "nonGeographicDataset"
    assert record.created == datetime(2026, 6, 1, 7, 54, 39, 676514, tzinfo=UTC)
    assert record.date_stamp == datetime(2026, 9, 25, 7, 36, 46, 809835, tzinfo=UTC)
    assert record.children == ()


def test_parse_collects_series_children() -> None:
    record = iso.parse(iso_xml(SERIES_UUID, scope="series", children=("a", "b")))

    assert record.children == ("a", "b")
    assert record.hierarchy_level == "series"


def test_plain_date_is_taken_as_utc() -> None:
    xml = iso_xml().replace(
        b"<gco:DateTime>2026-09-25T07:36:46.809835Z</gco:DateTime>",
        b"<gco:Date>2026-09-25</gco:Date>",
    )
    assert iso.parse(xml).revised == datetime(2026, 9, 25, tzinfo=UTC)


@pytest.mark.parametrize(
    "xml",
    [
        b'<gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"/>',
        b"<html><body>Service unavailable",
        b"",
    ],
)
def test_parse_rejects_anything_but_iso_19115_3(xml: bytes) -> None:
    with pytest.raises(NotIso19115_3):
        iso.parse(xml)


def test_check_rejects_another_records_xml() -> None:
    with pytest.raises(UuidMismatch, match="identifies 'other'"):
        iso.check(iso_xml("other"), UUID)
