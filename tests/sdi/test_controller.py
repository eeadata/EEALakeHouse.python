from __future__ import annotations

import httpx
import pytest
import respx

from eea_datalakehouse.sdi import (
    NotCurrentError,
    NotIso19115_3,
    SdiController,
    SdiMetadata,
    SdiNotFound,
    UuidMismatch,
)

from .conftest import API, SERIES_UUID, UUID, iso_xml, record_url, search_hit


def extracted(sdi: SdiController, xml: bytes | None = None) -> SdiMetadata:
    with respx.mock:
        respx.get(record_url()).mock(return_value=httpx.Response(200, content=xml or iso_xml()))
        return sdi.get_xml(UUID)


# -- get_xml ------------------------------------------------------------


def test_extract_returns_sdi_bytes_and_summary(sdi: SdiController) -> None:
    metadata = extracted(sdi)

    assert metadata.uuid == UUID
    assert metadata.xml == iso_xml()
    assert metadata.record.edition == "01.00"
    assert "bytes" in repr(metadata)


@respx.mock
def test_extract_404_is_not_found(sdi: SdiController) -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(404))

    with pytest.raises(SdiNotFound):
        sdi.get_xml(UUID)


@respx.mock
def test_extract_rejects_non_iso_19115_3(sdi: SdiController) -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, text="<html><body>"))

    with pytest.raises(NotIso19115_3, match=UUID):
        sdi.get_xml(UUID)


@respx.mock
def test_extract_rejects_another_records_xml(sdi: SdiController) -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml("other")))

    with pytest.raises(UuidMismatch):
        sdi.get_xml(UUID)


@pytest.mark.parametrize("bad", ["", "  ", "a/b"])
def test_extract_rejects_bad_uuid(sdi: SdiController, bad: str) -> None:
    with pytest.raises(ValueError):
        sdi.get_xml(bad)


def test_provenance_tags_match_setmeta2wiki_shape(sdi: SdiController) -> None:
    tags = {t["tag_name"]: t["tag_value"] for t in extracted(sdi).provenance_tags()}

    assert tags == {
        "sdi_record_uuid": UUID,
        "sdi_edition": "01.00",
        "sdi_date_stamp": "2026-09-25T07:36:46.809835+00:00",
    }


# -- as a file for DDS -----------------------------------------------------


def test_record_is_a_file_dds_can_push(sdi: SdiController) -> None:
    metadata = extracted(sdi)

    assert metadata.filename == f"{UUID}.xml"
    assert metadata.content == iso_xml()
    assert metadata.content_type == "application/xml"


def test_controller_has_no_push(sdi: SdiController) -> None:
    assert not hasattr(sdi, "push_to_dds")
    assert not hasattr(sdi, "check_catalog_path")


# -- resolve_series ---------------------------------------------------------


def _mock_series(hits: list[dict[str, object]]) -> None:
    respx.get(record_url(SERIES_UUID)).mock(
        return_value=httpx.Response(
            200, content=iso_xml(SERIES_UUID, scope="series", children=("old", UUID))
        )
    )
    respx.post(f"{API}/search/records/_search").mock(
        return_value=httpx.Response(200, json={"hits": {"hits": hits}})
    )


@respx.mock
def test_resolve_series_returns_the_one_current_release(sdi: SdiController) -> None:
    _mock_series([search_hit("old", superseded=True), search_hit(UUID, superseded=False)])

    assert sdi.resolve_series(SERIES_UUID) == UUID


@respx.mock
@pytest.mark.parametrize("current", [0, 2])
def test_resolve_series_refuses_to_guess(sdi: SdiController, current: int) -> None:
    _mock_series(
        [search_hit("old", superseded=current == 0), search_hit(UUID, superseded=current == 0)]
    )

    with pytest.raises(NotCurrentError) as info:
        sdi.resolve_series(SERIES_UUID)

    assert {c.uuid for c in info.value.candidates} == {"old", UUID}
    assert f"has {current} current release(s)" in str(info.value)


# -- configuration ----------------------------------------------------------


def test_repr_hides_credentials(sdi: SdiController) -> None:
    assert "s3cr3t" not in repr(sdi)
