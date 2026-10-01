from __future__ import annotations

import httpx
import pytest
import respx

from eea_datalakehouse.sdi import (
    DdsCopyConflict,
    NotCurrentError,
    NotIso19115_3,
    SdiController,
    SdiMetadata,
    SdiNotFound,
    UuidMismatch,
    metadata_path,
)

from .conftest import (
    API,
    SERIES_UUID,
    UUID,
    dds_file_url,
    iso_xml,
    not_found,
    record_url,
    search_hit,
)

TARGET = f"water/bathing_water/metadata/{UUID}.xml"


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


# -- push_to_dds ------------------------------------------------------------


def test_metadata_path() -> None:
    assert metadata_path("/water/bathing_water/", UUID) == TARGET
    assert metadata_path("water", UUID, folder="/DOCS/") == f"water/docs/{UUID}.xml"
    with pytest.raises(ValueError):
        metadata_path("/", UUID)


def test_push_uploads_new_file(sdi: SdiController) -> None:
    metadata = extracted(sdi)
    with respx.mock:
        respx.get(dds_file_url(TARGET)).mock(return_value=not_found())
        put = respx.put(dds_file_url(TARGET)).mock(return_value=httpx.Response(201))

        result = sdi.push_to_dds(metadata, "/water/bathing_water/")

    assert (result.dds_path, result.action) == (TARGET, "uploaded")
    request = put.calls.last.request
    assert request.content == iso_xml()
    assert request.headers["Authorization"] == "Bearer s3cr3t-pwd"
    assert request.headers["Content-Type"] == "application/xml"
    assert request.url.params["overwrite"] == "false"


def test_push_same_bytes_is_unchanged(sdi: SdiController) -> None:
    metadata = extracted(sdi)
    with respx.mock:
        respx.get(dds_file_url(TARGET)).mock(return_value=httpx.Response(200, content=iso_xml()))
        put = respx.put(dds_file_url(TARGET))

        result = sdi.push_to_dds(metadata, "water/bathing_water")

    assert result.action == "unchanged"
    assert not put.called


def test_push_replaces_older_copy(sdi: SdiController) -> None:
    metadata = extracted(sdi)
    older = iso_xml(revised="2026-06-02T00:00:00Z")
    with respx.mock:
        respx.get(dds_file_url(TARGET)).mock(return_value=httpx.Response(200, content=older))
        put = respx.put(dds_file_url(TARGET)).mock(return_value=httpx.Response(200))

        result = sdi.push_to_dds(metadata, "water/bathing_water")

    assert result.action == "replaced"
    assert put.calls.last.request.url.params["overwrite"] == "true"


@pytest.mark.parametrize(
    "dds_copy",
    [
        iso_xml(revised="2026-09-30T00:00:00Z"),  # newer than SDI's
        iso_xml(title="edited in DDS"),  # same date, different content
        b"not xml at all",
    ],
)
def test_push_refuses_to_overwrite_dds_edits(sdi: SdiController, dds_copy: bytes) -> None:
    metadata = extracted(sdi)
    with respx.mock:
        respx.get(dds_file_url(TARGET)).mock(return_value=httpx.Response(200, content=dds_copy))
        put = respx.put(dds_file_url(TARGET))

        with pytest.raises(DdsCopyConflict, match="force=True"):
            sdi.push_to_dds(metadata, "water/bathing_water")

    assert not put.called


def test_push_force_overwrites_dds_edits(sdi: SdiController) -> None:
    metadata = extracted(sdi)
    newer = iso_xml(revised="2026-09-30T00:00:00Z")
    with respx.mock:
        respx.get(dds_file_url(TARGET)).mock(return_value=httpx.Response(200, content=newer))
        respx.put(dds_file_url(TARGET)).mock(return_value=httpx.Response(200))

        assert sdi.push_to_dds(metadata, "water/bathing_water", force=True).action == "replaced"


def test_push_refuses_mismatched_record(sdi: SdiController) -> None:
    metadata = extracted(sdi)
    forged = SdiMetadata(UUID, iso_xml("other"), metadata.record)

    with pytest.raises(UuidMismatch):
        sdi.push_to_dds(forged, "water")


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
