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
    to_catalog_path,
    to_storage_path,
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
    assert metadata_path("water.bathing_water", UUID) == TARGET  # catalog format
    assert metadata_path("water", UUID, folder="/DOCS/") == f"water/docs/{UUID}.xml"
    with pytest.raises(ValueError):
        metadata_path("/", UUID)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("catalog.water.bwd", "catalog/water/bwd"),
        (" catalog.water.bwd. ", "catalog/water/bwd"),
        ("catalog/water/bwd", "catalog/water/bwd"),
        ("catalog/water/v1.0", "catalog/water/v1.0"),  # has '/': storage format, dots kept
        ("bwd", "bwd"),
        ('catalog."water_management_resources".bwd', "catalog/water_management_resources/bwd"),
        ('catalog."bathing water"."v1.0"', "catalog/bathing water/v1.0"),
        (
            'nossl_s3."datahub-pre-01".datasets."[EU SDG 14_40] Bathing waters."',
            "nossl_s3/datahub-pre-01/datasets/[EU SDG 14_40] Bathing waters.",
        ),
        ('catalog."say ""hi""".x', 'catalog/say "hi"/x'),  # "" escapes a quote
        ('"a/b".c', "a/b/c"),  # '/' only inside quotes: still catalog format
        ('catalog/"v1.0"/x', "catalog/v1.0/x"),  # storage format, quotes removed too
    ],
)
def test_to_storage_path(path: str, expected: str) -> None:
    assert to_storage_path(path) == expected


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("catalog/water_management_resources/bwd", "catalog.water_management_resources.bwd"),
        ("catalog.water.bwd", "catalog.water.bwd"),
        ("catalog/bathing water/v1.0", 'catalog."bathing water"."v1.0"'),
        ("nossl_s3/datahub-pre-01/x", 'nossl_s3."datahub-pre-01".x'),
        ('catalog."say ""hi"""', 'catalog."say ""hi"""'),  # escaped quote kept escaped
    ],
)
def test_to_catalog_path(path: str, expected: str) -> None:
    assert to_catalog_path(path) == expected
    assert to_storage_path(expected) == to_storage_path(path)  # round trip


def test_metadata_path_accepts_the_record_for_uuid(sdi: SdiController) -> None:
    assert metadata_path("water.bathing_water", extracted(sdi)) == TARGET


def test_metadata_path_rejects_the_record_as_target_name(sdi: SdiController) -> None:
    record = extracted(sdi)
    with pytest.raises(TypeError, match="not SdiMetadata — pass the record as metadata="):
        metadata_path("water", UUID, target_name=record)  # type: ignore[arg-type]


def test_controller_push_checks_argument_types(sdi: SdiController) -> None:
    record = extracted(sdi)
    with pytest.raises(TypeError, match=r"argument 3 \(target_name\) must be str or None"):
        sdi.push_to_dds(record, "water", record)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=r"argument 1 \(metadata\) must be SdiMetadata, not str"):
        sdi.push_to_dds("water", "water")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=r"argument 5 \(force\) must be bool, not int"):
        sdi.push_to_dds(record, "water", force=1)  # type: ignore[arg-type]


def test_metadata_path_rejects_non_str_uuid() -> None:
    with pytest.raises(TypeError, match="uuid must be a str"):
        metadata_path("water", 42)  # type: ignore[arg-type]


def test_metadata_path_target_name() -> None:
    assert metadata_path("water", UUID, target_name="bwd_2025.xml") == "water/metadata/bwd_2025.xml"
    assert metadata_path("water", UUID, target_name=" bwd_2025 ") == "water/metadata/bwd_2025.xml"
    assert metadata_path("water", UUID, target_name="A.XML") == "water/metadata/A.XML"


@pytest.mark.parametrize(
    "bad", ["", "  ", "a/b.xml", "..", "a\\b.xml", "a:b.xml", "what?.xml", 'a"b', "a\tb", "x*"]
)
def test_metadata_path_rejects_bad_target_name(bad: str) -> None:
    with pytest.raises(ValueError, match="target_name"):
        metadata_path("water", UUID, target_name=bad)


def test_push_with_target_name(sdi: SdiController) -> None:
    metadata = extracted(sdi)
    renamed = "water/bathing_water/metadata/bwd_2025_v1.0.xml"
    with respx.mock:
        respx.get(dds_file_url(renamed)).mock(return_value=not_found())
        put = respx.put(dds_file_url(renamed)).mock(return_value=httpx.Response(201))

        result = sdi.push_to_dds(metadata, "water.bathing_water", target_name="bwd_2025_v1.0")

    assert (result.dds_path, result.action, result.uuid) == (renamed, "uploaded", UUID)
    assert put.calls.last.request.content == iso_xml()


def test_to_storage_path_rejects_an_unclosed_quote() -> None:
    with pytest.raises(ValueError, match="unclosed quote"):
        to_storage_path('catalog."water.bwd')


def test_push_converts_a_catalog_path(sdi: SdiController) -> None:
    metadata = extracted(sdi)
    with respx.mock:
        respx.get(dds_file_url(TARGET)).mock(return_value=not_found())
        put = respx.put(dds_file_url(TARGET)).mock(return_value=httpx.Response(201))

        result = sdi.push_to_dds(metadata, "water.bathing_water")

    assert result.dds_path == TARGET
    assert put.called


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
