from __future__ import annotations

import json
import os

import httpx
import pytest
import respx

from eea_datalakehouse.sdi import (
    SdiApiError,
    SdiAuthError,
    SdiCatalogue,
    SdiConfig,
    SdiController,
    SdiNotFound,
)

from .conftest import API, SDI_URL, UUID, iso_xml, record_url, search_hit


@pytest.fixture
def catalogue() -> SdiCatalogue:
    return SdiCatalogue(SdiConfig(api_url=SDI_URL))


@respx.mock
def test_get_xml_returns_bytes_unchanged(catalogue: SdiCatalogue) -> None:
    route = respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))

    assert catalogue.get_xml(UUID) == iso_xml()
    assert route.calls.last.request.headers["Accept"] == "application/xml"
    assert "Authorization" not in route.calls.last.request.headers


@respx.mock
def test_get_xml_sends_basic_auth_when_configured() -> None:
    route = respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    catalogue = SdiCatalogue(SdiConfig(api_url=SDI_URL, username="u", _password="p"))

    catalogue.get_xml(UUID)

    assert route.calls.last.request.headers["Authorization"].startswith("Basic ")


@respx.mock
@pytest.mark.parametrize(
    ("status", "error"), [(404, SdiNotFound), (401, SdiAuthError), (403, SdiAuthError)]
)
def test_get_xml_errors(catalogue: SdiCatalogue, status: int, error: type[Exception]) -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(status))

    with pytest.raises(error):
        catalogue.get_xml(UUID)


@respx.mock
def test_get_xml_other_error_names_the_call(catalogue: SdiCatalogue) -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(502, text="Bad Gateway"))

    with pytest.raises(SdiApiError, match=r"502 on GET /srv/api/records/.*Bad Gateway"):
        catalogue.get_xml(UUID)


@respx.mock
def test_search_by_uuid(catalogue: SdiCatalogue) -> None:
    route = respx.post(f"{API}/search/records/_search").mock(
        return_value=httpx.Response(
            200,
            json={
                "hits": {
                    "hits": [search_hit("a", superseded=True), search_hit("b", superseded=False)]
                }
            },
        )
    )

    hits = catalogue.search_by_uuid(["a", "b"])

    assert [(h.uuid, h.status) for h in hits] == [("a", "superseded"), ("b", None)]
    assert hits[1].publication_date == "2026-06-02"
    assert hits[1].title == "release b"
    body = json.loads(route.calls.last.request.content)
    assert body["query"] == {"terms": {"uuid": ["a", "b"]}}
    assert body["size"] == 2


def test_search_by_uuid_with_nothing_makes_no_call(catalogue: SdiCatalogue) -> None:
    assert catalogue.search_by_uuid([]) == []


def test_config_from_env_defaults_to_public_catalogue() -> None:
    assert SdiConfig.from_env({}).api_url == "https://sdi.eea.europa.eu/catalogue"
    assert SdiConfig.from_env({"SDI_API_URL": f"{SDI_URL}/"}).api_url == SDI_URL


def test_config_repr_hides_password() -> None:
    config = SdiConfig.from_env({"SDI_USERNAME": "u", "SDI_PASSWORD": "hunter2"})
    assert "hunter2" not in repr(config)
    assert config.auth is not None


@pytest.mark.skipif(
    not os.environ.get("SDI_NETWORK_TESTS"), reason="set SDI_NETWORK_TESTS=1 to call live SDI"
)
def test_live_catalogue_extracts_bathing_water_release() -> None:
    with SdiController.from_env() as sdi:
        metadata = sdi.get_xml(UUID)
    assert metadata.record.uuid == UUID
    assert metadata.record.title
