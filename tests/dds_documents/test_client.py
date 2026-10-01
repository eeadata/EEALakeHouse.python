from __future__ import annotations

import httpx
import pytest
import respx

from eea_datalakehouse.dds_documents import (
    DocumentExistsError,
    DocumentsApiError,
    DocumentsClient,
)
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds

BASE_URL = "https://dds.example.test"


@pytest.fixture
def client() -> DocumentsClient:
    return DocumentsClient(BASE_URL, DremioCreds(username="alice", _password="s3cr3t-pwd"))


@respx.mock
def test_put_sends_body_type_and_bearer(client: DocumentsClient) -> None:
    route = respx.put(f"{BASE_URL}/api/v1/files/a/b/x.xml").mock(
        return_value=httpx.Response(201)
    )

    client.put("/a/b/x.xml", b"<x/>", content_type="application/xml")

    request = route.calls.last.request
    assert request.content == b"<x/>"
    assert request.headers["Content-Type"] == "application/xml"
    assert request.headers["Authorization"] == "Bearer s3cr3t-pwd"
    assert request.url.params["overwrite"] == "false"


@respx.mock
def test_put_encodes_spaces_in_path(client: DocumentsClient) -> None:
    route = respx.put(f"{BASE_URL}/api/v1/files/a%20b/x.xml").mock(
        return_value=httpx.Response(201)
    )

    client.put("a b/x.xml", b"")

    assert route.called


@respx.mock
def test_put_conflict_is_document_exists(client: DocumentsClient) -> None:
    respx.put(f"{BASE_URL}/api/v1/files/a/x.xml").mock(
        return_value=httpx.Response(409, json={"error": "exists", "message": "file exists"})
    )

    with pytest.raises(DocumentExistsError, match="file exists"):
        client.put("a/x.xml", b"")


@respx.mock
def test_put_error_names_the_call(client: DocumentsClient) -> None:
    respx.put(f"{BASE_URL}/api/v1/files/a/x.xml").mock(
        return_value=httpx.Response(403, json={"error": "forbidden", "message": "no write access"})
    )

    with pytest.raises(DocumentsApiError, match=r"403 on PUT /api/v1/files/a/x.xml: no write"):
        client.put("a/x.xml", b"")


@respx.mock
def test_get_and_exists(client: DocumentsClient) -> None:
    respx.get(f"{BASE_URL}/api/v1/files/a/x.xml").mock(
        return_value=httpx.Response(200, content=b"<x/>")
    )
    respx.get(f"{BASE_URL}/api/v1/files/a/missing.xml").mock(return_value=httpx.Response(404))

    assert client.get("a/x.xml") == b"<x/>"
    assert client.exists("a/x.xml")
    assert client.get("a/missing.xml") is None
    assert not client.exists("a/missing.xml")


@respx.mock
@pytest.mark.parametrize(
    "payload", [{"files": [{"path": "a/x.xml"}, {"path": "a/y.xml"}]}, ["a/x.xml", "a/y.xml"]]
)
def test_list(client: DocumentsClient, payload: object) -> None:
    route = respx.get(f"{BASE_URL}/api/v1/files").mock(
        return_value=httpx.Response(200, json=payload)
    )

    assert client.list("/a/") == ["a/x.xml", "a/y.xml"]
    assert route.calls.last.request.url.params["prefix"] == "a"


@pytest.mark.parametrize("bad", ["", "/", "a/../b", "a//b"])
def test_rejects_bad_paths(client: DocumentsClient, bad: str) -> None:
    with pytest.raises(ValueError):
        client.get(bad)


def test_from_env_and_repr_hide_the_token() -> None:
    client = DocumentsClient.from_env(
        {"DDS_BASE_URL": f"{BASE_URL}/", "_DREMIO_USER": "u", "_DREMIO_PWD": "s3cr3t"}
    )
    assert repr(client) == f"DocumentsClient(base_url={BASE_URL!r})"
