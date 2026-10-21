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
    route = respx.put(f"{BASE_URL}/api/v1/files/a/b/x.xml").mock(return_value=httpx.Response(201))

    client.put("/a/b/x.xml", b"<x/>", content_type="application/xml")

    request = route.calls.last.request
    assert request.content == b"<x/>"
    assert request.headers["Content-Type"] == "application/xml"
    assert request.headers["Authorization"] == "Bearer s3cr3t-pwd"
    assert request.headers["X-DDS-Overwrite"] == "false"
    assert "overwrite" not in request.url.params


@respx.mock
def test_folders_reads_the_live_list(client: DocumentsClient) -> None:
    respx.get(f"{BASE_URL}/api/v1/folders").mock(
        return_value=httpx.Response(
            200,
            json={"folders": [{"key": "metadata", "role": "x"}, {"key": "maps"}], "is_admin": 0},
        )
    )

    assert client.folders() == ["metadata", "maps"]


@respx.mock
def test_put_encodes_spaces_in_path(client: DocumentsClient) -> None:
    route = respx.put(f"{BASE_URL}/api/v1/files/a%20b/x.xml").mock(return_value=httpx.Response(201))

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
    head = respx.head(f"{BASE_URL}/api/v1/files/a/x.xml").mock(return_value=httpx.Response(200))
    respx.head(f"{BASE_URL}/api/v1/files/a/missing.xml").mock(return_value=httpx.Response(404))

    assert client.get("a/x.xml") == b"<x/>"
    assert client.get("a/missing.xml") is None
    assert client.exists("a/x.xml")
    assert not client.exists("a/missing.xml")
    assert head.calls.last.request.headers["Authorization"] == "Bearer s3cr3t-pwd"


LIST_URL = f"{BASE_URL}/api/v1/files/a/b/"


@respx.mock
@pytest.mark.parametrize(
    "payload",
    [
        ["a/b/x.xml", "a/b/y.xml"],
        {"files": [{"path": "a/b/x.xml"}, {"path": "a/b/y.xml"}]},
        {"items": [{"name": "x.xml", "type": "file"}, {"name": "y.xml", "type": "file"}]},
        {"entries": [{"key": "a/b/x.xml"}, {"key": "a/b/y.xml"}], "next_cursor": None},
        {"children": [{"rel_path": "x.xml"}, {"rel_path": "y.xml"}]},
    ],
)
def test_list_reads_the_folder_listing(client: DocumentsClient, payload: object) -> None:
    route = respx.get(LIST_URL).mock(return_value=httpx.Response(200, json=payload))

    names = client.list("/a/b/")

    assert [name.rsplit("/", 1)[-1] for name in names] == ["x.xml", "y.xml"]
    request = route.calls.last.request
    assert request.url.path == "/api/v1/files/a/b/"  # the trailing slash asks for a listing
    assert request.url.params["limit"] == "200"
    assert request.headers["Authorization"] == "Bearer s3cr3t-pwd"


@respx.mock
def test_list_leaves_subfolders_out(client: DocumentsClient) -> None:
    respx.get(LIST_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    {"name": "annex/", "type": "file"},
                    {"name": "old", "type": "directory"},
                    {"name": "older", "is_dir": True},
                    {"name": "x.xml", "type": "file"},
                ]
            },
        )
    )

    assert client.list("a/b") == ["x.xml"]


@respx.mock
def test_list_follows_the_cursor_to_the_last_page(client: DocumentsClient) -> None:
    route = respx.get(LIST_URL).mock(
        side_effect=[
            httpx.Response(200, json={"items": ["x.xml"], "next_cursor": "p2"}),
            httpx.Response(200, json={"items": ["y.xml"], "next_cursor": "p3"}),
            httpx.Response(200, json={"items": ["z.xml"]}),
        ]
    )

    assert client.list("a/b") == ["x.xml", "y.xml", "z.xml"]
    assert [call.request.url.params.get("cursor") for call in route.calls] == [None, "p2", "p3"]


@respx.mock
def test_list_stops_when_the_cursor_does_not_move(client: DocumentsClient) -> None:
    route = respx.get(LIST_URL).mock(
        return_value=httpx.Response(200, json={"items": ["x.xml"], "cursor": "same"})
    )

    assert client.list("a/b") == ["x.xml", "x.xml"]
    assert route.call_count == 2


@respx.mock
def test_list_of_a_folder_dds_does_not_have_is_empty(client: DocumentsClient) -> None:
    respx.get(LIST_URL).mock(return_value=httpx.Response(404))

    assert client.list("a/b") == []


@respx.mock
@pytest.mark.parametrize(
    ("response", "message"),
    [
        (httpx.Response(500, json={"message": "boom"}), "500 on GET /api/v1/files/a/b/: boom"),
        (
            httpx.Response(200, json={"weird": 1}),
            r"unrecognised folder listing \(top-level keys: weird\)",
        ),
        (
            httpx.Response(200, json={"items": [{"size": 3}]}),
            r"entry without a path or name \(keys: size\)",
        ),
        (
            httpx.Response(200, text="<html/>", headers={"content-type": "text/html"}),
            "expected a JSON folder listing, got 'text/html'",
        ),
    ],
)
def test_list_errors_say_what_came_back(
    client: DocumentsClient, response: httpx.Response, message: str
) -> None:
    respx.get(LIST_URL).mock(return_value=response)

    with pytest.raises(DocumentsApiError, match=message):
        client.list("a/b")


@pytest.mark.parametrize("bad", ["", "/", "a/../b", "a//b"])
def test_rejects_bad_paths(client: DocumentsClient, bad: str) -> None:
    with pytest.raises(ValueError):
        client.get(bad)


def test_from_env_and_repr_hide_the_token() -> None:
    client = DocumentsClient.from_env(
        {"DDS_BASE_URL": f"{BASE_URL}/", "_DREMIO_USER": "u", "_DREMIO_PWD": "s3cr3t"}
    )
    assert repr(client) == f"DocumentsClient(base_url={BASE_URL!r})"
