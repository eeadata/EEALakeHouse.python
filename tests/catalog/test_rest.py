from __future__ import annotations

import httpx
import pytest
import respx

from eea_datalakehouse.catalog.errors import CatalogOperationError, EngineStartingError
from eea_datalakehouse.catalog.rest import CatalogRestClient

BASE_URL = "https://dremio.example.test"


@respx.mock
def test_exists_true_on_200() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(return_value=httpx.Response(200, json={}))
    client = CatalogRestClient(BASE_URL, "pat")

    assert client.exists("a.b") is True


@respx.mock
def test_exists_false_on_404() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/missing").mock(return_value=httpx.Response(404))
    client = CatalogRestClient(BASE_URL, "pat")

    assert client.exists("a.missing") is False


@respx.mock
def test_exists_raises_on_other_errors() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(500, text="boom")
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError):
        client.exists("a.b")


@respx.mock
def test_exists_raises_engine_starting_on_timeout() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(side_effect=httpx.TimeoutException("t"))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(EngineStartingError):
        client.exists("a.b")


@respx.mock
def test_ensure_folder_path_raises_when_space_missing() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(404))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="does not exist"):
        client.ensure_folder_path("a.b")


@respx.mock
def test_ensure_folder_path_creates_only_missing_levels() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(200, json={}))
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(return_value=httpx.Response(200, json={}))
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b/c").mock(return_value=httpx.Response(404))
    create_route = respx.post(f"{BASE_URL}/api/v3/catalog").mock(
        return_value=httpx.Response(201, json={})
    )

    client = CatalogRestClient(BASE_URL, "pat")
    created = client.ensure_folder_path("a.b.c")

    assert created == ["a.b.c"]
    assert create_route.call_count == 1
    body = create_route.calls.last.request.content
    assert b'"folder"' in body
    assert b'"c"' in body


@respx.mock
def test_ensure_folder_path_returns_empty_when_everything_exists() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(200, json={}))
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(return_value=httpx.Response(200, json={}))
    client = CatalogRestClient(BASE_URL, "pat")

    created = client.ensure_folder_path("a.b")

    assert created == []


@respx.mock
def test_create_folder_tolerates_409_already_exists() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(200, json={}))
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(return_value=httpx.Response(404))
    respx.post(f"{BASE_URL}/api/v3/catalog").mock(return_value=httpx.Response(409))
    client = CatalogRestClient(BASE_URL, "pat")

    created = client.ensure_folder_path("a.b")  # must not raise

    assert created == ["a.b"]


def test_repr_does_not_expose_the_token() -> None:
    client = CatalogRestClient(BASE_URL, "super-secret-pat")

    rendered = repr(client)

    assert "super-secret-pat" not in rendered
    assert "Bearer" not in rendered
