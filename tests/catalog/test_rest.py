from __future__ import annotations

import json

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
def test_is_folder_true_for_a_folder_entity() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"entityType": "folder"})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    assert client.is_folder("a.b") is True


@respx.mock
def test_is_folder_false_for_a_non_folder_entity() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"entityType": "dataset"})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    assert client.is_folder("a.b") is False


@respx.mock
def test_is_folder_false_on_404() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/missing").mock(return_value=httpx.Response(404))
    client = CatalogRestClient(BASE_URL, "pat")

    assert client.is_folder("a.missing") is False


@respx.mock
def test_is_folder_false_on_a_broken_by_path_lookup_rather_than_raising() -> None:
    # The real error this project hit: a Dremio source type whose by-path
    # lookup rejects nested items with a 400, not a 404.
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/catalog/deep/nested").mock(
        return_value=httpx.Response(
            400,
            json={
                "errorMessage": (
                    "Can not get internal item from non-filesystem source [catalog] "
                    "of type [com.dremio.plugins.dremiocatalog.store.DremioCatalogLocalPlugin]"
                )
            },
        )
    )
    client = CatalogRestClient(BASE_URL, "pat")

    assert client.is_folder("catalog.deep.nested") is False


@respx.mock
def test_ensure_folder_path_raises_when_space_missing() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(404))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="does not exist"):
        client.ensure_folder_path("a.b")


@respx.mock
def test_ensure_folder_path_creates_only_missing_levels() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(200, json={}))

    def create_response(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        # "a.b" already there (409); "a.b.c" is genuinely new (201).
        return httpx.Response(409 if body["path"] == ["a", "b"] else 201, json={})

    create_route = respx.post(f"{BASE_URL}/api/v3/catalog").mock(side_effect=create_response)

    client = CatalogRestClient(BASE_URL, "pat")
    created = client.ensure_folder_path("a.b.c")

    assert created == ["a.b.c"]
    assert create_route.call_count == 2
    body = create_route.calls.last.request.content
    assert b'"folder"' in body
    assert b'"c"' in body


@respx.mock
def test_ensure_folder_path_returns_empty_when_everything_exists() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(200, json={}))
    respx.post(f"{BASE_URL}/api/v3/catalog").mock(return_value=httpx.Response(409))
    client = CatalogRestClient(BASE_URL, "pat")

    created = client.ensure_folder_path("a.b")

    assert created == []


@respx.mock
def test_create_folder_tolerates_409_already_exists() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(200, json={}))
    respx.post(f"{BASE_URL}/api/v3/catalog").mock(return_value=httpx.Response(409))
    client = CatalogRestClient(BASE_URL, "pat")

    created = client.ensure_folder_path("a.b")  # must not raise

    assert created == []


@respx.mock
def test_ensure_folder_path_does_not_depend_on_by_path_lookups_for_nested_levels() -> None:
    # Reproduces the real failure: this Dremio source type answers nested
    # by-path GETs with 400 ("Can not get internal item from non-filesystem
    # source ... DremioCatalogLocalPlugin"), not 404 — ensure_folder_path
    # must not need that lookup to succeed for levels under the space/source.
    # No by-path/a/b or by-path/a/b/c route is mocked at all: if the code
    # tried to call either, respx would fail this test on its own.
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a").mock(return_value=httpx.Response(200, json={}))
    create_route = respx.post(f"{BASE_URL}/api/v3/catalog").mock(
        return_value=httpx.Response(201, json={})
    )

    client = CatalogRestClient(BASE_URL, "pat")
    created = client.ensure_folder_path("a.b.c")

    assert created == ["a.b", "a.b.c"]
    assert create_route.call_count == 2


@respx.mock
def test_create_folder_returns_true_when_newly_created() -> None:
    create_route = respx.post(f"{BASE_URL}/api/v3/catalog").mock(
        return_value=httpx.Response(201, json={})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    created = client.create_folder("a.b")

    assert created is True
    body = create_route.calls.last.request.content
    assert b'"folder"' in body
    assert b'"b"' in body


@respx.mock
def test_create_folder_returns_false_when_already_there() -> None:
    respx.post(f"{BASE_URL}/api/v3/catalog").mock(return_value=httpx.Response(409))
    client = CatalogRestClient(BASE_URL, "pat")

    assert client.create_folder("a.b") is False


@respx.mock
def test_create_folder_raises_engine_starting_on_timeout() -> None:
    respx.post(f"{BASE_URL}/api/v3/catalog").mock(side_effect=httpx.TimeoutException("t"))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(EngineStartingError):
        client.create_folder("a.b")


@respx.mock
def test_create_folder_raises_on_other_errors() -> None:
    respx.post(f"{BASE_URL}/api/v3/catalog").mock(return_value=httpx.Response(500, text="boom"))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError):
        client.create_folder("a.b")


@respx.mock
def test_delete_folder_is_a_no_op_when_already_gone() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(return_value=httpx.Response(404))
    delete_route = respx.delete(url__regex=r".*/api/v3/catalog/.*")
    client = CatalogRestClient(BASE_URL, "pat")

    client.delete_folder("a.b")  # must not raise

    assert delete_route.call_count == 0


@respx.mock
def test_delete_folder_raises_when_path_is_not_a_folder() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "id-a-b", "entityType": "dataset"})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="not a folder"):
        client.delete_folder("a.b")


@respx.mock
def test_delete_folder_deletes_an_empty_folder() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "id-a-b", "entityType": "folder", "children": []})
    )
    delete_route = respx.delete(f"{BASE_URL}/api/v3/catalog/id-a-b").mock(
        return_value=httpx.Response(204)
    )
    client = CatalogRestClient(BASE_URL, "pat")

    client.delete_folder("a.b")

    assert delete_route.call_count == 1


@respx.mock
def test_delete_folder_raises_when_not_empty_and_not_cascading() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": "id-a-b",
                "entityType": "folder",
                "children": [{"id": "id-view1", "type": "DATASET", "path": ["a", "b", "view1"]}],
            },
        )
    )
    delete_route = respx.delete(url__regex=r".*/api/v3/catalog/.*")
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="not empty"):
        client.delete_folder("a.b")

    assert delete_route.call_count == 0


@respx.mock
def test_delete_folder_cascade_deletes_contents_and_subfolders() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": "id-a-b",
                "entityType": "folder",
                "children": [
                    {"id": "id-view1", "type": "DATASET", "path": ["a", "b", "view1"]},
                    {
                        "id": "id-sub",
                        "type": "CONTAINER",
                        "containerType": "FOLDER",
                        "path": ["a", "b", "sub"],
                    },
                ],
            },
        )
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b/sub").mock(
        return_value=httpx.Response(
            200,
            json={
                "id": "id-sub",
                "entityType": "folder",
                "children": [
                    {"id": "id-table1", "type": "DATASET", "path": ["a", "b", "sub", "table1"]},
                ],
            },
        )
    )
    delete_route = respx.delete(url__regex=r".*/api/v3/catalog/.*").mock(
        return_value=httpx.Response(204)
    )
    client = CatalogRestClient(BASE_URL, "pat")

    client.delete_folder("a.b", cascade=True)

    deleted_ids = {call.request.url.path.rsplit("/", 1)[-1] for call in delete_route.calls}
    assert deleted_ids == {"id-view1", "id-table1", "id-sub", "id-a-b"}


@respx.mock
def test_delete_folder_raises_engine_starting_on_delete_timeout() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "id-a-b", "entityType": "folder", "children": []})
    )
    respx.delete(f"{BASE_URL}/api/v3/catalog/id-a-b").mock(side_effect=httpx.TimeoutException("t"))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(EngineStartingError):
        client.delete_folder("a.b")


def test_repr_does_not_expose_the_token() -> None:
    client = CatalogRestClient(BASE_URL, "super-secret-pat")

    rendered = repr(client)

    assert "super-secret-pat" not in rendered
    assert "Bearer" not in rendered


@respx.mock
def test_get_wiki_looks_up_id_then_fetches_wiki_text() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123", "path": ["a", "b"]})
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        return_value=httpx.Response(200, json={"text": "# Docs\n\nSome wiki content.", "version": 2})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    text = client.get_wiki("a.b")

    assert text == "# Docs\n\nSome wiki content."


@respx.mock
def test_get_wiki_raises_when_path_does_not_exist() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/missing").mock(return_value=httpx.Response(404))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="does not exist"):
        client.get_wiki("a.missing")


@respx.mock
def test_get_wiki_raises_when_entity_has_no_wiki() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        return_value=httpx.Response(404)
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="has no wiki"):
        client.get_wiki("a.b")


@respx.mock
def test_get_wiki_raises_engine_starting_on_wiki_fetch_timeout() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        side_effect=httpx.TimeoutException("t")
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(EngineStartingError):
        client.get_wiki("a.b")


@respx.mock
def test_get_tags_looks_up_id_then_fetches_tags() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        return_value=httpx.Response(200, json={"tags": ["pii", "reviewed"], "version": 1})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    tags = client.get_tags("a.b")

    assert tags == ["pii", "reviewed"]


@respx.mock
def test_get_tags_raises_when_path_does_not_exist() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/missing").mock(return_value=httpx.Response(404))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="does not exist"):
        client.get_tags("a.missing")


@respx.mock
def test_get_tags_returns_empty_list_when_no_tag_record_yet() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        return_value=httpx.Response(404)
    )
    client = CatalogRestClient(BASE_URL, "pat")

    # No tags is normal, not an error — unlike get_wiki's "has no wiki" case.
    assert client.get_tags("a.b") == []


@respx.mock
def test_get_tags_raises_engine_starting_on_tag_fetch_timeout() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        side_effect=httpx.TimeoutException("t")
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(EngineStartingError):
        client.get_tags("a.b")


@respx.mock
def test_set_wiki_creates_new_wiki_on_the_first_try_no_version_field() -> None:
    # The common case: create succeeds immediately, no GET needed at all.
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    post_route = respx.post(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        return_value=httpx.Response(200, json={"text": "# Docs", "version": 0})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    client.set_wiki("a.b", "# Docs")

    assert post_route.call_count == 1
    body = json.loads(post_route.calls.last.request.content)
    assert body == {"text": "# Docs"}


@respx.mock
def test_set_wiki_falls_back_to_update_when_create_is_rejected() -> None:
    # A wiki already exists (Dremio's GET can answer 200-with-defaults even
    # when nothing was ever set, so we can't tell from a GET beforehand —
    # this is confirmed by trying the create and reacting to its failure).
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    post_route = respx.post(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        side_effect=[
            httpx.Response(409, json={"errorMessage": "already exists"}),
            httpx.Response(200, json={"text": "new text", "version": 4}),
        ]
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        return_value=httpx.Response(200, json={"text": "old text", "version": 3})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    client.set_wiki("a.b", "new text")

    assert post_route.call_count == 2
    first_body = json.loads(post_route.calls[0].request.content)
    second_body = json.loads(post_route.calls[1].request.content)
    assert first_body == {"text": "new text"}
    assert second_body == {"text": "new text", "version": 3}


@respx.mock
def test_set_wiki_raises_when_path_does_not_exist() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/missing").mock(return_value=httpx.Response(404))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="does not exist"):
        client.set_wiki("a.missing", "text")


@respx.mock
def test_set_wiki_raises_when_the_fallback_retry_also_fails() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.post(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        return_value=httpx.Response(500, text="boom")
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        return_value=httpx.Response(404)
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError):
        client.set_wiki("a.b", "text")


@respx.mock
def test_set_wiki_raises_engine_starting_on_first_post_timeout() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.post(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/wiki").mock(
        side_effect=httpx.TimeoutException("t")
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(EngineStartingError):
        client.set_wiki("a.b", "text")


@respx.mock
def test_set_tags_creates_new_tag_record_on_the_first_try_no_version_field() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    post_route = respx.post(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        return_value=httpx.Response(200, json={"tags": ["pii"], "version": 0})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    client.set_tags("a.b", ["pii"])

    assert post_route.call_count == 1
    body = json.loads(post_route.calls.last.request.content)
    assert body == {"tags": ["pii"]}


@respx.mock
def test_set_tags_falls_back_to_update_when_create_is_rejected() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    post_route = respx.post(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        side_effect=[
            httpx.Response(409, json={"errorMessage": "already exists"}),
            httpx.Response(200, json={"tags": ["pii", "reviewed"], "version": 3}),
        ]
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        return_value=httpx.Response(200, json={"tags": ["pii"], "version": 2})
    )
    client = CatalogRestClient(BASE_URL, "pat")

    client.set_tags("a.b", ["pii", "reviewed"])

    assert post_route.call_count == 2
    first_body = json.loads(post_route.calls[0].request.content)
    second_body = json.loads(post_route.calls[1].request.content)
    assert first_body == {"tags": ["pii", "reviewed"]}
    assert second_body == {"tags": ["pii", "reviewed"], "version": 2}


@respx.mock
def test_set_tags_raises_when_path_does_not_exist() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/missing").mock(return_value=httpx.Response(404))
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError, match="does not exist"):
        client.set_tags("a.missing", ["pii"])


@respx.mock
def test_set_tags_raises_when_the_fallback_retry_also_fails() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.post(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        return_value=httpx.Response(500, text="boom")
    )
    respx.get(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        return_value=httpx.Response(404)
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(CatalogOperationError):
        client.set_tags("a.b", ["pii"])


@respx.mock
def test_set_tags_raises_engine_starting_on_first_post_timeout() -> None:
    respx.get(f"{BASE_URL}/api/v3/catalog/by-path/a/b").mock(
        return_value=httpx.Response(200, json={"id": "entity-123"})
    )
    respx.post(f"{BASE_URL}/api/v3/catalog/entity-123/collaboration/tag").mock(
        side_effect=httpx.TimeoutException("t")
    )
    client = CatalogRestClient(BASE_URL, "pat")

    with pytest.raises(EngineStartingError):
        client.set_tags("a.b", ["pii"])
