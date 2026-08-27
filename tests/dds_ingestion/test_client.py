from __future__ import annotations

import httpx
import pytest
import respx
from eea_datalakehouse.dds_ingestion.client import (
    IngestApiError,
    IngestClient,
    S3UploadError,
    StorageUnavailableError,
)
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds
from eea_datalakehouse.dds_ingestion.models import FileSpec, UploadPart, UploadTarget

from .conftest import BASE_URL


@respx.mock
def test_begin_sends_contract_body_and_bearer_auth(creds: DremioCreds) -> None:
    route = respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "sess-1",
                "status": "open",
                "s3": {"bucket": "b", "key_prefix": "p/", "uploads": []},
                "collision": None,
            },
        )
    )
    with IngestClient(BASE_URL, creds) as client:
        result = client.begin(
            target_catalog_path="bio.uploads",
            intent="read_only",
            data_format="parquet",
            conflict_mode="fail",
            files=[FileSpec("a.parquet", 6)],
            table_name="t",
            idempotency_key="idem-1",
        )

    assert result.session_id == "sess-1"
    request = route.calls.last.request
    sent = request.read()
    import json

    body = json.loads(sent)
    assert body["target_catalog_path"] == "bio.uploads"
    assert body["runner"] == "notebook"
    assert body["format"] == "parquet"
    assert body["files"] == [{"rel_path": "a.parquet", "size": 6}]
    assert body["idempotency_key"] == "idem-1"
    # Bearer header carries the Dremio PAT (the kernel's _DREMIO_PWD)
    assert request.headers["authorization"] == "Bearer s3cr3t-pwd"


@respx.mock
def test_upload_file_posts_to_presigned_policy(creds: DremioCreds) -> None:
    route = respx.post("https://s3.example/bucket").mock(
        return_value=httpx.Response(204, headers={"ETag": '"abc123"'})
    )
    with IngestClient(BASE_URL, creds) as client:
        target = UploadTarget(
            rel_path="a.parquet",
            url="https://s3.example/bucket",
            method="POST",
            fields={"key": "p/a.parquet", "policy": "x", "x-amz-signature": "sig"},
        )
        etag = client.upload_file(target, b"PAR1-a")

    assert etag == '"abc123"'
    body = route.calls.last.request.read()
    # multipart/form-data carries the POST-policy fields and the file part
    assert b"PAR1-a" in body
    assert b"x-amz-signature" in body
    assert b'name="file"' in body
    # The DDS Bearer token must NOT leak onto the presigned S3 upload.
    assert "authorization" not in route.calls.last.request.headers


@respx.mock
def test_upload_rejects_oversize(creds: DremioCreds) -> None:
    with IngestClient(BASE_URL, creds) as client:
        target = UploadTarget(rel_path="a", url="https://s3.example/a", max_bytes=2)
        with pytest.raises(IngestApiError) as exc:
            client.upload_file(target, b"too long")
    assert exc.value.status_code == 413


@respx.mock
def test_upload_file_multipart_splits_and_puts_parts(creds: DremioCreds) -> None:
    p1 = respx.put("https://s3.example/part1").mock(
        return_value=httpx.Response(200, headers={"ETag": '"etag-1"'})
    )
    p2 = respx.put("https://s3.example/part2").mock(
        return_value=httpx.Response(200, headers={"ETag": '"etag-2"'})
    )
    target = UploadTarget(
        rel_path="big.parquet",
        method="PUT",
        upload_id="up-123",
        parts=(
            UploadPart(part_number=1, url="https://s3.example/part1"),
            UploadPart(part_number=2, url="https://s3.example/part2"),
        ),
    )
    with IngestClient(BASE_URL, creds) as client:
        parts = client.upload_file_multipart(target, b"AAAABBBB")  # 8 bytes → 4+4

    assert parts == [
        {"part_number": 1, "etag": '"etag-1"'},
        {"part_number": 2, "etag": '"etag-2"'},
    ]
    # The bytes were split: part 1 got the first half, part 2 the second.
    assert p1.calls.last.request.read() == b"AAAA"
    assert p2.calls.last.request.read() == b"BBBB"
    # No DDS bearer token leaks onto the presigned part PUTs.
    assert "authorization" not in p1.calls.last.request.headers


@respx.mock
def test_upload_file_multipart_missing_etag_raises(creds: DremioCreds) -> None:
    respx.put("https://s3.example/part1").mock(return_value=httpx.Response(200))
    target = UploadTarget(
        rel_path="big.parquet",
        method="PUT",
        upload_id="up-1",
        parts=(UploadPart(part_number=1, url="https://s3.example/part1"),),
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError) as exc:
        client.upload_file_multipart(target, b"data")
    assert exc.value.status_code == 502


def test_upload_file_multipart_rejects_non_multipart(creds: DremioCreds) -> None:
    target = UploadTarget(rel_path="a", url="https://s3.example/a")
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError):
        client.upload_file_multipart(target, b"x")


def test_repr_does_not_expose_bearer_token(creds: DremioCreds) -> None:
    with IngestClient(BASE_URL, creds) as client:
        rendered = repr(client)
    assert creds.password not in rendered
    assert "Bearer" not in rendered
    assert "Authorization" not in rendered


@respx.mock
def test_commit_returns_table_path(creds: DremioCreds) -> None:
    respx.post(f"{BASE_URL}/api/v1/ingest/commit").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "sess-1",
                "status": "committed",
                "table_path": "bio.uploads.t",
                "record_count": 42,
            },
        )
    )
    with IngestClient(BASE_URL, creds) as client:
        result = client.commit(session_id="sess-1")
    assert result.table_path == "bio.uploads.t"
    assert result.record_count == 42


@respx.mock
def test_error_response_raises_with_message(creds: DremioCreds) -> None:
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(409, json={"error": "conflict", "message": "exists"})
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError) as exc:
        client.begin(
            target_catalog_path="x",
            intent="read_only",
            data_format="csv",
            conflict_mode="fail",
            files=[],
        )
    assert exc.value.status_code == 409
    assert "exists" in str(exc.value)


@respx.mock
def test_dds_error_names_the_call_that_failed(creds: DremioCreds) -> None:
    """A 500 with Starlette's bare body must still say WHICH call broke.

    The message an ingest reported was "DDS ingest API error 500: Internal
    Server Error" — true, and useless: a transfer calls begin, upload and commit
    against two different systems.
    """
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError) as exc:
        client.begin(
            target_catalog_path="x",
            intent="read_only",
            data_format="parquet",
            conflict_mode="fail",
            files=[FileSpec("a.parquet", 6)],
        )

    assert exc.value.where == "POST /api/v1/ingest/begin"
    assert "POST /api/v1/ingest/begin" in str(exc.value)
    assert "Internal Server Error" in str(exc.value)


@respx.mock
def test_failed_presigned_upload_is_not_reported_as_a_dds_error(
    creds: DremioCreds,
) -> None:
    """The upload goes straight to object storage; its failures are its own."""
    respx.post("https://s3.example/bucket").mock(
        return_value=httpx.Response(500, text="<html>gateway said no</html>")
    )
    target = UploadTarget(
        rel_path="a.parquet", url="https://s3.example/bucket", method="POST"
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(S3UploadError) as exc:
        client.upload_file(target, b"PAR1")

    assert exc.value.rel_path == "a.parquet"
    assert exc.value.url == "https://s3.example/bucket"
    message = str(exc.value)
    assert message.startswith("S3 upload of 'a.parquet' failed: HTTP 500")
    assert "https://s3.example/bucket" in message
    assert "DDS ingest API error" not in message
    # …and it is still an IngestApiError, so existing handling keeps working.
    assert isinstance(exc.value, IngestApiError)


@respx.mock
def test_long_error_bodies_are_truncated(creds: DremioCreds) -> None:
    respx.post(f"{BASE_URL}/api/v1/ingest/commit").mock(
        return_value=httpx.Response(502, text="x" * 5000)
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError) as exc:
        client.commit(session_id="sess-1")

    assert exc.value.message.endswith("… (truncated)")
    assert len(exc.value.message) < 600


@respx.mock
def test_begin_sends_sub_path_only_when_given(creds: DremioCreds) -> None:
    """DI-11.12: the named sub-folder rides on ``begin`` and nowhere else.

    Omitted when unset, so a client that never uses it sends the body it always
    sent — an older server sees no new field.
    """
    import json

    route = respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(
            200,
            json={
                "session_id": "sess-1",
                "status": "open",
                "s3": {"bucket": "b", "key_prefix": "p/", "uploads": []},
                "collision": None,
            },
        )
    )
    kwargs: dict[str, object] = {
        "target_catalog_path": "bio.uploads",
        "intent": "read_only",
        "data_format": "parquet",
        "conflict_mode": "append",
        "files": [FileSpec("a.parquet", 6)],
    }
    with IngestClient(BASE_URL, creds) as client:
        client.begin(**kwargs)  # type: ignore[arg-type]
        assert "sub_path" not in json.loads(route.calls.last.request.read())

        client.begin(sub_path="2026", **kwargs)  # type: ignore[arg-type]
        assert json.loads(route.calls.last.request.read())["sub_path"] == "2026"


def test_commit_result_carries_the_physical_location() -> None:
    """DI-11.9: ``storage_path`` says where permanently-stored files actually are.

    ``table_path`` is where the table is queried; the two differ for a read-only
    ingest that keeps its files. Absent (a staged ingest, or a server predating
    the field) parses as ``None`` rather than failing — the models are
    deliberately permissive about keys they do not know.
    """
    from eea_datalakehouse.dds_ingestion.models import CommitResult

    kept = CommitResult.from_json(
        {
            "session_id": "s1",
            "status": "done",
            "table_path": "catalog/water/bwd/assessments",
            "record_count": 12,
            "storage_path": "local_s3/dh-prod-data/read/water/bwd/assessments",
        }
    )
    assert kept.table_path == "catalog/water/bwd/assessments"
    assert kept.storage_path == "local_s3/dh-prod-data/read/water/bwd/assessments"

    staged = CommitResult.from_json(
        {"session_id": "s2", "status": "done", "table_path": "x/y", "record_count": 1}
    )
    assert staged.storage_path is None


# --- storage the service itself cannot write to (DDS 503) ------------------

_STORAGE_DOWN = (
    "this transfer cannot start: DDS cannot write to the S3 storage behind the "
    "catalog, so every file would fail to upload. This is a dependency problem "
    "between DDS and S3 that has to be resolved by an administrator — it is not "
    "caused by your data or your permissions, and nothing has been uploaded. "
    "Verified at 2026-08-25T14:02:11+00:00 — catalog bucket (ingest): write: "
    "ClientError: AccessDenied (HTTP 403): Access Denied."
)


@respx.mock
def test_storage_unavailable_is_its_own_error(creds: DremioCreds) -> None:
    """DDS refusing up front is not the caller's mistake, and says so.

    The server has already written the explanation for whoever is reading it in
    a notebook, so it must arrive intact rather than wrapped in plumbing.
    """
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(
            503,
            json={
                "error": "storage_unavailable",
                "message": _STORAGE_DOWN,
                "path": "bio.uploads",
            },
        )
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(
        StorageUnavailableError
    ) as exc:
        client.begin(
            target_catalog_path="bio.uploads",
            intent="read_only",
            data_format="parquet",
            conflict_mode="fail",
            files=[FileSpec("a.parquet", 6)],
        )

    assert exc.value.status_code == 503
    assert str(exc.value) == _STORAGE_DOWN
    assert "DDS ingest API error" not in str(exc.value)
    assert exc.value.where == "POST /api/v1/ingest/begin"
    # …and still an IngestApiError, so existing handling keeps working.
    assert isinstance(exc.value, IngestApiError)


@respx.mock
def test_a_503_from_a_proxy_is_not_called_a_storage_fault(creds: DremioCreds) -> None:
    """Only DDS's own slug means storage; a gateway's 503 means the gateway."""
    respx.post(f"{BASE_URL}/api/v1/ingest/begin").mock(
        return_value=httpx.Response(503, text="<html>503 Service Unavailable</html>")
    )
    with IngestClient(BASE_URL, creds) as client, pytest.raises(IngestApiError) as exc:
        client.begin(
            target_catalog_path="bio.uploads",
            intent="read_only",
            data_format="parquet",
            conflict_mode="fail",
            files=[FileSpec("a.parquet", 6)],
        )

    assert not isinstance(exc.value, StorageUnavailableError)
    assert "POST /api/v1/ingest/begin" in str(exc.value)
