"""Typed models for the DDS Ingest API contract (v0.1).

These mirror the request/response shapes of the ``/api/v1/ingest/*`` endpoints.
They are intentionally permissive on parsing (extra fields are ignored) so the
client keeps working if the server adds new keys.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Intent = Literal["read_only", "editable"]
DataFormat = Literal["parquet", "csv", "json"]


@dataclass(frozen=True, slots=True)
class FileSpec:
    """A local file scheduled for upload, identified by its folder-relative path."""

    rel_path: str
    size: int

    def as_payload(self) -> dict[str, Any]:
        return {"rel_path": self.rel_path, "size": self.size}


@dataclass(frozen=True, slots=True)
class UploadPart:
    """One pre-signed part URL of a multipart upload target (DI-7.3)."""

    part_number: int
    url: str

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> UploadPart:
        return cls(part_number=int(data["part_number"]), url=data["url"])


@dataclass(frozen=True, slots=True)
class UploadTarget:
    """A presigned-upload instruction returned by ``begin`` for one file.

    A *single-shot* target carries a POST ``url`` + ``fields`` (DI-2). A
    *multipart* target instead carries an ``upload_id`` and per-part presigned
    URLs in ``parts`` (DI-7); ``method`` is then ``"PUT"`` and ``url`` is empty.
    """

    rel_path: str
    url: str = ""
    method: str = "POST"
    # Pre-signed POST policy form fields, submitted alongside the file part.
    fields: dict[str, str] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    max_bytes: int | None = None
    # Multipart (DI-7): present only when the server chose a multipart upload.
    upload_id: str | None = None
    parts: tuple[UploadPart, ...] = ()

    @property
    def is_multipart(self) -> bool:
        return self.upload_id is not None and bool(self.parts)

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> UploadTarget:
        return cls(
            rel_path=data["rel_path"],
            url=data.get("url", ""),
            method=data.get("method", "POST"),
            fields=dict(data.get("fields") or {}),
            headers=dict(data.get("headers") or {}),
            max_bytes=data.get("max_bytes"),
            upload_id=data.get("upload_id"),
            parts=tuple(UploadPart.from_json(p) for p in data.get("parts") or ()),
        )


@dataclass(frozen=True, slots=True)
class S3Plan:
    """The S3 upload plan: bucket, key prefix and per-file presigned targets."""

    bucket: str
    key_prefix: str
    uploads: tuple[UploadTarget, ...]

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> S3Plan:
        return cls(
            bucket=data["bucket"],
            key_prefix=data["key_prefix"],
            uploads=tuple(UploadTarget.from_json(u) for u in data.get("uploads", [])),
        )


@dataclass(frozen=True, slots=True)
class BeginResult:
    """Response from ``POST /api/v1/ingest/begin``."""

    session_id: str
    status: str
    s3: S3Plan
    collision: Any = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> BeginResult:
        return cls(
            session_id=data["session_id"],
            status=data["status"],
            s3=S3Plan.from_json(data["s3"]),
            collision=data.get("collision"),
        )


@dataclass(frozen=True, slots=True)
class CommitResult:
    """Response from ``POST /api/v1/ingest/commit``."""

    session_id: str
    status: str
    table_path: str | None = None
    record_count: int | None = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> CommitResult:
        return cls(
            session_id=data["session_id"],
            status=data["status"],
            table_path=data.get("table_path"),
            record_count=data.get("record_count"),
        )


@dataclass(frozen=True, slots=True)
class Progress:
    files_done: int
    files_total: int
    step: str

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Progress:
        return cls(
            files_done=int(data.get("files_done", 0)),
            files_total=int(data.get("files_total", 0)),
            step=str(data.get("step", "")),
        )


@dataclass(frozen=True, slots=True)
class StatusResult:
    """Response from ``GET /api/v1/ingest/{session_id}``."""

    status: str
    progress: Progress
    raw: dict[str, Any]

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> StatusResult:
        return cls(
            status=data["status"],
            progress=Progress.from_json(data.get("progress") or {}),
            raw=data,
        )
