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
    """Response from ``GET /api/v1/ingest/{session_id}`` (and the list/retry calls).

    ``status`` is the lifecycle state — ``pending`` → ``uploading`` →
    ``committing`` → ``done``, or ``failed`` / ``cancelled``. ``raw`` keeps the
    whole payload so fields the client does not model yet stay reachable.
    """

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

    @property
    def session_id(self) -> str | None:
        value = self.raw.get("session_id")
        return str(value) if value is not None else None

    @property
    def table_path(self) -> str | None:
        value = self.raw.get("table_path")
        return str(value) if value is not None else None

    @property
    def record_count(self) -> int | None:
        value = self.raw.get("record_count")
        return int(value) if value is not None else None

    @property
    def placement(self) -> str:
        """Where this transfer's files live: ``staged`` or ``read_permanent``.

        ``staged`` (the default, and what a server without DI-11 reports by
        omitting the field) means the upload was copied into the catalog and
        deleted. ``read_permanent`` means the files were stored where the table
        lives and kept — so re-uploading them is not a safe way to recover.
        """
        value = self.raw.get("placement")
        return str(value) if value else "staged"

    @property
    def stores_permanently(self) -> bool:
        """Whether this transfer's uploaded files ARE the table (DI-11)."""
        return self.placement == "read_permanent"

    @property
    def error(self) -> str | None:
        """Why the transfer failed, including the stage — e.g. ``"Dremio load
        failed: ..."`` or ``"S3 upload failed: ..."``. ``None`` unless failed."""
        value = self.raw.get("error")
        return str(value) if value is not None else None

    @property
    def is_terminal(self) -> bool:
        """Whether the session has finished, one way or another."""
        return self.status in ("done", "failed", "cancelled")

    @property
    def is_resumable(self) -> bool:
        """Whether :meth:`FolderIngest.retry` can re-run this transfer.

        A failed transfer is resumable when the server kept its staged files —
        which it does when the upload landed and only the Dremio load failed on
        a managed catalog. Anything else has to be uploaded again.

        This reads the server's own ``resumable`` verdict rather than re-deriving
        it from ``failed_stage``: a load-stage failure the server could not hold
        the bytes for reports ``failed_stage="load"`` too, and inferring from that
        sent :meth:`~eea_datalakehouse.dds_ingestion.folder.FolderIngest.retry`
        down the resume path to be told the staged data was gone — instead of
        simply re-uploading. A server that does not send the field reads as not
        resumable, which is the safe direction: the transfer is re-uploaded
        under a new session.
        """
        return self.status == "failed" and bool(self.raw.get("resumable"))


@dataclass(frozen=True, slots=True)
class EstimateResult:
    """Response from ``POST /api/v1/ingest/estimate`` (pre-flight sizing)."""

    record_count: int
    size_class: str

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> EstimateResult:
        return cls(
            record_count=int(data.get("record_count", 0)),
            size_class=str(data.get("size_class", "")),
        )


@dataclass(frozen=True, slots=True)
class StageResult:
    """Response from ``POST /api/v1/ingest/stage`` (server-proxied upload)."""

    rel_path: str
    key: str
    bytes_written: int | None = None

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> StageResult:
        return cls(
            rel_path=str(data["rel_path"]),
            key=str(data["key"]),
            bytes_written=data.get("bytes"),
        )
