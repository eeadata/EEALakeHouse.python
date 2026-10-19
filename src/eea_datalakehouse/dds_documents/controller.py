"""Push files into a dataset's non-structured DDS folders.

A dataset folder in DDS (``catalog/water/bwd``) has a few sub-folders for
files that are not data: its metadata records, assessments, deliverables.
:class:`DdsController` uploads files there with ``PUT``, one request per file::

    with DdsController.from_env() as dds:
        dds.push("catalog.water.bwd", DdsFolder.DELIVERABLES, ["report.pdf", "annex.xlsx"])
        # -> catalog/water/bwd/deliverables/report.pdf, .../annex.xlsx

=================================  ==============================================
:meth:`DdsController.push`         upload files to ``{dds_path}/{folder}/{name}``
:meth:`DdsController.target`       the folder they would go to, without uploading
=================================  ==============================================

Like :class:`~eea_datalakehouse.dds_documents.DocumentsClient`, this never
calls the ingest API: nothing pushed here becomes a Dremio table.
"""

from __future__ import annotations

import glob
import mimetypes
import os
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Any

from .client import DocumentsClient
from .paths import path_segments, to_storage_path

DEFAULT_CONTENT_TYPE = "application/octet-stream"
_INVALID_NAME_CHARS = set('<>:"|?*')
_GLOB_CHARS = set("*?[")


class DdsFolder(StrEnum):
    """The non-structured folders a dataset has in DDS; the value is the folder's name."""

    METADATA = "metadata"
    ASSESSMENTS = "assessments"
    DELIVERABLES = "deliverables"

    @classmethod
    def parse(cls, value: DdsFolder | str) -> DdsFolder:
        """``value`` as a folder: a member, or its name in any case (``"Metadata"``)."""
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise TypeError(f"folder must be a DdsFolder or str, not {type(value).__name__}")
        try:
            return cls(value.strip().strip("/").lower())
        except ValueError:
            raise ValueError(
                f"unknown DDS folder {value!r}; use one of {', '.join(m.name for m in cls)}"
            ) from None


@dataclass(frozen=True)
class DdsFile:
    """One file to push: its name in DDS and its bytes."""

    name: str
    data: bytes = field(repr=False)
    content_type: str | None = None

    def __repr__(self) -> str:
        return f"DdsFile(name={self.name!r}, data=<{len(self.data)} bytes>)"


@dataclass(frozen=True)
class PushReport:
    """What :meth:`DdsController.push` uploaded, in order."""

    target: str
    paths: tuple[str, ...]
    overwrite: bool

    def __repr__(self) -> str:
        lines = "".join(f"\n  {path}" for path in self.paths)
        return f"pushed {len(self.paths)} file(s) to {self.target}:{lines}"


class DdsPushError(RuntimeError):
    """A push stopped part-way; ``uploaded`` lists the files already in DDS."""

    def __init__(self, message: str, *, uploaded: Iterable[str] = ()) -> None:
        self.uploaded = tuple(uploaded)
        if self.uploaded:
            message += "; already uploaded: " + ", ".join(self.uploaded)
        super().__init__(message)


class DdsFilesExistError(DdsPushError):
    """``overwrite=False`` and some of the files are already in DDS; nothing was uploaded."""

    def __init__(self, existing: Iterable[str]) -> None:
        self.existing = tuple(existing)
        super().__init__(
            "already in DDS: " + ", ".join(self.existing) + "; pass overwrite=True to replace "
            "them. Nothing was uploaded."
        )


class DdsController:
    """Uploads files to a dataset's non-structured DDS folders.

    ``documents`` is built from the environment (``DDS_BASE_URL``,
    ``_DREMIO_USER`` / ``_DREMIO_PWD``) on first use if not given.
    """

    def __init__(self, documents: DocumentsClient | None = None) -> None:
        self._documents = documents
        self._owns_documents = documents is None

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> DdsController:
        return cls(DocumentsClient.from_env(env))

    def __repr__(self) -> str:
        return f"DdsController(documents={self._documents!r})"

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        if self._documents is not None and self._owns_documents:
            self._documents.close()

    def __enter__(self) -> DdsController:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- public API -------------------------------------------------------

    @staticmethod
    def target(dds_path: str, folder: DdsFolder | str) -> str:
        """``{dds_path}/{folder}`` in storage format — where :meth:`push` puts files.

        ``dds_path`` is the dataset's folder, in catalog (``a.b.c``) or storage
        (``a/b/c``) format. It must be below the catalog root: a bare source
        name (``catalog``) has no dataset to hold the files.
        """
        segments = path_segments(dds_path)
        if not segments:
            raise ValueError("dds_path must not be empty")
        if len(segments) == 1:
            raise ValueError(
                f"{dds_path!r} is the catalog root, not a dataset folder; "
                "pick one with %catalog use(...) or pass its path"
            )
        return f"{to_storage_path(dds_path)}/{DdsFolder.parse(folder).value}"

    def push(
        self,
        dds_path: str,
        folder: DdsFolder | str,
        files: Any,
        *,
        overwrite: bool = True,
    ) -> PushReport:
        """Upload ``files`` to ``{dds_path}/{folder}/{name}`` with ``PUT``.

        ``files`` is one file or a list of them, each a local path (``str`` or
        :class:`os.PathLike`; one with ``*``, ``?`` or ``[`` is a glob
        pattern, whose matching folders are skipped), a :class:`DdsFile`, or
        an object with ``filename`` and ``content`` attributes (an
        ``SdiMetadata`` record, for instance). Every
        file is read and checked before the first upload, so a typo uploads
        nothing.

        ``overwrite=True`` (the default) replaces a file DDS already has.
        ``overwrite=False`` first checks every target and raises
        :class:`DdsFilesExistError`, uploading nothing, if any is taken. If an
        upload fails part-way, :class:`DdsPushError` names the files already
        uploaded.
        """
        target = self.target(dds_path, folder)
        to_push = collect_files(files)
        paths = [f"{target}/{item.name}" for item in to_push]
        documents = self._documents_client()
        if not overwrite:
            existing = [path for path in paths if documents.exists(path)]
            if existing:
                raise DdsFilesExistError(existing)
        uploaded: list[str] = []
        for item, path in zip(to_push, paths, strict=True):
            content_type = (
                item.content_type or mimetypes.guess_type(item.name)[0] or DEFAULT_CONTENT_TYPE
            )
            try:
                documents.put(path, item.data, content_type=content_type, overwrite=overwrite)
            except Exception as exc:
                raise DdsPushError(f"could not upload {path}: {exc}", uploaded=uploaded) from exc
            uploaded.append(path)
        return PushReport(target, tuple(uploaded), overwrite)

    # -- internals --------------------------------------------------------

    def _documents_client(self) -> DocumentsClient:
        if self._documents is None:
            self._documents = DocumentsClient.from_env()
        return self._documents


def collect_files(files: Any) -> list[DdsFile]:
    """``files`` (see :meth:`DdsController.push`) read into :class:`DdsFile` objects.

    Raises ``ValueError`` for no files, a missing file, a folder, a glob that
    matches nothing, an invalid name or two files with the same name, and
    ``TypeError`` for anything that is not a file.
    """
    items = [files] if _is_single(files) else list(files) if files is not None else []
    collected: list[DdsFile] = []
    for item in items:
        collected.extend(_read(item))
    if not collected:
        raise ValueError("no files to push")
    counts = Counter(f.name for f in collected)
    duplicates = sorted(name for name, count in counts.items() if count > 1)
    if duplicates:
        raise ValueError(f"several files would be pushed as {', '.join(duplicates)}")
    return collected


def _is_single(files: Any) -> bool:
    return isinstance(files, (str, os.PathLike, DdsFile, bytes)) or _is_in_memory(files)


def _is_in_memory(item: Any) -> bool:
    return hasattr(item, "filename") and hasattr(item, "content")


def _read(item: Any) -> list[DdsFile]:
    if isinstance(item, DdsFile):
        return [DdsFile(require_file_name(item.name), item.data, item.content_type)]
    if _is_in_memory(item):
        return [
            DdsFile(
                require_file_name(item.filename),
                bytes(item.content),
                getattr(item, "content_type", None),
            )
        ]
    if isinstance(item, (str, os.PathLike)):
        text = os.path.expanduser(os.fspath(item))
        if not _GLOB_CHARS & set(text):
            return [_read_path(Path(text))]
        matches = sorted(glob.glob(text))
        if not matches:
            raise ValueError(f"no file matches {text!r}")
        return [_read_path(Path(match)) for match in matches if not Path(match).is_dir()]
    raise TypeError(
        f"cannot push a {type(item).__name__}; pass a file path, a DdsFile or a list of them"
    )


def _read_path(path: Path) -> DdsFile:
    if path.is_dir():
        raise ValueError(f"{str(path)!r} is a folder; pass its files, e.g. '{path}/*'")
    if not path.is_file():
        raise ValueError(f"no such file: {str(path)!r}")
    return DdsFile(require_file_name(path.name), path.read_bytes())


def require_file_name(name: str) -> str:
    """``name`` as a valid single file name.

    Raises ``ValueError`` if it is empty, a path (``/``, ``\\``, ``.``,
    ``..``), or has a character no file system accepts in a name
    (``< > : " | ? *`` or a control character).
    """
    if not isinstance(name, str):
        raise TypeError(f"file name must be a str, not {type(name).__name__}")
    cleaned = name.strip()
    if not cleaned:
        raise ValueError("file name must not be empty")
    if "/" in cleaned or "\\" in cleaned or cleaned in (".", ".."):
        raise ValueError(f"file name must not be a path: {name!r}")
    bad = sorted({c for c in cleaned if c in _INVALID_NAME_CHARS or ord(c) < 32 or ord(c) == 127})
    if bad:
        raise ValueError(f"{name!r} is not a valid file name: it contains {bad!r}")
    return cleaned
