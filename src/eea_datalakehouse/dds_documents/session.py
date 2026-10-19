"""Notebook-facing session behind the `%dds` magic::

    dds = DdsSession(resolve_path=...)      # %dds — writes files to DDS
    dds.push(METADATA, "bwd_2025.xml")
    dds.push(DELIVERABLES, "report.pdf", "annex/*.xlsx", overwrite=False)

- **The target path is the `%catalog` context.** `push(folder, *files)` puts
  the files in `{context}/{folder}/` — the dataset folder `%catalog use(...)`
  last set. `path=` picks another one, absolute or relative to the context,
  exactly as `%catalog` resolves paths. The resolution is the ``resolve_path``
  callable the magic hands in; without one, `path=` is required and taken as
  given.
- **Every failure is a `DdsSessionError`**, so the magic can print it as one
  short line instead of a traceback (the original error is ``__cause__``).

Connection details come from the kernel environment, as `%catalog` /
`%ingest` read theirs: ``DDS_BASE_URL`` and the Dremio identity —
``_DREMIO_USER`` / ``_DREMIO_PWD`` (what `%ingest` reads), falling back to
``DREMIO_USERNAME`` / ``DREMIO_TOKEN`` (what `%catalog` reads).

**`DDS_BASE_URL` comes from a `.env` file first.** When a `DdsSession` is
built, it looks for `.env` in the notebook's working directory, then in each
parent directory, and takes it from the first one found. Only when that `.env`
doesn't set it does the kernel environment's value apply. Nothing else is read
from the file, and it never changes ``os.environ``. `dds_base_url()` shows
which URL is in use and where it came from.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx

from ..dds_ingestion.credentials import (
    ENV_BASE_URL,
    ENV_PWD,
    ENV_USER,
    DremioCreds,
    MissingCredentialsError,
)
from ..session_utils import check_arg_types, find_dotenv, friendly_errors, read_dotenv_value
from .client import DocumentsApiError, DocumentsClient
from .controller import DdsController, DdsFolder, DdsPushError, PushReport, folder_name

# What `%catalog` reads (see notebook/magics.py's _build_catalog_session).
ENV_CATALOG_USER = "DREMIO_USERNAME"
ENV_CATALOG_TOKEN = "DREMIO_TOKEN"  # noqa: S105 — env var name, not a secret value

# Failures a notebook user can act on; anything else is a bug and keeps its traceback.
_EXPECTED = (
    DdsPushError,
    DocumentsApiError,
    MissingCredentialsError,
    ValueError,
    TypeError,
    OSError,
    httpx.HTTPError,
)


class DdsSessionError(RuntimeError):
    """Any failure of a `DdsSession` call; the original error is ``__cause__``."""


_friendly = friendly_errors(DdsSessionError, _EXPECTED)


def load_dds_creds(env: Mapping[str, str] | None = None) -> DremioCreds:
    """Dremio identity for DDS: `%ingest`'s variables, else `%catalog`'s."""
    source = os.environ if env is None else env
    for user_var, token_var in ((ENV_USER, ENV_PWD), (ENV_CATALOG_USER, ENV_CATALOG_TOKEN)):
        user, token = source.get(user_var), source.get(token_var)
        if user and token:
            return DremioCreds(username=user, _password=token)
    raise MissingCredentialsError(
        f"pushing to DDS needs a Dremio identity in the kernel environment: "
        f"{ENV_USER}/{ENV_PWD} or {ENV_CATALOG_USER}/{ENV_CATALOG_TOKEN}"
    )


class DdsSession:
    """`push` / `target` / `folders` / `dds_base_url` for a notebook — what
    `%dds` dispatches onto.

    ``resolve_path(path)`` turns the ``path`` argument into the dataset's
    catalog path: ``None`` means the current `%catalog` context. ``controller``
    is built from the kernel environment on first use if not given.
    ``dotenv_path`` names the ``.env`` to read ``DDS_BASE_URL`` from (by
    default searched for from the working directory upwards, once, when the
    session is built).
    """

    def __init__(
        self,
        *,
        resolve_path: Callable[[str | None], str | None] | None = None,
        controller: DdsController | None = None,
        env: Mapping[str, str] | None = None,
        dotenv_path: str | Path | None = None,
    ) -> None:
        self._resolve_path_fn = resolve_path or (lambda path: path)
        self._controller = controller
        self._env = env
        self._dotenv_path = Path(dotenv_path) if dotenv_path is not None else find_dotenv()
        self._dotenv_url = (
            read_dotenv_value(self._dotenv_path, ENV_BASE_URL)
            if self._dotenv_path is not None and self._dotenv_path.is_file()
            else None
        )

    def __repr__(self) -> str:
        return f"DdsSession(dotenv={str(self._dotenv_path) if self._dotenv_path else None!r})"

    @_friendly
    def push(
        self,
        folder: DdsFolder,
        *files: Any,
        path: str | None = None,
        overwrite: bool = True,
        target_name: str | dict[str, str] = "",
    ) -> PushReport:
        """Upload `files` to `{path}/{folder}/` in DDS with PUT; `path` defaults
        to the `%catalog` context. `overwrite=False` uploads nothing if any
        file is already there. `target_name` renames files in DDS: a str for
        the one file pushed, or a dict from a file's own name to its new name
        (`{"draft_v3.pdf": "report_2025.pdf"}`); empty (the default) keeps
        each file's own name.

        `files` are given one after another, as lists, or both:
        `push(METADATA, "a.xml", "b.xml")`, `push(METADATA, ["a.xml", "b.xml"])`
        and `push(METADATA, "a.xml", ["b.xml"])` push the same two files."""
        # files is checked by collect_files, which knows every shape it takes.
        folder_name(folder)  # a DdsFolder, nothing else; the error names the choices
        check_arg_types(
            "push",
            ("folder", folder, DdsFolder, False),
            ("path", path, str, True),
            ("overwrite", overwrite, bool, False),
            ("target_name", target_name, (str, dict), False),
        )
        dds_path = self._dds_path(path)
        DdsController.target(dds_path, folder)  # bad folder or path: no settings needed
        flat = [f for item in files for f in (item if isinstance(item, (list, tuple)) else [item])]
        return self._dds().push(
            dds_path, folder, flat, overwrite=overwrite, target_name=target_name
        )

    @_friendly
    def target(self, folder: DdsFolder, path: str | None = None) -> str:
        """The DDS folder `push` would upload to, without uploading anything."""
        folder_name(folder)
        check_arg_types("target", ("folder", folder, DdsFolder, False), ("path", path, str, True))
        return DdsController.target(self._dds_path(path), folder)

    def folders(self) -> list[str]:
        """The folders `push` accepts — the `DdsFolder` values, typed bare on a `%dds` line."""
        return [member.name for member in DdsFolder]

    @_friendly
    def dds_base_url(self) -> str:
        """The DDS URL `push` uses, and where it came from."""
        url, source = self._resolve_url()
        return f"{url}  (from {source})"

    # -- internals --------------------------------------------------------

    def _dds_path(self, path: str | None) -> str:
        resolved = self._resolve_path_fn(path)
        if not resolved:
            raise ValueError(
                "no DDS target path: pick a dataset folder with %catalog use(...) or pass path="
            )
        return resolved

    def _dds(self) -> DdsController:
        if self._controller is None:
            env = os.environ if self._env is None else self._env
            self._controller = DdsController(
                DocumentsClient(self._resolve_url()[0], load_dds_creds(env))
            )
        return self._controller

    def _resolve_url(self) -> tuple[str, str]:
        """``(url, source)``: ``DDS_BASE_URL`` from the ``.env`` file, else the kernel env."""
        if self._dotenv_url and self._dotenv_path is not None:
            return self._dotenv_url.rstrip("/"), str(self._dotenv_path)
        env = os.environ if self._env is None else self._env
        if env.get(ENV_BASE_URL):
            return env[ENV_BASE_URL].rstrip("/"), "the kernel environment"
        where = f"{self._dotenv_path} has none and " if self._dotenv_path else "no .env found and "
        raise MissingCredentialsError(
            f"pushing to DDS needs {ENV_BASE_URL}: {where}it is not set in the kernel environment"
        )
