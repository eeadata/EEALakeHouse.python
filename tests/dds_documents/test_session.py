from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from eea_datalakehouse.dds_documents import (
    DdsController,
    DdsFile,
    DdsSession,
    DdsSessionError,
    DocumentsClient,
)
from eea_datalakehouse.dds_documents.session import load_dds_creds
from eea_datalakehouse.dds_ingestion.credentials import MissingCredentialsError
from eea_datalakehouse.session_utils import find_dotenv, read_dotenv_value

from .conftest import DATASET, DDS_URL, TARGET, file_url

IDENTITY = {"_DREMIO_USER": "a", "_DREMIO_PWD": "pat-a"}
ENV = {"DDS_BASE_URL": DDS_URL, **IDENTITY}
NO_DOTENV = Path("/nonexistent/.env")
REPORT = DdsFile("report.pdf", b"%PDF-1")


def _session(context: str | None = DATASET, **kwargs: object) -> DdsSession:
    """A session whose `%catalog` context is `context`; a given path is taken as-is."""
    kwargs.setdefault("dotenv_path", NO_DOTENV)
    kwargs.setdefault("env", ENV)
    return DdsSession(
        resolve_path=lambda path: context if path is None else path,
        **kwargs,  # type: ignore[arg-type]
    )


@respx.mock
def test_push_goes_to_the_context_with_the_kernel_identity() -> None:
    put = respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))

    report = _session().push("DELIVERABLES", REPORT)

    assert report.paths == (f"{TARGET}/report.pdf",)
    assert put.calls.last.request.headers["Authorization"] == "Bearer pat-a"


@respx.mock
def test_push_path_overrides_the_context() -> None:
    put = respx.put(file_url("catalog/air/aq/metadata/report.pdf")).mock(
        return_value=httpx.Response(201)
    )

    _session().push("metadata", REPORT, path="catalog/air/aq")

    assert put.called


def test_target_shows_the_folder_without_any_settings() -> None:
    assert _session(env={}).target("DELIVERABLES") == TARGET
    assert _session(env={}).target("metadata", "catalog.air") == "catalog/air/metadata"


@pytest.mark.parametrize("context", [None, ""])
def test_no_context_and_no_path_is_a_session_error(context: str | None) -> None:
    with pytest.raises(DdsSessionError, match=r"%catalog use\(\.\.\.\) or pass path="):
        _session(context).push("metadata", REPORT)


def test_root_context_is_refused_before_any_settings() -> None:
    with pytest.raises(DdsSessionError, match="catalog root"):
        _session("catalog", env={}).push("metadata", REPORT)


def test_unknown_folder_is_refused_before_any_settings() -> None:
    with pytest.raises(DdsSessionError, match="unknown DDS folder 'reports'"):
        _session(env={}).push("reports", REPORT)


def test_a_missing_file_is_a_session_error(tmp_path: Path) -> None:
    with pytest.raises(DdsSessionError, match="no such file"):
        _session().push("metadata", str(tmp_path / "nope.xml"))


def test_push_without_dremio_identity_is_a_session_error() -> None:
    with pytest.raises(DdsSessionError, match="DREMIO_TOKEN") as info:
        _session(env={"DDS_BASE_URL": DDS_URL}).push("metadata", REPORT)
    assert isinstance(info.value.__cause__, MissingCredentialsError)


@pytest.mark.parametrize(
    ("args", "kwargs", "message"),
    [
        ((42, REPORT), {}, r"argument 1 \(folder\) must be str, not int"),
        (("metadata", REPORT), {"path": 1}, r"argument 2 \(path\) must be str or None"),
        (("metadata", REPORT), {"overwrite": 1}, r"argument 3 \(overwrite\) must be bool"),
    ],
)
def test_push_checks_argument_types_strictly(
    args: tuple[object, ...], kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(DdsSessionError, match=message):
        _session(env={}).push(*args, **kwargs)  # type: ignore[arg-type]


def test_call_that_does_not_fit_the_signature_shows_the_usage() -> None:
    with pytest.raises(DdsSessionError) as info:
        _session().push("metadata", REPORT, force=True)  # type: ignore[call-arg]

    assert "got an unexpected keyword argument 'force'" in str(info.value)
    assert str(info.value).endswith("usage: push(folder, files, path=None, overwrite=True)")


def test_folders_lists_the_names_push_accepts() -> None:
    assert _session().folders() == ["METADATA", "ASSESSMENTS", "DELIVERABLES"]


@respx.mock
def test_a_controller_handed_in_is_used(documents: DocumentsClient) -> None:
    respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))

    report = _session(env={}, controller=DdsController(documents)).push("deliverables", REPORT)

    assert report.paths == (f"{TARGET}/report.pdf",)


# -- identity and DDS_BASE_URL ------------------------------------------------


def test_load_dds_creds_prefers_ingest_variables() -> None:
    env = {"_DREMIO_USER": "a", "_DREMIO_PWD": "1", "DREMIO_USERNAME": "b", "DREMIO_TOKEN": "2"}
    assert load_dds_creds(env).username == "a"
    assert load_dds_creds({"DREMIO_USERNAME": "b", "DREMIO_TOKEN": "2"}).username == "b"
    with pytest.raises(MissingCredentialsError):
        load_dds_creds({})


def test_dotenv_dds_url_wins_over_kernel_env(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("# comment\nexport DDS_BASE_URL='https://dds.from-dotenv.test/'\nOTHER=x\n")

    assert _session(dotenv_path=dotenv).dds_base_url() == (
        f"https://dds.from-dotenv.test  (from {dotenv})"
    )


@respx.mock
def test_push_goes_to_the_dotenv_url(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=https://dds.from-dotenv.test\n")
    put = respx.put(file_url(f"{TARGET}/report.pdf", base="https://dds.from-dotenv.test")).mock(
        return_value=httpx.Response(201)
    )

    _session(dotenv_path=tmp_path / ".env").push("deliverables", REPORT)

    assert put.called


def test_empty_dotenv_value_falls_back_to_kernel_env(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=\n")

    assert _session(dotenv_path=tmp_path / ".env").dds_base_url() == (
        f"{DDS_URL}  (from the kernel environment)"
    )


def test_no_dds_url_anywhere_names_both_places(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("OTHER=x\n")

    with pytest.raises(DdsSessionError, match=r"\.env has none and it is not set"):
        _session(dotenv_path=tmp_path / ".env", env={}).dds_base_url()


def test_find_dotenv_walks_up_from_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=https://dds.parent.test\n")
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    assert find_dotenv() == tmp_path / ".env"
    assert DdsSession(env={}).dds_base_url() == (
        f"https://dds.parent.test  (from {tmp_path / '.env'})"
    )


def test_read_dotenv_value_last_assignment_wins(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text('DDS_BASE_URL=one\nDDS_BASE_URL="two"\n')

    assert read_dotenv_value(dotenv, "DDS_BASE_URL") == "two"
    assert read_dotenv_value(dotenv, "MISSING") is None
