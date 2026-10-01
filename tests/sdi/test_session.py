from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from eea_datalakehouse.dds_ingestion.credentials import MissingCredentialsError
from eea_datalakehouse.sdi import (
    MetadataSession,
    MetadataSessionError,
    NotIso19115_3,
    SdiController,
    SdiSession,
    SdiSessionError,
)
from eea_datalakehouse.sdi.session import find_dotenv, load_dds_creds, read_dotenv_value

from .conftest import (
    DDS_URL,
    SDI_URL,
    UUID,
    dds_file_url,
    iso_xml,
    not_found,
    record_url,
)

TARGET = f"water/metadata/{UUID}.xml"
ENV = {"SDI_API_URL": SDI_URL, "DDS_BASE_URL": DDS_URL}
NO_DOTENV = Path("/nonexistent/.env")


def _metadata_session(sdi: SdiSession | None = None, **kwargs: object) -> MetadataSession:
    kwargs.setdefault("dotenv_path", NO_DOTENV)
    kwargs.setdefault("env", ENV)
    last = (lambda: sdi.last) if sdi is not None else None
    return MetadataSession(last=last, **kwargs)  # type: ignore[arg-type]


# -- SdiSession ---------------------------------------------------------------


@respx.mock
def test_get_xml_remembers_the_record_and_needs_no_dds_settings() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    session = SdiSession(env={"SDI_API_URL": SDI_URL})

    metadata = session.get_xml(UUID)

    assert session.last is metadata
    assert metadata.uuid == UUID
    assert repr(session) == f"SdiSession(last={UUID!r})"


@respx.mock
def test_sdi_errors_become_session_errors() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, text="<html>"))

    with pytest.raises(SdiSessionError) as info:
        SdiSession(env=ENV).get_xml(UUID)
    assert isinstance(info.value.__cause__, NotIso19115_3)


def test_sdi_session_has_no_push() -> None:
    assert not hasattr(SdiSession, "push_to_dds")
    assert not hasattr(SdiSession, "dds_base_url")


# -- MetadataSession ----------------------------------------------------------


@respx.mock
def test_push_uploads_the_last_sdi_record_with_the_kernel_identity() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    respx.get(dds_file_url(TARGET)).mock(return_value=not_found())
    put = respx.put(dds_file_url(TARGET)).mock(return_value=httpx.Response(201))
    sdi = SdiSession(env=ENV)
    metadata = _metadata_session(
        sdi, env={**ENV, "DREMIO_USERNAME": "carol", "DREMIO_TOKEN": "pat-c"}
    )

    sdi.get_xml(UUID)
    result = metadata.push_to_dds("water")

    assert (result.action, result.dds_path) == ("uploaded", TARGET)
    assert put.calls.last.request.headers["Authorization"] == "Bearer pat-c"


@respx.mock
def test_push_takes_an_explicit_record(sdi: SdiController) -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    respx.get(dds_file_url(TARGET)).mock(return_value=not_found())
    respx.put(dds_file_url(TARGET)).mock(return_value=httpx.Response(201))
    record = sdi.get_xml(UUID)

    result = _metadata_session(controller=sdi).push_to_dds("water", metadata=record)

    assert result.action == "uploaded"


def test_push_before_get_xml_is_a_session_error() -> None:
    with pytest.raises(MetadataSessionError, match=r"run %sdi get_xml"):
        _metadata_session(SdiSession(env=ENV)).push_to_dds("water")


@respx.mock
def test_push_without_dremio_identity_is_a_session_error() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    sdi = SdiSession(env=ENV)
    sdi.get_xml(UUID)

    with pytest.raises(MetadataSessionError, match="DREMIO_TOKEN") as info:
        _metadata_session(sdi).push_to_dds("water")
    assert isinstance(info.value.__cause__, MissingCredentialsError)


def test_load_dds_creds_prefers_ingest_variables() -> None:
    env = {"_DREMIO_USER": "a", "_DREMIO_PWD": "1", "DREMIO_USERNAME": "b", "DREMIO_TOKEN": "2"}
    assert load_dds_creds(env).username == "a"
    assert load_dds_creds({"DREMIO_USERNAME": "b", "DREMIO_TOKEN": "2"}).username == "b"
    with pytest.raises(MissingCredentialsError):
        load_dds_creds({})


# -- DDS_BASE_URL from .env -------------------------------------------------


def test_dotenv_dds_url_wins_over_kernel_env(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "# comment\nexport DDS_BASE_URL='https://dds.from-dotenv.test/'\nOTHER=x\n"
    )
    session = _metadata_session(dotenv_path=dotenv)

    assert session.dds_base_url() == f"https://dds.from-dotenv.test  (from {dotenv})"


@respx.mock
def test_push_goes_to_the_dotenv_url(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=https://dds.from-dotenv.test\n")
    url = f"https://dds.from-dotenv.test/api/v1/files/{TARGET}"
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    respx.get(url).mock(return_value=not_found())
    put = respx.put(url).mock(return_value=httpx.Response(201))
    sdi = SdiSession(env=ENV)
    sdi.get_xml(UUID)

    _metadata_session(
        sdi,
        dotenv_path=tmp_path / ".env",
        env={**ENV, "_DREMIO_USER": "a", "_DREMIO_PWD": "p"},
    ).push_to_dds("water")

    assert put.called


def test_empty_dotenv_value_falls_back_to_kernel_env(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=\n")
    session = _metadata_session(dotenv_path=tmp_path / ".env")

    assert session.dds_base_url() == f"{DDS_URL}  (from the kernel environment)"


def test_no_dds_url_anywhere_names_both_places(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("OTHER=x\n")
    session = _metadata_session(dotenv_path=tmp_path / ".env", env={})

    with pytest.raises(MetadataSessionError, match=r"\.env has none and it is not set"):
        session.dds_base_url()


def test_find_dotenv_walks_up_from_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=https://dds.parent.test\n")
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    assert find_dotenv() == tmp_path / ".env"
    assert MetadataSession(env={}).dds_base_url().startswith("https://dds.parent.test")


def test_read_dotenv_value_last_assignment_wins(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text('DDS_BASE_URL=one\nDDS_BASE_URL="two"\n')

    assert read_dotenv_value(dotenv, "DDS_BASE_URL") == "two"
    assert read_dotenv_value(dotenv, "MISSING") is None
