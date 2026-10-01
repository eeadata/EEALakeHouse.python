from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from eea_datalakehouse.dds_ingestion.credentials import MissingCredentialsError
from eea_datalakehouse.sdi import NotIso19115_3, SdiController, SdiSession, SdiSessionError
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


@respx.mock
def test_get_xml_then_push_uses_the_last_record_and_kernel_env() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    respx.get(dds_file_url(TARGET)).mock(return_value=not_found())
    put = respx.put(dds_file_url(TARGET)).mock(return_value=httpx.Response(201))
    session = SdiSession(
        dotenv_path=NO_DOTENV, env={**ENV, "DREMIO_USERNAME": "carol", "DREMIO_TOKEN": "pat-c"}
    )

    metadata = session.get_xml(UUID)
    result = session.push_to_dds("water")

    assert session.last is metadata
    assert (result.action, result.dds_path) == ("uploaded", TARGET)
    assert put.calls.last.request.headers["Authorization"] == "Bearer pat-c"
    assert repr(session) == f"SdiSession(last={UUID!r})"


@respx.mock
def test_get_xml_needs_no_dds_settings() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))

    session = SdiSession(dotenv_path=NO_DOTENV, env={"SDI_API_URL": SDI_URL})
    assert session.get_xml(UUID).uuid == UUID


def test_push_before_get_xml_is_a_session_error() -> None:
    with pytest.raises(SdiSessionError, match="run get_xml"):
        SdiSession(dotenv_path=NO_DOTENV, env=ENV).push_to_dds("water")


@respx.mock
def test_push_without_dremio_identity_is_a_session_error() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    session = SdiSession(dotenv_path=NO_DOTENV, env=ENV)
    session.get_xml(UUID)

    with pytest.raises(SdiSessionError, match="DREMIO_TOKEN") as info:
        session.push_to_dds("water")
    assert isinstance(info.value.__cause__, MissingCredentialsError)


@respx.mock
def test_sdi_errors_become_session_errors() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, text="<html>"))

    with pytest.raises(SdiSessionError) as info:
        SdiSession(dotenv_path=NO_DOTENV, env=ENV).get_xml(UUID)
    assert isinstance(info.value.__cause__, NotIso19115_3)


def test_given_controller_is_used_as_is(sdi: SdiController) -> None:
    session = SdiSession(controller=sdi)
    assert session._sdi(with_dds=True) is sdi


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
    session = SdiSession(dotenv_path=dotenv, env=ENV)

    assert session.dds_base_url() == f"https://dds.from-dotenv.test  (from {dotenv})"


@respx.mock
def test_push_goes_to_the_dotenv_url(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=https://dds.from-dotenv.test\n")
    url = f"https://dds.from-dotenv.test/api/v1/files/{TARGET}"
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    respx.get(url).mock(return_value=not_found())
    put = respx.put(url).mock(return_value=httpx.Response(201))
    session = SdiSession(
        dotenv_path=tmp_path / ".env", env={**ENV, "_DREMIO_USER": "a", "_DREMIO_PWD": "p"}
    )

    session.get_xml(UUID)
    session.push_to_dds("water")

    assert put.called


def test_empty_dotenv_value_falls_back_to_kernel_env(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=\n")
    session = SdiSession(dotenv_path=tmp_path / ".env", env=ENV)

    assert session.dds_base_url() == f"{DDS_URL}  (from the kernel environment)"


def test_no_dds_url_anywhere_names_both_places(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("OTHER=x\n")
    session = SdiSession(dotenv_path=tmp_path / ".env", env={})

    with pytest.raises(SdiSessionError, match=r"\.env has none and it is not set in the kernel"):
        session.dds_base_url()


def test_find_dotenv_walks_up_from_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text("DDS_BASE_URL=https://dds.parent.test\n")
    nested = tmp_path / "a" / "b"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    assert find_dotenv() == tmp_path / ".env"
    assert SdiSession(env={}).dds_base_url().startswith("https://dds.parent.test")


def test_read_dotenv_value_last_assignment_wins(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text('DDS_BASE_URL=one\nDDS_BASE_URL="two"\n')

    assert read_dotenv_value(dotenv, "DDS_BASE_URL") == "two"
    assert read_dotenv_value(dotenv, "MISSING") is None
