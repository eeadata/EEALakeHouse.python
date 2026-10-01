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
    DREMIO_URL,
    SDI_URL,
    UUID,
    catalog_url,
    dds_file_url,
    folder_entity,
    iso_xml,
    not_found,
    record_url,
)

TARGET = f"water/metadata/{UUID}.xml"
ENV = {"SDI_API_URL": SDI_URL, "DDS_BASE_URL": DDS_URL, "DREMIO_BASE_URL": DREMIO_URL}
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
    lookup = respx.get(catalog_url("water")).mock(return_value=folder_entity())
    sdi = SdiSession(env=ENV)
    metadata = _metadata_session(
        sdi, env={**ENV, "DREMIO_USERNAME": "carol", "DREMIO_TOKEN": "pat-c"}
    )

    sdi.get_xml(UUID)
    result = metadata.push_to_dds("water")

    assert (result.action, result.dds_path) == ("uploaded", TARGET)
    assert put.calls.last.request.headers["Authorization"] == "Bearer pat-c"
    assert lookup.calls.last.request.headers["Authorization"] == "Bearer pat-c"


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
    ).push_to_dds("water", check_catalog=False)

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


# -- Dremio catalog check ---------------------------------------------------

IDENTITY = {"_DREMIO_USER": "a", "_DREMIO_PWD": "p"}


@respx.mock
def test_push_refuses_a_path_missing_from_the_catalog() -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=iso_xml()))
    respx.get(catalog_url("catalog/water")).mock(return_value=httpx.Response(404))
    dds = respx.route(host="dds.example.test")
    sdi = SdiSession(env=ENV)
    sdi.get_xml(UUID)

    with pytest.raises(MetadataSessionError, match=r"catalog\.water does not exist"):
        _metadata_session(sdi, env={**ENV, **IDENTITY}).push_to_dds("catalog/water")

    assert not dds.called  # nothing reached DDS


@respx.mock
def test_check_catalog_path_converts_slashes_to_catalog_format() -> None:
    lookup = respx.get(catalog_url("catalog/bathing%20water/v1.0")).mock(
        return_value=folder_entity()
    )
    session = _metadata_session(env={**ENV, **IDENTITY})

    assert session.check_catalog_path("catalog/bathing water/v1.0") == (
        'catalog."bathing water"."v1.0" exists in the Dremio catalog'
    )
    assert lookup.called


@respx.mock
def test_catalog_source_400_for_a_missing_item_reads_as_absent() -> None:
    # What the EEA Dremio answers for a missing folder under its `catalog` source.
    message = (
        "Can not get internal item from non-filesystem source [catalog] of type "
        "[com.dremio.plugins.dremiocatalog.store.DremioCatalogLocalPlugin]"
    )
    respx.get(catalog_url("catalog/water/nope")).mock(
        return_value=httpx.Response(400, json={"errorMessage": message, "moreInfo": ""})
    )

    with pytest.raises(MetadataSessionError, match=r"catalog\.water\.nope does not exist"):
        _metadata_session(env={**ENV, **IDENTITY}).check_catalog_path("catalog/water/nope")


@respx.mock
def test_catalog_lookup_failure_is_an_error_not_absent() -> None:
    respx.get(catalog_url("catalog/water")).mock(return_value=httpx.Response(400, text="bad"))

    with pytest.raises(MetadataSessionError, match="could not check catalog.water"):
        _metadata_session(env={**ENV, **IDENTITY}).check_catalog_path("catalog.water")


def test_catalog_check_needs_a_dremio_url() -> None:
    env = {"DDS_BASE_URL": DDS_URL, **IDENTITY}

    with pytest.raises(MetadataSessionError, match="checking the Dremio catalog needs DREMIO_BASE"):
        _metadata_session(env=env).check_catalog_path("catalog.water")


def test_dremio_url_comes_from_dotenv_first(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text("DREMIO_BASE_URL=https://dremio.from-dotenv.test/\n")

    assert _metadata_session(dotenv_path=dotenv).dremio_base_url() == (
        f"https://dremio.from-dotenv.test  (from {dotenv})"
    )
