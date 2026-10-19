from __future__ import annotations

import httpx
import pytest
import respx

from eea_datalakehouse.sdi import NotIso19115_3, SdiSession, SdiSessionError

from .conftest import SDI_URL, UUID, iso_xml, record_url

ENV = {"SDI_API_URL": SDI_URL}


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


def test_call_that_does_not_fit_the_signature_shows_the_usage() -> None:
    with pytest.raises(SdiSessionError) as info:
        SdiSession(env=ENV).get_xml(UUID, colour="red")  # type: ignore[call-arg]

    assert "got an unexpected keyword argument 'colour'" in str(info.value)
    assert str(info.value).endswith("usage: get_xml(uuid)")
