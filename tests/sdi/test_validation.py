from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from eea_datalakehouse.sdi import SdiSession, SdiSessionError, SdiValidation
from eea_datalakehouse.sdi.controller import SdiController, SdiMetadata
from eea_datalakehouse.sdi.validation import validate

from .conftest import SDI_URL, UUID, record_url

_CODELISTS = "http://standards.iso.org/iso/19115/resources/Codelists/cat/codelists.xml"


def valid_xml(*, contact: bool = True, abstract: bool = True) -> bytes:
    """The smallest ISO 19115-3 record the XSD accepts, plus a title."""
    contact_xml = f"""
  <mdb:contact><cit:CI_Responsibility>
    <cit:role><cit:CI_RoleCode codeList="{_CODELISTS}#CI_RoleCode"
      codeListValue="pointOfContact" /></cit:role>
    <cit:party><cit:CI_Organisation>
      <cit:name><gco:CharacterString>EEA</gco:CharacterString></cit:name>
    </cit:CI_Organisation></cit:party>
  </cit:CI_Responsibility></mdb:contact>"""
    abstract_xml = (
        "\n    <mri:abstract><gco:CharacterString>About</gco:CharacterString></mri:abstract>"
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<mdb:MD_Metadata xmlns:mdb="http://standards.iso.org/iso/19115/-3/mdb/2.0"
    xmlns:mcc="http://standards.iso.org/iso/19115/-3/mcc/1.0"
    xmlns:mri="http://standards.iso.org/iso/19115/-3/mri/1.0"
    xmlns:cit="http://standards.iso.org/iso/19115/-3/cit/2.0"
    xmlns:gco="http://standards.iso.org/iso/19115/-3/gco/1.0">
  <mdb:metadataIdentifier><mcc:MD_Identifier>
    <mcc:code><gco:CharacterString>{UUID}</gco:CharacterString></mcc:code>
  </mcc:MD_Identifier></mdb:metadataIdentifier>{contact_xml if contact else ""}
  <mdb:dateInfo><cit:CI_Date>
    <cit:date><gco:DateTime>2026-06-01T07:54:39Z</gco:DateTime></cit:date>
    <cit:dateType><cit:CI_DateTypeCode codeList="{_CODELISTS}#CI_DateTypeCode"
      codeListValue="creation" /></cit:dateType>
  </cit:CI_Date></mdb:dateInfo>
  <mdb:identificationInfo><mri:MD_DataIdentification>
    <mri:citation><cit:CI_Citation>
      <cit:title><gco:CharacterString>Bathing water</gco:CharacterString></cit:title>
    </cit:CI_Citation></mri:citation>{abstract_xml if abstract else ""}
  </mri:MD_DataIdentification></mdb:identificationInfo>
</mdb:MD_Metadata>
""".encode()


@pytest.fixture
def record_file(tmp_path: Path) -> Path:
    path = tmp_path / f"{UUID}.xml"
    path.write_bytes(valid_xml())
    return path


# -- validate -------------------------------------------------------------------


def test_valid_file_is_truthy(record_file: Path) -> None:
    result = validate(str(record_file))

    assert result and result.valid
    assert result.issues == ()
    assert repr(result) == f"{record_file} is valid ISO 19115-3 (XSD)"


def test_path_like_and_bytes_are_accepted(record_file: Path) -> None:
    assert validate(record_file).valid
    assert validate(valid_xml()).source == "XML"
    assert validate(bytearray(valid_xml())).valid


def test_schema_error_comes_back_with_line_and_path() -> None:
    result = validate(valid_xml(contact=False))

    assert not result
    (issue,) = result.issues
    assert issue.line == 10
    assert "Element 'mdb:dateInfo': This element is not expected" in issue.message
    assert "mdb:contact" in issue.message  # prefixes, not {namespace URI}s
    assert issue.path == "/mdb:MD_Metadata/mdb:dateInfo"
    assert repr(result).startswith("XML is NOT valid ISO 19115-3 (XSD): 1 issue(s)\n  line 10: ")


def test_errors_in_different_elements_are_all_listed() -> None:
    xml = valid_xml(abstract=False).replace(f'codeList="{_CODELISTS}#CI_RoleCode"'.encode(), b"")

    role, abstract = validate(xml).issues

    assert "Element 'cit:CI_RoleCode': The attribute 'codeList' is required" in role.message
    assert "Missing child element(s). Expected is ( mri:abstract )" in abstract.message


def test_missing_code_list_attribute_is_an_issue() -> None:
    xml = valid_xml().replace(f'codeList="{_CODELISTS}#CI_DateTypeCode"'.encode(), b"")

    (issue,) = validate(xml).issues

    assert "Element 'cit:CI_DateTypeCode': The attribute 'codeList' is required" in issue.message


def test_not_well_formed_xml_is_an_issue_not_an_exception() -> None:
    (issue,) = validate(b"<metadata>\n<broken").issues

    assert issue.line == 2
    assert issue.message.startswith("not well-formed XML: ")
    assert "column" not in issue.message  # the line is already in the issue


def test_other_root_element_is_named() -> None:
    xml = b'<gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"/>'

    (issue,) = validate(xml).issues

    assert (
        issue.message == "root element is gmd:MD_Metadata, expected mdb:MD_Metadata (ISO 19115-3)"
    )


def test_external_entities_are_not_read(tmp_path: Path) -> None:
    secret = tmp_path / "secret.txt"
    secret.write_text("TOP-SECRET")
    xml = (
        valid_xml()
        .replace(
            b"<mdb:MD_Metadata",
            f'<!DOCTYPE mdb:MD_Metadata [<!ENTITY x SYSTEM "{secret.as_uri()}">]>\n'
            "<mdb:MD_Metadata".encode(),
            1,
        )
        .replace(b"About", b"&x;")
    )

    result = validate(xml)

    assert not result
    assert "TOP-SECRET" not in repr(result)
    assert result.issues[0].message == "entity &x; is not allowed (it is not expanded)"


def test_missing_file_and_folder_raise(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="no file"):
        validate(tmp_path / "nope.xml")
    with pytest.raises(IsADirectoryError, match="is a folder"):
        validate(tmp_path)


def test_other_types_are_refused() -> None:
    with pytest.raises(TypeError, match="not int"):
        validate(42)


# -- SdiController / SdiSession -------------------------------------------------


@respx.mock
def test_controller_validates_a_get_xml_record(sdi: SdiController) -> None:
    respx.get(record_url()).mock(return_value=httpx.Response(200, content=valid_xml()))

    record = sdi.get_xml(UUID)
    result = sdi.validate(record)

    assert isinstance(record, SdiMetadata)
    assert isinstance(result, SdiValidation)
    assert result.valid and result.source == UUID


def test_session_validate_needs_no_sdi_settings(record_file: Path) -> None:
    assert SdiSession(env={}).validate(str(record_file)).valid


def test_session_turns_a_missing_file_into_a_session_error(tmp_path: Path) -> None:
    with pytest.raises(SdiSessionError, match="no file") as info:
        SdiSession(env={"SDI_API_URL": SDI_URL}).validate(str(tmp_path / "nope.xml"))
    assert isinstance(info.value.__cause__, FileNotFoundError)
