from __future__ import annotations

import httpx
import pytest

from eea_datalakehouse.dds_documents import DocumentsClient
from eea_datalakehouse.dds_ingestion.credentials import DremioCreds
from eea_datalakehouse.sdi import SdiCatalogue, SdiConfig, SdiController

SDI_URL = "https://sdi.example.test/catalogue"
API = f"{SDI_URL}/srv/api"
DDS_URL = "https://dds.example.test"
UUID = "070d9baa-448d-4168-8514-7dadb3ad876d"
SERIES_UUID = "c3858959-90da-4c1b-b9ca-492db0e514df"

_NAMESPACES = (
    'xmlns:mdb="http://standards.iso.org/iso/19115/-3/mdb/2.0" '
    'xmlns:mcc="http://standards.iso.org/iso/19115/-3/mcc/1.0" '
    'xmlns:mri="http://standards.iso.org/iso/19115/-3/mri/1.0" '
    'xmlns:cit="http://standards.iso.org/iso/19115/-3/cit/2.0" '
    'xmlns:gco="http://standards.iso.org/iso/19115/-3/gco/1.0"'
)


def iso_xml(
    uuid: str = UUID,
    *,
    revised: str = "2026-09-25T07:36:46.809835Z",
    title: str = "Bathing Water Directive - Status of bathing water, 2025 v.1.0",
    children: tuple[str, ...] = (),
    scope: str = "nonGeographicDataset",
) -> bytes:
    """A trimmed ISO 19115-3 record with the same structure SDI serves."""
    assoc = "".join(
        f"""
      <mri:associatedResource><mri:MD_AssociatedResource>
        <mri:associationType>
          <mri:DS_AssociationTypeCode codeListValue="isComposedOf" />
        </mri:associationType>
        <mri:metadataReference uuidref="{child}" />
      </mri:MD_AssociatedResource></mri:associatedResource>"""
        for child in children
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<mdb:MD_Metadata {_NAMESPACES}>
  <mdb:metadataIdentifier><mcc:MD_Identifier>
    <mcc:code><gco:CharacterString>{uuid}</gco:CharacterString></mcc:code>
  </mcc:MD_Identifier></mdb:metadataIdentifier>
  <mdb:metadataScope><mdb:MD_MetadataScope><mdb:resourceScope>
    <mcc:MD_ScopeCode codeListValue="{scope}" />
  </mdb:resourceScope></mdb:MD_MetadataScope></mdb:metadataScope>
  <mdb:dateInfo><cit:CI_Date>
    <cit:date><gco:DateTime>2026-06-01T07:54:39.676514Z</gco:DateTime></cit:date>
    <cit:dateType><cit:CI_DateTypeCode codeListValue="creation" /></cit:dateType>
  </cit:CI_Date></mdb:dateInfo>
  <mdb:dateInfo><cit:CI_Date>
    <cit:date><gco:DateTime>{revised}</gco:DateTime></cit:date>
    <cit:dateType><cit:CI_DateTypeCode codeListValue="revision" /></cit:dateType>
  </cit:CI_Date></mdb:dateInfo>
  <mdb:identificationInfo><mri:MD_DataIdentification>
    <mri:citation><cit:CI_Citation>
      <cit:title><gco:CharacterString>{title}</gco:CharacterString></cit:title>
      <cit:edition><gco:CharacterString>01.00</gco:CharacterString></cit:edition>
    </cit:CI_Citation></mri:citation>{assoc}
  </mri:MD_DataIdentification></mdb:identificationInfo>
</mdb:MD_Metadata>
""".encode()


def search_hit(uuid: str, *, superseded: bool, edition: str = "01.00") -> dict[str, object]:
    source: dict[str, object] = {
        "uuid": uuid,
        "resourceTitleObject": {"default": f"release {uuid}"},
        "resourceEdition": edition,
        "publicationDateForResource": ["2026-06-02"],
    }
    if superseded:
        source["cl_status"] = [{"key": "superseded", "default": "Superseded"}]
    return {"_source": source}


def record_url(uuid: str = UUID) -> str:
    return f"{API}/records/{uuid}/formatters/xml"


def dds_file_url(path: str) -> str:
    return f"{DDS_URL}/api/v1/files/{path}"


@pytest.fixture
def creds() -> DremioCreds:
    return DremioCreds(username="alice", _password="s3cr3t-pwd")


@pytest.fixture
def sdi(creds: DremioCreds) -> SdiController:
    return SdiController(
        SdiCatalogue(SdiConfig(api_url=SDI_URL)),
        DocumentsClient(DDS_URL, creds),
    )


def not_found() -> httpx.Response:
    return httpx.Response(404, json={"error": "not_found", "message": "no such file"})
