from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from eea_datalakehouse.dds_documents import (
    DdsController,
    DdsFile,
    DdsFilesExistError,
    DdsFolder,
    DdsPushError,
    DocumentsClient,
)
from eea_datalakehouse.dds_documents.controller import collect_files

from .conftest import DATASET, TARGET, file_url, not_found


@pytest.fixture
def dds(documents: DocumentsClient) -> DdsController:
    return DdsController(documents)


@pytest.fixture
def files(tmp_path: Path) -> Path:
    (tmp_path / "report.pdf").write_bytes(b"%PDF-1")
    (tmp_path / "annex.xlsx").write_bytes(b"PK-xlsx")
    (tmp_path / "notes.txt").write_text("hello")
    (tmp_path / "sub").mkdir()
    return tmp_path


# -- folders and target -------------------------------------------------------


@pytest.mark.parametrize("value", [DdsFolder.METADATA, "METADATA", "metadata", " Metadata/ "])
def test_folder_parses_a_member_or_its_name_in_any_case(value: DdsFolder | str) -> None:
    assert DdsFolder.parse(value) is DdsFolder.METADATA


def test_unknown_folder_lists_the_valid_ones() -> None:
    with pytest.raises(ValueError, match="use one of METADATA, ASSESSMENTS, DELIVERABLES"):
        DdsFolder.parse("reports")


@pytest.mark.parametrize(
    ("dds_path", "folder", "expected"),
    [
        (DATASET, DdsFolder.DELIVERABLES, TARGET),
        ("catalog/water/bwd/", "assessments", "catalog/water/bwd/assessments"),
        ('catalog."bathing water"."v1.0"', "METADATA", "catalog/bathing water/v1.0/metadata"),
    ],
)
def test_target(dds_path: str, folder: str, expected: str) -> None:
    assert DdsController.target(dds_path, folder) == expected


@pytest.mark.parametrize(("dds_path", "message"), [("", "empty"), ("catalog", "catalog root")])
def test_target_needs_a_dataset_folder(dds_path: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        DdsController.target(dds_path, DdsFolder.METADATA)


# -- collecting files ---------------------------------------------------------


def test_collect_reads_paths_globs_and_in_memory_files(files: Path) -> None:
    class Record:  # what SdiMetadata offers
        filename = "u-1.xml"
        content = b"<x/>"
        content_type = "application/xml"

    collected = collect_files(
        [str(files / "report.pdf"), files / "*.xlsx", DdsFile("a.csv", b"1,2"), Record()]
    )

    assert [(f.name, f.data) for f in collected] == [
        ("report.pdf", b"%PDF-1"),
        ("annex.xlsx", b"PK-xlsx"),
        ("a.csv", b"1,2"),
        ("u-1.xml", b"<x/>"),
    ]
    assert collected[3].content_type == "application/xml"


def test_collect_takes_a_single_file(files: Path) -> None:
    assert [f.name for f in collect_files(str(files / "notes.txt"))] == ["notes.txt"]


def test_a_glob_skips_folders(files: Path) -> None:
    names = [f.name for f in collect_files(str(files / "*"))]
    assert names == ["annex.xlsx", "notes.txt", "report.pdf"]


@pytest.mark.parametrize(
    ("make", "error", "message"),
    [
        (lambda d: [], ValueError, "no files to push"),
        (lambda d: None, ValueError, "no files to push"),
        (lambda d: str(d / "missing.pdf"), ValueError, "no such file"),
        (lambda d: str(d / "*.doc"), ValueError, "no file matches"),
        (lambda d: str(d / "sub"), ValueError, "is a folder"),
        (lambda d: [str(d / "notes.txt"), DdsFile("notes.txt", b"")], ValueError, "several"),
        (lambda d: DdsFile("a:b.txt", b""), ValueError, "not a valid file name"),
        (lambda d: b"raw bytes", TypeError, "cannot push a bytes"),
        (lambda d: 42, TypeError, "int"),
    ],
)
def test_collect_rejects(files: Path, make: object, error: type[Exception], message: str) -> None:
    with pytest.raises(error, match=message):
        collect_files(make(files))  # type: ignore[operator]


# -- push -------------------------------------------------------------------


@respx.mock
def test_push_puts_every_file_with_its_content_type(dds: DdsController, files: Path) -> None:
    pdf = respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))
    txt = respx.put(file_url(f"{TARGET}/notes.txt")).mock(return_value=httpx.Response(201))

    report = dds.push(DATASET, "DELIVERABLES", [files / "report.pdf", files / "notes.txt"])

    assert report.paths == (f"{TARGET}/report.pdf", f"{TARGET}/notes.txt")
    assert report.target == TARGET
    assert pdf.calls.last.request.content == b"%PDF-1"
    assert pdf.calls.last.request.headers["Content-Type"] == "application/pdf"
    assert pdf.calls.last.request.url.params["overwrite"] == "true"
    assert txt.calls.last.request.headers["Content-Type"] == "text/plain"
    assert repr(report).startswith(f"pushed 2 file(s) to {TARGET}:")


@respx.mock
def test_an_unknown_extension_goes_as_octet_stream(dds: DdsController) -> None:
    route = respx.put(file_url(f"{TARGET}/data.zzz")).mock(return_value=httpx.Response(201))

    dds.push(DATASET, DdsFolder.DELIVERABLES, DdsFile("data.zzz", b"?"))

    assert route.calls.last.request.headers["Content-Type"] == "application/octet-stream"


@respx.mock
def test_no_overwrite_uploads_nothing_if_any_file_exists(dds: DdsController, files: Path) -> None:
    respx.get(file_url(f"{TARGET}/report.pdf")).mock(return_value=not_found())
    respx.get(file_url(f"{TARGET}/notes.txt")).mock(
        return_value=httpx.Response(200, content=b"old")
    )
    put = respx.put(url__startswith=file_url(TARGET))

    with pytest.raises(DdsFilesExistError, match="notes.txt; pass overwrite=True") as info:
        dds.push(
            DATASET, "deliverables", [files / "report.pdf", files / "notes.txt"], overwrite=False
        )

    assert info.value.existing == (f"{TARGET}/notes.txt",)
    assert not put.called


@respx.mock
def test_no_overwrite_pushes_new_files_without_overwrite(dds: DdsController, files: Path) -> None:
    respx.get(file_url(f"{TARGET}/report.pdf")).mock(return_value=not_found())
    put = respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))

    dds.push(DATASET, "deliverables", files / "report.pdf", overwrite=False)

    assert put.calls.last.request.url.params["overwrite"] == "false"


@respx.mock
def test_a_failure_part_way_names_what_was_uploaded(dds: DdsController, files: Path) -> None:
    respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))
    respx.put(file_url(f"{TARGET}/notes.txt")).mock(
        return_value=httpx.Response(500, json={"message": "disk full"})
    )

    with pytest.raises(DdsPushError, match="disk full") as info:
        dds.push(DATASET, "deliverables", [files / "report.pdf", files / "notes.txt"])

    assert info.value.uploaded == (f"{TARGET}/report.pdf",)
    assert f"already uploaded: {TARGET}/report.pdf" in str(info.value)


def test_bad_input_uploads_nothing(dds: DdsController, files: Path) -> None:
    with respx.mock(assert_all_called=False) as mock:
        put = mock.put(url__startswith=file_url(TARGET))
        with pytest.raises(ValueError, match="no such file"):
            dds.push(DATASET, "deliverables", [files / "report.pdf", files / "nope.pdf"])

    assert not put.called


def test_repr_hides_credentials(dds: DdsController) -> None:
    assert "s3cr3t" not in repr(dds)
