from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from eea_datalakehouse.dds_documents import (
    DdsController,
    DdsFile,
    DdsFileNotFoundError,
    DdsFilesExistError,
    DdsFolder,
    DdsPushError,
    DocumentsClient,
)
from eea_datalakehouse.dds_documents.controller import (
    closest_folder,
    collect_files,
    folder_name,
    matches,
    name_patterns,
)

from .conftest import DATASET, DDS_URL, TARGET, file_url, list_url, not_found

VALID = (
    "valid folders are DERIVED, METADATA, METHODOLOGY, ASSESSMENTS, ANALYSES, REFERENCES, "
    "DELIVERABLES, OTHERS"
)


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


def test_the_eight_built_in_folders_match_dds() -> None:
    assert [m.value for m in DdsFolder] == [
        "derived",
        "metadata",
        "methodology",
        "assessments",
        "analyses",
        "references",
        DdsFolder.DELIVERABLES,
        "others",
    ]


def test_folder_name_is_the_members_value() -> None:
    assert folder_name(DdsFolder.METADATA) == "metadata"


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ("metadata", "not str 'metadata' \\(did you mean METADATA, without quotes\\?\\); "),
        ("METADATA", "did you mean METADATA, without quotes"),
        ("deliverable", "did you mean DELIVERABLES, without quotes"),
        ("maps", "folder must be a DdsFolder, not str 'maps'; "),
        (None, "not NoneType None"),
    ],
)
def test_folder_must_be_a_dds_folder(value: object, message: str) -> None:
    with pytest.raises(TypeError, match=message) as raised:
        folder_name(value)  # type: ignore[arg-type]
    assert str(raised.value).endswith(VALID)


def test_an_unknown_folder_value_lists_the_valid_ones() -> None:
    with pytest.raises(ValueError, match="'maps' is not a DDS folder") as raised:
        DdsFolder("maps")
    assert str(raised.value).endswith(VALID)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("deliverables", DdsFolder.DELIVERABLES),
        ("Deliverable", DdsFolder.DELIVERABLES),
        ("/metadata/", DdsFolder.METADATA),
        ("analyses", DdsFolder.ANALYSES),
        ("maps", None),
    ],
)
def test_closest_folder(text: str, expected: DdsFolder | None) -> None:
    assert closest_folder(text) is expected


@pytest.mark.parametrize(
    ("dds_path", "folder", "expected"),
    [
        (DATASET, DdsFolder.DELIVERABLES, TARGET),
        ("catalog/water/bwd/", DdsFolder.ASSESSMENTS, "catalog/water/bwd/assessments"),
        (
            'catalog."bathing water"."v1.0"',
            DdsFolder.METADATA,
            "catalog/bathing water/v1.0/metadata",
        ),
    ],
)
def test_target(dds_path: str, folder: DdsFolder, expected: str) -> None:
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
        (lambda d: True, TypeError, "for overwrite, write overwrite=True"),
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

    report = dds.push(DATASET, DdsFolder.DELIVERABLES, [files / "report.pdf", files / "notes.txt"])

    assert report.paths == (f"{TARGET}/report.pdf", f"{TARGET}/notes.txt")
    assert report.target == TARGET
    assert pdf.calls.last.request.content == b"%PDF-1"
    assert pdf.calls.last.request.headers["Content-Type"] == "application/pdf"
    assert pdf.calls.last.request.headers["X-DDS-Overwrite"] == "true"
    assert txt.calls.last.request.headers["Content-Type"] == "text/plain"
    assert repr(report).startswith(f"pushed 2 file(s) to {TARGET}:")


@respx.mock
def test_an_unknown_extension_goes_as_octet_stream(dds: DdsController) -> None:
    route = respx.put(file_url(f"{TARGET}/data.zzz")).mock(return_value=httpx.Response(201))

    dds.push(DATASET, DdsFolder.DELIVERABLES, DdsFile("data.zzz", b"?"))

    assert route.calls.last.request.headers["Content-Type"] == "application/octet-stream"


@respx.mock
def test_no_overwrite_uploads_nothing_if_any_file_exists(dds: DdsController, files: Path) -> None:
    respx.head(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(404))
    respx.head(file_url(f"{TARGET}/notes.txt")).mock(return_value=httpx.Response(200))
    put = respx.put(url__startswith=file_url(TARGET))

    with pytest.raises(DdsFilesExistError, match="notes.txt; pass overwrite=True") as info:
        dds.push(
            DATASET,
            DdsFolder.DELIVERABLES,
            [files / "report.pdf", files / "notes.txt"],
            overwrite=False,
        )

    assert info.value.existing == (f"{TARGET}/notes.txt",)
    assert not put.called


@respx.mock
def test_no_overwrite_pushes_new_files_without_overwrite(dds: DdsController, files: Path) -> None:
    respx.head(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(404))
    put = respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))

    dds.push(DATASET, DdsFolder.DELIVERABLES, files / "report.pdf", overwrite=False)

    assert put.calls.last.request.headers["X-DDS-Overwrite"] == "false"


def test_a_string_folder_uploads_nothing(dds: DdsController, files: Path) -> None:
    with respx.mock(assert_all_called=False) as mock:
        put = mock.put(url__startswith=file_url("catalog"))
        with pytest.raises(TypeError, match="did you mean DELIVERABLES"):
            dds.push(DATASET, "deliverables", files / "notes.txt")  # type: ignore[arg-type]

    assert not put.called


@respx.mock
def test_a_failure_part_way_names_what_was_uploaded(dds: DdsController, files: Path) -> None:
    respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))
    respx.put(file_url(f"{TARGET}/notes.txt")).mock(
        return_value=httpx.Response(500, json={"message": "disk full"})
    )

    with pytest.raises(DdsPushError, match="disk full") as info:
        dds.push(DATASET, DdsFolder.DELIVERABLES, [files / "report.pdf", files / "notes.txt"])

    assert info.value.uploaded == (f"{TARGET}/report.pdf",)
    assert f"already uploaded: {TARGET}/report.pdf" in str(info.value)


def test_bad_input_uploads_nothing(dds: DdsController, files: Path) -> None:
    with respx.mock(assert_all_called=False) as mock:
        put = mock.put(url__startswith=file_url(TARGET))
        with pytest.raises(ValueError, match="no such file"):
            dds.push(DATASET, DdsFolder.DELIVERABLES, [files / "report.pdf", files / "nope.pdf"])

    assert not put.called


def test_repr_hides_credentials(dds: DdsController) -> None:
    assert "s3cr3t" not in repr(dds)


# -- list ---------------------------------------------------------------------


@respx.mock
@pytest.mark.parametrize(
    "paths",
    [
        [f"{TARGET}/report.pdf", f"{TARGET}/annex/a.xlsx", f"/{TARGET}/notes.txt"],
        ["report.pdf", "annex/a.xlsx", "notes.txt"],  # names relative to the folder
    ],
)
def test_list_names_the_files_in_the_folder(dds: DdsController, paths: list[str]) -> None:
    route = respx.get(list_url(TARGET)).mock(
        return_value=httpx.Response(200, json={"files": [{"path": p} for p in paths]})
    )

    listing = dds.list(DATASET, DdsFolder.DELIVERABLES)

    assert route.calls.last.request.url.params["limit"] == "200"
    assert listing.target == TARGET
    assert listing.names == ("annex/a.xlsx", "notes.txt", "report.pdf")
    assert list(listing) == list(listing.names)
    assert len(listing) == 3 and listing[0] == "annex/a.xlsx" and "report.pdf" in listing
    assert repr(listing) == (f"3 file(s) in {TARGET}:\n  annex/a.xlsx\n  notes.txt\n  report.pdf")


@respx.mock
@pytest.mark.parametrize("response", [httpx.Response(200, json={"files": []}), not_found()])
def test_an_empty_or_missing_folder_lists_empty(
    dds: DdsController, response: httpx.Response
) -> None:
    respx.get(list_url("catalog/water/bwd/metadata")).mock(return_value=response)

    listing = dds.list(DATASET, DdsFolder.METADATA)

    assert listing.names == ()
    assert repr(listing) == "no files in catalog/water/bwd/metadata"


NAMES = ["Report_2025.PDF", "report_2024.pdf", "annex/a.xlsx", "annex/b.csv", "notes.txt"]


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        (None, sorted(NAMES)),
        ("", sorted(NAMES)),
        ("*.pdf", ["Report_2025.PDF", "report_2024.pdf"]),  # case ignored
        (".PDF", ["Report_2025.PDF", "report_2024.pdf"]),  # an extension
        ("pdf", ["Report_2025.PDF", "report_2024.pdf"]),  # text in the name
        ("report_202?.*", ["Report_2025.PDF", "report_2024.pdf"]),
        ("*2025*", ["Report_2025.PDF"]),
        ("[ab].*", ["annex/a.xlsx", "annex/b.csv"]),  # the part after the last /
        ("annex/*", ["annex/a.xlsx", "annex/b.csv"]),  # or the whole name
        ("annex", ["annex/a.xlsx", "annex/b.csv"]),
        ([".xlsx", "*.csv"], ["annex/a.xlsx", "annex/b.csv"]),  # any of a list
        (("notes*",), ["notes.txt"]),
        (".tx", []),  # an extension matches whole: .txt is not .tx
        ("*.docx", []),
    ],
)
@respx.mock
def test_list_pattern_keeps_the_matching_names(
    dds: DdsController, pattern: object, expected: list[str]
) -> None:
    respx.get(list_url(TARGET)).mock(return_value=httpx.Response(200, json=NAMES))

    listing = dds.list(DATASET, DdsFolder.DELIVERABLES, pattern)  # type: ignore[arg-type]

    assert list(listing) == expected


@respx.mock
def test_a_filtered_listing_says_what_it_matched(dds: DdsController) -> None:
    respx.get(list_url(TARGET)).mock(return_value=httpx.Response(200, json=NAMES))

    pdfs = dds.list(DATASET, DdsFolder.DELIVERABLES, ["*.pdf", " *.pdf ", ".docx"])
    none = dds.list(DATASET, DdsFolder.DELIVERABLES, "*.zip")

    assert pdfs.patterns == ("*.pdf", ".docx")
    assert repr(pdfs).startswith(f"2 file(s) matching '*.pdf' or '.docx' in {TARGET}:\n")
    assert repr(none) == f"no files matching '*.zip' in {TARGET}"


def test_name_patterns_need_strings() -> None:
    assert name_patterns(None) == ()
    assert name_patterns(" *.pdf ") == ("*.pdf",)
    with pytest.raises(TypeError, match="pattern must be a str or a list of str, not int"):
        name_patterns(["*.pdf", 3])  # type: ignore[list-item]


def test_matches_without_patterns_keeps_everything() -> None:
    assert matches("anything", ())


def test_list_needs_a_dds_folder_and_a_dataset(dds: DdsController) -> None:
    with respx.mock(assert_all_called=False) as mock:
        get = mock.get(url__startswith=DDS_URL)
        with pytest.raises(TypeError, match="did you mean DELIVERABLES") as raised:
            dds.list(DATASET, "deliverables")  # type: ignore[arg-type]
        assert str(raised.value).endswith(VALID)
        with pytest.raises(ValueError, match="catalog root"):
            dds.list("catalog", DdsFolder.DELIVERABLES)

    assert not get.called


# -- get ----------------------------------------------------------------------


@respx.mock
def test_get_returns_the_file_named_after_itself(dds: DdsController) -> None:
    route = respx.get(file_url(f"{TARGET}/annex/a.xlsx")).mock(
        return_value=httpx.Response(200, content=b"PK-xlsx")
    )

    got = dds.get(DATASET, DdsFolder.DELIVERABLES, "/annex/a.xlsx")

    assert got == DdsFile(
        "a.xlsx", b"PK-xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert route.calls.last.request.headers["Authorization"] == "Bearer s3cr3t-pwd"


def test_get_a_missing_file_is_an_error_without_listing_the_folder(dds: DdsController) -> None:
    with respx.mock(assert_all_called=False) as mock:
        mock.get(file_url(f"{TARGET}/annex/repot.pdf")).mock(return_value=not_found())
        listing = mock.get(url__startswith=f"{DDS_URL}/api/v1/files/{TARGET}/")

        with pytest.raises(DdsFileNotFoundError) as raised:
            dds.get(DATASET, DdsFolder.DELIVERABLES, "annex/repot.pdf")

    assert str(raised.value) == f"no file 'annex/repot.pdf' in {TARGET}"
    assert raised.value.path == f"{TARGET}/annex/repot.pdf"
    assert not listing.called


@pytest.mark.parametrize(
    ("folder", "filename", "error", "message"),
    [
        ("deliverables", "a.pdf", TypeError, "did you mean DELIVERABLES"),
        (DdsFolder.DELIVERABLES, "", ValueError, "must not be empty"),
        (DdsFolder.DELIVERABLES, "../x.pdf", ValueError, "must not be a path"),
        (DdsFolder.DELIVERABLES, "a?.pdf", ValueError, "not a valid file name"),
        (DdsFolder.DELIVERABLES, 3, TypeError, "must be a str"),
    ],
)
def test_get_bad_input_asks_dds_nothing(
    dds: DdsController, folder: object, filename: object, error: type[Exception], message: str
) -> None:
    with respx.mock(assert_all_called=False) as mock:
        route = mock.get(url__startswith=DDS_URL)
        with pytest.raises(error, match=message) as raised:
            dds.get(DATASET, folder, filename)  # type: ignore[arg-type]
        if not isinstance(folder, DdsFolder):
            assert str(raised.value).endswith(VALID)

    assert not route.called
