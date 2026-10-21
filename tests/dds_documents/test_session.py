from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from eea_datalakehouse.dds_documents import (
    DdsController,
    DdsFile,
    DdsFolder,
    DdsSession,
    DdsSessionError,
    DocumentsClient,
)
from eea_datalakehouse.dds_documents.session import load_dds_creds
from eea_datalakehouse.dds_ingestion.credentials import MissingCredentialsError
from eea_datalakehouse.session_utils import find_dotenv, read_dotenv_value

from .conftest import DATASET, DDS_URL, TARGET, file_url, list_url

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

    report = _session().push(DdsFolder.DELIVERABLES, REPORT)

    assert report.paths == (f"{TARGET}/report.pdf",)
    assert put.calls.last.request.headers["Authorization"] == "Bearer pat-a"


@respx.mock
def test_push_path_overrides_the_context() -> None:
    put = respx.put(file_url("catalog/air/aq/metadata/report.pdf")).mock(
        return_value=httpx.Response(201)
    )

    _session().push(DdsFolder.METADATA, REPORT, path="catalog/air/aq")

    assert put.called


def test_target_shows_the_folder_without_any_settings() -> None:
    assert _session(env={}).target(DdsFolder.DELIVERABLES) == TARGET
    assert _session(env={}).target(DdsFolder.METADATA, "catalog.air") == "catalog/air/metadata"


@pytest.mark.parametrize("context", [None, ""])
def test_no_context_and_no_path_is_a_session_error(context: str | None) -> None:
    with pytest.raises(DdsSessionError, match=r"%catalog use\(\.\.\.\) or pass path="):
        _session(context).push(DdsFolder.METADATA, REPORT)


def test_root_context_is_refused_before_any_settings() -> None:
    with pytest.raises(DdsSessionError, match="catalog root"):
        _session("catalog", env={}).push(DdsFolder.METADATA, REPORT)


@pytest.mark.parametrize("folder", ["metadata", "converted", 42])
def test_a_folder_that_is_not_a_dds_folder_is_refused_before_any_settings(folder: object) -> None:
    calls = [
        lambda session: session.push(folder, REPORT),
        lambda session: session.list(folder),
        lambda session: session.get(folder, "report.pdf"),
        lambda session: session.target(folder),
    ]
    for call in calls:
        with pytest.raises(DdsSessionError, match="folder must be a DdsFolder") as raised:
            call(_session(env={}))
        assert "valid folders are DERIVED, METADATA," in str(raised.value)
        assert str(raised.value).endswith("DELIVERABLES, OTHERS")


@respx.mock
def test_list_shows_the_files_in_the_context_folder() -> None:
    route = respx.get(list_url(TARGET)).mock(
        return_value=httpx.Response(
            200, json={"files": [{"path": f"{TARGET}/b.pdf"}, {"path": f"{TARGET}/a.pdf"}]}
        )
    )

    listing = _session().list(DdsFolder.DELIVERABLES)

    assert listing.names == ("a.pdf", "b.pdf")
    request = route.calls.last.request
    assert request.url.path == f"/api/v1/files/{TARGET}/"
    assert request.headers["Authorization"] == "Bearer pat-a"


@respx.mock
def test_list_path_overrides_the_context() -> None:
    route = respx.get(list_url("catalog/air/aq/metadata")).mock(
        return_value=httpx.Response(200, json={"files": []})
    )

    _session().list(DdsFolder.METADATA, path="catalog.air.aq")

    assert route.called


@respx.mock
def test_list_pattern_filters_the_names() -> None:
    respx.get(list_url(TARGET)).mock(
        return_value=httpx.Response(200, json=[f"{TARGET}/a.pdf", f"{TARGET}/b.docx"])
    )
    session = _session()

    assert list(session.list(DdsFolder.DELIVERABLES, "*.PDF")) == ["a.pdf"]
    assert list(session.list(DdsFolder.DELIVERABLES, [".pdf", ".docx"])) == ["a.pdf", "b.docx"]
    assert list(session.list(DdsFolder.DELIVERABLES, pattern="b")) == ["b.docx"]


@pytest.mark.parametrize(
    ("pattern", "message"),
    [(3, r"\(pattern\) must be str or list or tuple or None"), (["*.pdf", 3], "list of str")],
)
def test_a_bad_pattern_needs_no_settings(pattern: object, message: str) -> None:
    with pytest.raises(DdsSessionError, match=message):
        _session(env={}).list(DdsFolder.DELIVERABLES, pattern)  # type: ignore[arg-type]


def test_list_without_a_context_or_with_a_bad_path_is_a_session_error() -> None:
    with pytest.raises(DdsSessionError, match="no DDS target path"):
        _session(None, env={}).list(DdsFolder.METADATA)
    with pytest.raises(DdsSessionError, match="catalog root"):
        _session("catalog", env={}).list(DdsFolder.METADATA)
    with pytest.raises(DdsSessionError, match=r"\(path\) must be str"):
        _session(env={}).list(DdsFolder.METADATA, path=3)  # type: ignore[arg-type]


@respx.mock
def test_list_server_error_is_a_session_error() -> None:
    respx.get(list_url("catalog/water/bwd/metadata")).mock(
        return_value=httpx.Response(500, text="down")
    )

    with pytest.raises(DdsSessionError, match="500"):
        _session().list(DdsFolder.METADATA)


def test_a_missing_file_is_a_session_error(tmp_path: Path) -> None:
    with pytest.raises(DdsSessionError, match="no such file"):
        _session().push(DdsFolder.METADATA, str(tmp_path / "nope.xml"))


def test_push_without_dremio_identity_is_a_session_error() -> None:
    with pytest.raises(DdsSessionError, match="DREMIO_TOKEN") as info:
        _session(env={"DDS_BASE_URL": DDS_URL}).push(DdsFolder.METADATA, REPORT)
    assert isinstance(info.value.__cause__, MissingCredentialsError)


@pytest.mark.parametrize(
    ("args", "kwargs", "message"),
    [
        ((DdsFolder.METADATA, REPORT), {"path": 1}, r"argument 2 \(path\) must be str or None"),
        ((DdsFolder.METADATA, REPORT), {"overwrite": 1}, r"argument 3 \(overwrite\) must be bool"),
    ],
)
def test_push_checks_argument_types_strictly(
    args: tuple[object, ...], kwargs: dict[str, object], message: str
) -> None:
    with pytest.raises(DdsSessionError, match=message):
        _session(env={}).push(*args, **kwargs)  # type: ignore[arg-type]


def test_call_that_does_not_fit_the_signature_shows_the_usage() -> None:
    with pytest.raises(DdsSessionError) as info:
        _session().push(DdsFolder.METADATA, REPORT, force=True)  # type: ignore[call-arg]

    assert "got an unexpected keyword argument 'force'" in str(info.value)
    assert str(info.value).endswith(
        "usage: push(folder, files..., path=None, overwrite=True, target_name='')"
    )


@respx.mock
@pytest.mark.parametrize(
    "files",
    [
        ("a.txt", "b.txt"),
        (["a.txt", "b.txt"],),
        ("a.txt", ["b.txt"]),
    ],
)
def test_push_takes_several_files_in_one_call(
    files: tuple[object, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a.txt").write_text("a")
    (tmp_path / "b.txt").write_text("b")
    monkeypatch.chdir(tmp_path)
    respx.put(url__startswith=file_url(TARGET)).mock(return_value=httpx.Response(201))

    report = _session().push(DdsFolder.DELIVERABLES, *files)

    assert report.paths == (f"{TARGET}/a.txt", f"{TARGET}/b.txt")


@respx.mock
def test_target_name_renames_the_one_file() -> None:
    put = respx.put(file_url(f"{TARGET}/bwd_2025.xml")).mock(return_value=httpx.Response(201))

    report = _session().push(DdsFolder.DELIVERABLES, REPORT, target_name=" bwd_2025.xml ")

    assert report.paths == (f"{TARGET}/bwd_2025.xml",)
    assert put.calls.last.request.content == REPORT.data


def test_empty_target_name_keeps_the_file_name() -> None:
    with respx.mock:
        respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))
        assert _session().push(DdsFolder.DELIVERABLES, REPORT, target_name="").paths == (
            f"{TARGET}/report.pdf",
        )


@pytest.mark.parametrize(
    ("files", "target_name", "message"),
    [
        ((REPORT, DdsFile("b.pdf", b"")), "x.pdf", "as a str names one file, but 2 .* pass a dict"),
        ((REPORT,), "a/b.pdf", "must not be a path"),
        ((REPORT,), "a:b.pdf", "not a valid file name"),
    ],
)
def test_bad_target_name_uploads_nothing(
    files: tuple[object, ...], target_name: str, message: str
) -> None:
    with respx.mock(assert_all_called=False) as mock:
        put = mock.put(url__startswith=file_url(TARGET))
        with pytest.raises(DdsSessionError, match=message):
            _session().push(DdsFolder.DELIVERABLES, *files, target_name=target_name)
    assert not put.called


def test_target_name_must_be_a_str_or_dict() -> None:
    with pytest.raises(
        DdsSessionError, match=r"argument 4 \(target_name\) must be str or dict, not NoneType"
    ):
        _session(env={}).push(DdsFolder.METADATA, REPORT, target_name=None)  # type: ignore[arg-type]


@respx.mock
def test_a_dict_target_name_renames_several_files_and_keeps_the_rest() -> None:
    put = respx.put(url__startswith=file_url(TARGET)).mock(return_value=httpx.Response(201))
    files = (REPORT, DdsFile("annex.xlsx", b"x"), DdsFile("notes.txt", b"n"))

    report = _session().push(
        DdsFolder.DELIVERABLES,
        *files,
        target_name={"report.pdf": "report_2025.pdf", " annex.xlsx ": "annex_2025.xlsx"},
    )

    assert report.paths == (
        f"{TARGET}/report_2025.pdf",
        f"{TARGET}/annex_2025.xlsx",
        f"{TARGET}/notes.txt",
    )
    assert [c.request.content for c in put.calls] == [b"%PDF-1", b"x", b"n"]
    assert put.calls[0].request.headers["Content-Type"] == "application/pdf"


@pytest.mark.parametrize(
    ("target_name", "message"),
    [
        ({"nope.pdf": "x.pdf"}, r"renames 'nope.pdf', but no file pushed has that name"),
        ({"report.pdf": "notes.txt"}, "several files would be pushed as notes.txt"),
        ({"report.pdf": "a/b.pdf"}, "must not be a path"),
        ({"report.pdf": 3}, "maps file names"),
    ],
)
def test_a_bad_dict_target_name_uploads_nothing(
    target_name: dict[str, object], message: str
) -> None:
    with respx.mock(assert_all_called=False) as mock:
        put = mock.put(url__startswith=file_url(TARGET))
        with pytest.raises(DdsSessionError, match=message):
            _session().push(
                DdsFolder.DELIVERABLES,
                REPORT,
                DdsFile("notes.txt", b""),
                target_name=target_name,  # type: ignore[arg-type]
            )
    assert not put.called


@respx.mock
def test_an_empty_value_in_the_dict_keeps_the_name() -> None:
    respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))

    report = _session().push(DdsFolder.DELIVERABLES, REPORT, target_name={"report.pdf": ""})

    assert report.paths == (f"{TARGET}/report.pdf",)


def test_push_with_no_files_is_a_session_error() -> None:
    with pytest.raises(DdsSessionError, match="no files to push"):
        _session().push(DdsFolder.METADATA)


def test_folders_lists_the_dds_folder_values() -> None:
    assert _session(env={}).folders() == [m.name for m in DdsFolder]


@respx.mock
def test_a_controller_handed_in_is_used(documents: DocumentsClient) -> None:
    respx.put(file_url(f"{TARGET}/report.pdf")).mock(return_value=httpx.Response(201))

    report = _session(env={}, controller=DdsController(documents)).push(
        DdsFolder.DELIVERABLES, REPORT
    )

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

    _session(dotenv_path=tmp_path / ".env").push(DdsFolder.DELIVERABLES, REPORT)

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


# -- get ----------------------------------------------------------------------


@respx.mock
def test_get_saves_the_file_in_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    respx.get(file_url(f"{TARGET}/report.pdf")).mock(
        return_value=httpx.Response(200, content=b"%PDF-1")
    )

    got = _session().get(DdsFolder.DELIVERABLES, "report.pdf")

    assert got.path == tmp_path / "report.pdf"
    assert (tmp_path / "report.pdf").read_bytes() == b"%PDF-1"
    assert Path(got).read_bytes() == b"%PDF-1"  # os.PathLike: the local file
    assert got.source == f"{TARGET}/report.pdf"
    assert (got.size, got.content_type) == (6, "application/pdf")
    assert repr(got) == f"got {TARGET}/report.pdf (6 bytes) -> {tmp_path / 'report.pdf'}"


@respx.mock
def test_get_path_and_output_dir(tmp_path: Path) -> None:
    respx.get(file_url("catalog/air/aq/metadata/annex/aq.xml")).mock(
        return_value=httpx.Response(200, content=b"<x/>" * 600)
    )

    got = _session().get(
        DdsFolder.METADATA, "annex/aq.xml", path="catalog.air.aq", output_dir=str(tmp_path / "dl")
    )

    assert got.path == tmp_path / "dl" / "aq.xml"
    assert got.path.read_bytes() == b"<x/>" * 600
    assert "(2.3 KB)" in repr(got)


@respx.mock
def test_get_overwrite(tmp_path: Path) -> None:
    respx.get(file_url(f"{TARGET}/report.pdf")).mock(
        return_value=httpx.Response(200, content=b"new")
    )
    (tmp_path / "report.pdf").write_bytes(b"old")
    session = _session()

    with pytest.raises(DdsSessionError, match="already exists; pass overwrite=True"):
        session.get(DdsFolder.DELIVERABLES, "report.pdf", output_dir=str(tmp_path), overwrite=False)
    assert (tmp_path / "report.pdf").read_bytes() == b"old"

    session.get(DdsFolder.DELIVERABLES, "report.pdf", output_dir=str(tmp_path))
    assert (tmp_path / "report.pdf").read_bytes() == b"new"


@respx.mock
def test_get_a_missing_file_is_a_session_error(tmp_path: Path) -> None:
    respx.get(file_url(f"{TARGET}/repot.pdf")).mock(return_value=httpx.Response(404))

    with pytest.raises(DdsSessionError, match=f"^no file 'repot.pdf' in {TARGET}$"):
        _session().get(DdsFolder.DELIVERABLES, "repot.pdf", output_dir=str(tmp_path))
    assert not (tmp_path / "repot.pdf").exists()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"filename": 3}, r"\(filename\) must be str"),
        ({"filename": "a/../b.pdf"}, "must not be a path"),
        ({"path": "catalog"}, "catalog root"),
        ({"overwrite": "yes"}, r"\(overwrite\) must be bool"),
    ],
)
def test_get_bad_arguments_need_no_settings(kwargs: dict[str, object], message: str) -> None:
    kwargs.setdefault("filename", "report.pdf")
    with pytest.raises(DdsSessionError, match=message):
        _session(env={}).get(DdsFolder.DELIVERABLES, **kwargs)  # type: ignore[arg-type]


def test_get_into_a_folder_of_that_name_is_refused(tmp_path: Path) -> None:
    (tmp_path / "report.pdf").mkdir()
    with pytest.raises(DdsSessionError, match="is a folder"):
        _session(env={}).get(DdsFolder.DELIVERABLES, "report.pdf", output_dir=str(tmp_path))
