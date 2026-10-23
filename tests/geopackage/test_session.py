from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest

from eea_datalakehouse.geopackage import GpkgController, GpkgSession, GpkgSessionError
from eea_datalakehouse.geopackage.session import output_file

from .conftest import DATASET, FakeSource


def _session(context: str | None = DATASET, source: FakeSource | None = None) -> GpkgSession:
    source = source or FakeSource({f"{DATASET}.t": [pa.record_batch({"v": [1]})]})
    return GpkgSession(
        resolve_path=lambda path: context if path is None else path,
        controller=GpkgController(source),
    )


def test_export_defaults_to_the_context_and_the_notebook_folder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)

    report = _session().export()

    assert report.path == tmp_path / "bwd.gpkg"
    assert report.path.is_file()


def test_export_prints_a_line_per_table(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _session().export(output_dir=str(tmp_path))

    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith(f"exporting 1 table(s)/view(s) from {DATASET}")
    assert out[1].startswith(f"✓ 1/1 t ← {DATASET}.t: attributes, 1 rows")
    assert out[2].startswith(f"done: {tmp_path / 'bwd.gpkg'} — 1 layer(s), 1 rows")


def test_filename_and_output_dir(tmp_path: Path) -> None:
    report = _session().export(filename="mine", output_dir=str(tmp_path), workers=1)

    assert report.path == tmp_path / "mine.gpkg"


@pytest.mark.parametrize(
    ("context", "message"),
    [
        (None, r"%catalog use\(\.\.\.\) or pass path="),
        ("catalog", "the catalog root"),
    ],
)
def test_no_dataset_folder_is_a_session_error(context: str | None, message: str) -> None:
    with pytest.raises(GpkgSessionError, match=message):
        _session(context).export()


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"limit": "10"}, r"argument 3 \(limit\) must be int or None"),
        ({"overwrite": 1}, r"argument 7 \(overwrite\) must be bool"),
        ({"workers": "4"}, r"argument 8 \(workers\) must be int"),
        ({"workers": 0}, "workers must be a positive whole number"),
        ({"srid": 3.5}, r"argument 4 \(srid\) must be int or str or None"),
        ({"filename": "a/b.gpkg"}, "use output_dir= for the folder"),
        ({"srid": "nonsense"}, "not an EPSG coordinate reference system"),
    ],
)
def test_bad_arguments_are_session_errors(
    kwargs: dict[str, object], message: str, tmp_path: Path
) -> None:
    with pytest.raises(GpkgSessionError, match=message):
        _session().export(output_dir=str(tmp_path), **kwargs)  # type: ignore[arg-type]


def test_a_missing_table_is_a_session_error(tmp_path: Path) -> None:
    with pytest.raises(GpkgSessionError, match="no table or view 'nope'"):
        _session().export(tables="nope", output_dir=str(tmp_path))


def test_call_that_does_not_fit_the_signature_shows_the_usage() -> None:
    with pytest.raises(GpkgSessionError) as info:
        _session().export(crs=3035)  # type: ignore[call-arg]

    assert str(info.value).endswith(
        "usage: export(path=None, tables=None, limit=None, srid=None, filename=None, "
        "output_dir=None, overwrite=True, workers=4)"
    )


def test_no_catalog_is_a_session_error(tmp_path: Path) -> None:
    session = GpkgSession(resolve_path=lambda path: DATASET)

    with pytest.raises(GpkgSessionError, match="no Dremio catalog"):
        session.export(output_dir=str(tmp_path))


def test_each_export_opens_and_closes_its_own_flight_channels(tmp_path: Path) -> None:
    opened: list[Any] = []

    class Channel:
        closed = False

        def stream_batches(self, sql: str, *, idempotency_key: str) -> Any:
            yield pa.record_batch({"v": [1]})

        def close(self) -> None:
            self.closed = True

    def open_flight_channel() -> Channel:
        opened.append(Channel())
        return opened[-1]

    catalog = SimpleNamespace(
        open_flight_channel=open_flight_channel,
        _catalog_rest=SimpleNamespace(
            list_dataset_kinds=lambda segments: [
                (f"{DATASET}.a", "TABLE"),
                (f"{DATASET}.b", "TABLE"),
            ]
        ),
    )
    session = GpkgSession(resolve_path=lambda path: DATASET, catalog=lambda: catalog)

    report = session.export(output_dir=str(tmp_path), workers=2)

    assert [layer.rows for layer in report.layers] == [1, 1]
    assert 1 <= len(opened) <= 2 and all(channel.closed for channel in opened)


def test_output_file() -> None:
    assert output_file('catalog."bathing water".bwd', None, "/x").as_posix() == "/x/bwd.gpkg"
    assert output_file(DATASET, "a.GPKG", "/x").as_posix() == "/x/a.GPKG"
