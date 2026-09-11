"""`%catalog`/`%ingest` — dispatch onto a per-kernel session, friendly errors.

Uses IPython's own test shell (`IPython.testing.globalipapp`) rather than a
real kernel — the magics only need `self.shell.user_ns`, which a test shell
provides just as well.
"""

from __future__ import annotations

from typing import Any

import pytest

from eea_datalakehouse.catalog.session import CatalogCommitError
from eea_datalakehouse.dds_ingestion.session import IngestSession
from eea_datalakehouse.notebook import magics as magics_module
from eea_datalakehouse.notebook.magics import EEALakehouseMagics


class _FakeCatalogSession:
    """Stands in for a real `CatalogSession` — records calls, never touches
    a real Catalog."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.committed = False

    def copy(self, *args: Any, **kwargs: Any) -> _FakeCatalogSession:
        self.calls.append(f"copy{args!r}{kwargs!r}")
        return self

    def commit(self, **kwargs: Any) -> str:
        self.committed = True
        return "committed"

    def raise_commit_error(self) -> None:
        raise CatalogCommitError(
            "boom",
            failed_step="copy",
            original_error=RuntimeError("x"),
            rolled_back=True,
            unresolved=[],
        )


@pytest.fixture
def ip(shell: Any) -> Any:
    # Fresh session state per test, so one test's queued/committed session
    # never leaks into the next — same shell and magics instance throughout
    # (registering again is a no-op if it's already there, see
    # magics.load_ipython_extension's guard).
    shell.register_magics(EEALakehouseMagics)
    instance = _magics_instance(shell)
    instance._catalog_session = None
    instance._ingest_session = None
    return shell


def _magics_instance(ip: Any) -> EEALakehouseMagics:
    return ip.magics_manager.registry["EEALakehouseMagics"]  # type: ignore[no-any-return]


def test_catalog_magic_builds_the_session_once_and_reuses_it(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeCatalogSession()
    build_calls = []
    monkeypatch.setattr(
        magics_module, "_build_catalog_session", lambda: (build_calls.append(1), fake)[1]
    )

    ip.run_line_magic("catalog", 'copy("a.b", "c.d", overwrite=True)')
    ip.run_line_magic("catalog", "commit")

    assert build_calls == [1]  # only built once, reused across calls
    assert fake.calls == ["copy('a.b', 'c.d'){'overwrite': True}"]
    assert fake.committed is True


def test_bare_commit_without_parens_is_accepted(ip: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)

    ip.run_line_magic("catalog", "commit")

    assert fake.committed is True


def test_catalog_session_error_is_printed_not_raised(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)

    ip.run_line_magic("catalog", "raise_commit_error()")  # must not raise out of the magic

    out = capsys.readouterr().out
    assert "catalog error:" in out
    assert "boom" in out


def test_missing_credentials_prints_a_friendly_message(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("DREMIO_BASE_URL", raising=False)
    monkeypatch.delenv("DREMIO_TOKEN", raising=False)
    # Force a fresh build attempt even if an earlier test in this module left
    # a session cached on a *different* Magics instance — this test's `ip`
    # fixture registers its own.

    ip.run_line_magic("catalog", 'copy("a.b", "c.d")')

    out = capsys.readouterr().out
    assert "catalog error:" in out
    assert "DREMIO_BASE_URL" in out


def test_ingest_magic_queues_onto_one_shared_session(ip: Any) -> None:
    ip.run_line_magic(
        "ingest",
        'ingest(folder="/tmp/does-not-matter", target_catalog_path="a.b", '
        'data_format="parquet", show_progress=False)',
    )

    session = _magics_instance(ip)._ingest_session
    assert isinstance(session, IngestSession)
    assert repr(session) == "IngestSession(pending=1)"


def test_empty_line_prints_usage(ip: Any, capsys: pytest.CaptureFixture[str]) -> None:
    # No credentials needed to reach the usage message — it's printed before
    # a session is ever built.
    ip.run_line_magic("ingest", "")

    out = capsys.readouterr().out
    assert "usage:" in out


@pytest.mark.parametrize("line", ["help", "help()"])
def test_catalog_help_lists_methods_without_needing_credentials(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], line: str
) -> None:
    monkeypatch.delenv("DREMIO_BASE_URL", raising=False)
    monkeypatch.delenv("DREMIO_TOKEN", raising=False)

    ip.run_line_magic("catalog", line)

    out = capsys.readouterr().out
    assert "%catalog methods" in out
    assert "copy(source_path: str, target_path: str" in out
    assert "commit(*, retry: bool = False" in out
    assert "DREMIO_BASE_URL" not in out  # never tried to build a session
    assert _magics_instance(ip)._catalog_session is None


@pytest.mark.parametrize("line", ["help", "help()"])
def test_ingest_help_lists_methods(ip: Any, capsys: pytest.CaptureFixture[str], line: str) -> None:
    ip.run_line_magic("ingest", line)

    out = capsys.readouterr().out
    assert "%ingest methods" in out
    assert "ingest(folder: str | Path, target_catalog_path: str" in out
    assert "commit(*, retry: bool = False, max_retries: int = 3)" in out
    assert _magics_instance(ip)._ingest_session is None
