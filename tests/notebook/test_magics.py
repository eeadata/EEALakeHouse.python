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
        self.commit_kwargs: dict[str, Any] | None = None

    def data_copy(self, *args: Any, **kwargs: Any) -> _FakeCatalogSession:
        self.calls.append(f"data_copy{args!r}{kwargs!r}")
        return self

    def use(self, path: str) -> _FakeCatalogSession:
        self.calls.append(f"use({path!r})")
        return self

    def set_tags(self, *args: Any, **kwargs: Any) -> _FakeCatalogSession:
        self.calls.append(f"set_tags{args!r}{kwargs!r}")
        return self

    def commit(self, **kwargs: Any) -> str:
        self.committed = True
        self.commit_kwargs = kwargs
        return "committed"

    def raise_commit_error(self) -> None:
        raise CatalogCommitError(
            "boom",
            failed_step="data_copy",
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


def test_catalog_magic_executes_and_commits_immediately(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeCatalogSession()
    build_calls = []
    monkeypatch.setattr(
        magics_module, "_build_catalog_session", lambda: (build_calls.append(1), fake)[1]
    )

    ip.run_line_magic("catalog", 'data_copy("a.b", "c.d", overwrite=True)')  # no separate commit

    assert build_calls == [1]
    assert fake.calls == ["data_copy('a.b', 'c.d'){'overwrite': True}"]
    assert fake.committed is True
    assert fake.commit_kwargs == {"retry": True}


def test_catalog_magic_builds_the_session_once_and_reuses_it(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeCatalogSession()
    build_calls = []
    monkeypatch.setattr(
        magics_module, "_build_catalog_session", lambda: (build_calls.append(1), fake)[1]
    )

    ip.run_line_magic("catalog", 'data_copy("a.b", "c.d")')
    ip.run_line_magic("catalog", 'data_copy("e.f", "g.h")')

    assert build_calls == [1]  # only built once, reused across calls
    assert fake.calls == ["data_copy('a.b', 'c.d'){}", "data_copy('e.f', 'g.h'){}"]


def test_bare_commit_is_still_accepted_as_a_harmless_noop(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # `commit` is no longer part of %catalog's primary vocabulary (see
    # _CATALOG_HELP), but old notebooks/muscle memory calling it explicitly
    # should still work rather than break.
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

    ip.run_line_magic("catalog", 'data_copy("a.b", "c.d")')

    out = capsys.readouterr().out
    assert "catalog error:" in out
    assert "DREMIO_BASE_URL" in out


def test_catalog_cell_magic_runs_the_magic_line_then_each_cell_line_in_order(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)

    ip.run_cell_magic(
        "catalog",
        'use("bwd.reference")',
        'set_tags(".water_temperature", ["reviewed"])\ndata_copy(".water_temperature", ".archive")',
    )

    assert fake.calls == [
        "use('bwd.reference')",
        "set_tags('.water_temperature', ['reviewed']){}",
        "data_copy('.water_temperature', '.archive'){}",
    ]
    assert fake.committed is True  # auto-committed after each call, same as the line magic


def test_catalog_cell_magic_stops_at_the_first_failing_line(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)

    ip.run_cell_magic("catalog", "", 'raise_commit_error()\nset_tags(".x", ["y"])')

    out = capsys.readouterr().out
    assert "catalog error:" in out
    assert "boom" in out
    assert fake.calls == []  # the second line never ran


def test_catalog_cell_magic_missing_credentials_prints_a_friendly_message(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("DREMIO_BASE_URL", raising=False)
    monkeypatch.delenv("DREMIO_TOKEN", raising=False)

    ip.run_cell_magic("catalog", 'use("bwd.reference")', 'set_tags(".x", ["y"])')

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
    # The table itself is rendered via IPython.display, not print() — capture
    # what gets displayed rather than relying on capsys for it.
    displayed = []
    monkeypatch.setattr(magics_module, "display", displayed.append)

    ip.run_line_magic("catalog", line)

    out = capsys.readouterr().out
    assert "%catalog methods" in out
    assert "DREMIO_BASE_URL" not in out  # never tried to build a session
    # General note about leading-'.'/'../' relative paths — printed once,
    # not per-row, since it applies across every path/source_path/target_path.
    assert "resolves against the current context" in out
    assert "except use's own" in out  # use is the one path that's always literal/absolute
    assert "walks up that many" in out  # ../ support, mentioned generally
    # A path already starting with 'catalog' (the one real root source) is never
    # appended to an existing context, even without a leading dot.
    assert "starts with 'catalog'" in out
    assert "taken literally as absolute" in out

    assert len(displayed) == 1
    table_html = displayed[0].data
    # A plain table — command / parameters / description — not raw Python
    # signatures, so it reads for a non-developer.
    assert "Command" in table_html and "Parameters" in table_html and "Description" in table_html
    assert "data_copy" in table_html and "source_path, target_path, overwrite=False" in table_html
    assert "str | None" not in table_html  # no Python type-hint syntax leaking into the table
    assert "list[str]" not in table_html
    assert ">commit<" not in table_html  # no "commit" row — not primary vocabulary anymore
    assert ">tag<" not in table_html and ">untag<" not in table_html  # renamed
    assert ">set_meta<" not in table_html  # removed entirely
    # set_tags/delete_tags/set_wiki spell out their `tags` parameter's format
    # (quotes come back html-escaped, e.g. &quot;, hence the split assertions).
    assert "tags: a list of strings, e.g." in table_html
    # set_wiki is the only remaining method using the {tag_name, tag_value,
    # tag_title} dict example, now that set_meta is gone.
    assert table_html.count("tags: a list of {tag_name, tag_value, tag_title} dicts") == 1
    assert table_html.count("bw-team") == 1
    # data_copy/data_move/create_view call out the CatalogOperationError overwrite=False can raise.
    assert table_html.count("overwrite=False (default) raises CatalogOperationError") == 3
    # create_view: a real command now, alongside data_move (which lost
    # entry_type — it always moves as a TABLE; create_view covers the VIEW case).
    assert "create_view" in table_html
    assert "leaving source_path untouched" in table_html
    assert "always as a TABLE" in table_html
    assert "entry_type" not in table_html  # no longer part of data_move's vocabulary
    # data_copy/data_move/create_view share the same create_target_folder explanation.
    assert table_html.count("creates every missing folder level of target_path") == 3
    assert table_html.count("the space/source itself, which must already exist") == 3
    # set_tags/delete_tags call out the specific error they can raise.
    assert "Tables/views only — raises CatalogOperationError otherwise." in table_html
    # use/get_context are listed; set_context is deliberately not (use covers it).
    assert "must be a whole" in table_html  # use takes path literally, never relative
    assert "never relative to the current context" in table_html
    # use's live existence check (apostrophe in "doesn't" comes back escaped).
    assert "Raises if path" in table_html
    assert "exist in the catalog" in table_html
    assert "get_context" in table_html
    assert "Show the current path" in table_html
    assert ">set_context<" not in table_html
    # create_folder spells out create_parents' two behaviors (apostrophe in
    # "doesn't" comes back html-escaped, hence stopping the check before it).
    assert "create_parents=True creates every missing level" in table_html
    assert (
        "create_parents=False (default) raises CatalogOperationError if the parent "
        "folder" in table_html
    )
    # delete_folder: cascade behavior, the not-empty error, and that a
    # missing path is idempotent rather than an error (apostrophe in
    # "isn't"/"that's" comes back html-escaped, hence the split checks).
    assert "cascade=True also deletes everything at path and below" in table_html
    assert "cascade=False (default) raises CatalogOperationError if the folder" in table_html
    assert "already gone is not an error" in table_html
    # New read-only queries and their delete counterparts.
    assert "get_wiki" in table_html and "get_tags" in table_html
    assert "delete_view" in table_html and "delete_table" in table_html
    assert "deleteview" not in table_html and "deletetable" not in table_html  # renamed
    assert ">list<" in table_html and ">schema<" in table_html
    assert "must be a table or view" in table_html  # schema's type requirement
    # list's path is now optional — defaults to listing the current context
    # (the empty-string default's quotes come back html-escaped as &#x27;).
    assert "path=&#x27;&#x27;" in table_html
    assert "path may be omitted to list the current context itself" in table_html
    assert "raises CatalogSessionError if none is set" in table_html
    # list/delete_view/delete_table each spell out the existence error the
    # same way (apostrophe in "doesn't" comes back html-escaped, hence
    # stopping before it).
    assert table_html.count("Raises CatalogOperationError if path") == 3
    assert "exists but has no wiki at all" in table_html  # get_wiki's own wording
    assert _magics_instance(ip)._catalog_session is None


@pytest.mark.parametrize("line", ["help", "help()"])
def test_ingest_help_lists_methods(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], line: str
) -> None:
    # Same HTML-table rendering as %catalog help — see that test for why
    # display() is captured directly rather than via capsys.
    displayed = []
    monkeypatch.setattr(magics_module, "display", displayed.append)

    ip.run_line_magic("ingest", line)

    out = capsys.readouterr().out
    assert "%ingest methods" in out

    assert len(displayed) == 1
    table_html = displayed[0].data
    assert "Command" in table_html and "Parameters" in table_html and "Description" in table_html
    assert ">ingest<" in table_html and ">commit<" in table_html
    assert "folder, target_catalog_path" in table_html
    assert "retry=False, max_retries=3" in table_html
    assert "str | None" not in table_html  # no Python type-hint syntax leaking into the table
    assert _magics_instance(ip)._ingest_session is None
