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


class _FakeCommitReport:
    """Mirrors the real `CommitReport`'s one field `_dispatch` inspects."""

    def __init__(self, succeeded: list[str]) -> None:
        self.succeeded = succeeded


class _FakeCatalogSession:
    """Stands in for a real `CatalogSession` — records calls, never touches
    a real Catalog. `use()` doesn't queue anything (matching the real
    class), so `commit()`'s report only ever reflects `data_copy`/
    `set_tags`/... calls made since the last commit."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.committed = False
        self.commit_kwargs: dict[str, Any] | None = None
        self._pending: list[str] = []
        self._context: str | None = None

    def data_copy(self, *args: Any, **kwargs: Any) -> _FakeCatalogSession:
        call = f"data_copy{args!r}{kwargs!r}"
        self.calls.append(call)
        self._pending.append(call)
        return self

    def use(self, path: str | None) -> _FakeCatalogSession:
        self.calls.append(f"use({path!r})")
        # Matches the real CatalogSession: None/"" reset to the catalog
        # root rather than clearing to no context at all.
        self._context = "catalog" if path is None or path == "" else path
        return self

    def get_context(self) -> str | None:
        return self._context

    def set_tags(self, *args: Any, **kwargs: Any) -> _FakeCatalogSession:
        call = f"set_tags{args!r}{kwargs!r}"
        self.calls.append(call)
        self._pending.append(call)
        return self

    def commit(self, **kwargs: Any) -> _FakeCommitReport:
        self.committed = True
        self.commit_kwargs = kwargs
        succeeded, self._pending = self._pending, []
        return _FakeCommitReport(succeeded)

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
    instance._sdi_session = None
    instance._dds_session = None
    instance._gpkg_session = None
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


def test_use_prints_the_resulting_context_not_an_empty_commit_report(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # use() chains back to `self` like every queueing verb, but queues
    # nothing — commit() then has nothing to report, so the context it
    # just set is printed instead of an uninformative empty CommitReport.
    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)

    result = ip.run_line_magic("catalog", 'use("bwd.reference")')

    assert result is None
    assert capsys.readouterr().out == "context set to 'bwd.reference'\n"
    assert fake.committed is True  # still committed — a harmless no-op, nothing was queued


def test_use_none_prints_context_set_to_the_catalog_root(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)

    ip.run_line_magic("catalog", "use(None)")

    assert capsys.readouterr().out == "context set to 'catalog'\n"


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


def test_catalog_cell_magic_does_not_stop_after_use_none(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # use(None) legitimately dispatches to a `None` result (it prints its
    # own status instead of an empty commit report) — the cell loop must
    # not mistake that for "an error was printed" and bail out before
    # running the rest of the cell.
    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)

    ip.run_cell_magic("catalog", "use(None)", 'set_tags("a.b", ["reviewed"])')

    assert fake.calls == ["use(None)", "set_tags('a.b', ['reviewed']){}"]


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


def test_ingest_diagnose_with_nothing_queued(ip: Any, capsys: pytest.CaptureFixture[str]) -> None:
    ip.run_line_magic("ingest", "diagnose")  # bare name, like commit
    assert "nothing queued" in capsys.readouterr().out


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
    # General note about absolute/relative paths — printed once, not per-row,
    # since it applies across every path/source_path/target_path.
    assert "is either absolute or" in out
    assert "except use's own" in out  # use is the one path that's always literal/absolute
    assert "walks up that many" in out  # ../ support, mentioned generally
    assert "the context itself, exactly as get_context() shows it" in out  # '.'/'./' alone
    # A path already starting with 'catalog.' (the one real root source) is never
    # appended to an existing context, even without a leading dot.
    assert "starts with 'catalog.'" in out
    assert "the dot is optional sugar, not what makes it relative" in out

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
    assert ">diagnose<" in table_html
    assert "folder, target_catalog_path" in table_html
    assert "retry=False, max_retries=3" in table_html
    # PY-11: the one-line options show, with their defaults.
    assert "csv_options=None" in table_html and "upload_mode=&#x27;auto&#x27;" in table_html
    assert "cleanup_on_failure=False" in table_html
    assert "str | None" not in table_html  # no Python type-hint syntax leaking into the table
    assert _magics_instance(ip)._ingest_session is None


def test_sdi_magic_builds_one_session_and_dispatches_onto_it(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    class _FakeSdiSession:
        def get_xml(self, uuid: str) -> str:
            calls.append(uuid)
            return f"metadata {uuid}"

    monkeypatch.setattr(magics_module, "_build_sdi_session", _FakeSdiSession)

    assert ip.run_line_magic("sdi", 'get_xml("u-1")') == "metadata u-1"
    first = _magics_instance(ip)._sdi_session
    ip.run_line_magic("sdi", 'get_xml("u-2")')

    assert calls == ["u-1", "u-2"]
    assert _magics_instance(ip)._sdi_session is first


def test_sdi_session_error_is_printed_not_raised(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SDI_API_URL", "https://sdi.example.test/catalogue")

    assert ip.run_line_magic("sdi", 'get_xml("")') is None

    assert "sdi error: uuid must not be empty" in capsys.readouterr().out


def test_dds_push_without_a_context_is_printed_not_raised(
    ip: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    assert ip.run_line_magic("dds", 'push(METADATA, "x.xml")') is None

    assert "dds error: no DDS target path" in capsys.readouterr().out


class _FakeDdsSession:
    """Records what `%dds push` receives; the target comes from `resolve_path`."""

    def __init__(self, resolve_path: Any) -> None:
        self._resolve_path = resolve_path
        self.calls: list[tuple[object, object, str | None, bool]] = []

    def push(
        self, folder: object, *files: object, path: str | None = None, overwrite: bool = True
    ) -> object:
        target = self._resolve_path(path)
        self.calls.append((folder, files, target, overwrite))
        return magics_module.PushReport(
            f"{target}/x", (f"{target}/x/a.pdf", f"{target}/x/b.pdf"), overwrite
        )


def test_dds_pushes_to_the_catalog_context_with_bare_folder_names(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)
    monkeypatch.setattr(magics_module, "_build_dds_session", _FakeDdsSession)
    notified: list[str] = []
    monkeypatch.setattr(
        magics_module, "_notify_dds_pushed", lambda shell, path: notified.append(path)
    )

    # %dds built first: the context is still looked up at push time.
    ip.run_line_magic("dds", "help")
    ip.run_line_magic("catalog", 'use("catalog.water.bwd")')
    report = ip.run_line_magic("dds", 'push(DELIVERABLES, "a.pdf", "b.pdf", overwrite=False)')

    session = _magics_instance(ip)._dds_session
    assert session.calls == [
        (magics_module.DdsFolder.DELIVERABLES, ("a.pdf", "b.pdf"), "catalog.water.bwd", False)
    ]
    assert report.paths == ("catalog.water.bwd/x/a.pdf", "catalog.water.bwd/x/b.pdf")
    assert notified == list(report.paths)


def test_dds_folder_names_win_over_notebook_variables(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(magics_module, "_build_dds_session", _FakeDdsSession)
    _magics_instance(ip)._pending_context = "catalog.air"
    ip.user_ns["METADATA"] = "assessments"
    try:
        ip.run_line_magic("dds", 'push(METADATA, "a.pdf")')
        ip.run_line_magic("dds", 'push(DdsFolder.OTHERS, "a.pdf")')
    finally:
        del ip.user_ns["METADATA"]

    calls = _magics_instance(ip)._dds_session.calls
    assert [call[0] for call in calls] == [
        magics_module.DdsFolder.METADATA,
        magics_module.DdsFolder.OTHERS,
    ]


def test_context_target_resolves_path_like_catalog(
    ip: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    from eea_datalakehouse.catalog.session import CatalogSession

    real = CatalogSession.__new__(CatalogSession)
    real._context = "catalog.water.bwd"
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: real)
    instance = _magics_instance(ip)
    instance._catalog_session = real

    assert instance._context_target(None) == "catalog.water.bwd"
    assert instance._context_target("../other") == "catalog.water.other"
    assert instance._context_target("catalog/air/aq") == "catalog.air.aq"


def test_context_target_before_any_catalog_session(ip: Any) -> None:
    instance = _magics_instance(ip)
    instance._pending_context = "catalog.from.tree"

    assert instance._context_target(None) == "catalog.from.tree"
    assert instance._context_target("catalog.x") == "catalog.x"


def test_sdi_empty_line_prints_usage(ip: Any, capsys: pytest.CaptureFixture[str]) -> None:
    ip.run_line_magic("sdi", "")

    assert "usage: %sdi get_xml" in capsys.readouterr().out


@pytest.mark.parametrize("line", ["help", "help()"])
def test_sdi_help_lists_methods_without_building_a_session(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], line: str
) -> None:
    displayed = []
    monkeypatch.setattr(magics_module, "display", displayed.append)

    ip.run_line_magic("sdi", line)

    out = capsys.readouterr().out
    assert "%sdi methods" in out
    assert "SDI_API_URL" in out
    table_html = displayed[0].data
    assert ">get_xml<" in table_html and ">resolve_series<" in table_html
    assert ">push_to_dds<" not in table_html  # pushing is %dds's job
    assert _magics_instance(ip)._sdi_session is None


@pytest.mark.parametrize("line", ["help", "help()"])
def test_dds_help_lists_methods_without_building_a_session(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], line: str
) -> None:
    displayed = []
    monkeypatch.setattr(magics_module, "display", displayed.append)

    ip.run_line_magic("dds", line)

    out = capsys.readouterr().out
    assert "%dds methods" in out
    assert "DDS_BASE_URL" in out
    assert "deliverables/report.pdf" in out
    examples = [line.strip() for line in out.splitlines() if line.strip().startswith("%dds push(")]
    for option in ("path=", "overwrite=False", 'target_name="', "target_name={", '"*.csv"'):
        assert any(option in line or option.strip('"') in line for line in examples), option
    assert all(line.count("(") == line.count(")") for line in examples)  # each fits on one line
    table_html = displayed[0].data
    for name in ("push", "target", "folders", "dds_base_url"):
        assert f">{name}<" in table_html
    assert "folder, files..., path=None, overwrite=True, target_name=&#x27;&#x27;" in table_html
    assert "DERIVED, METADATA, METHODOLOGY, ASSESSMENTS, ANALYSES" in table_html
    assert _magics_instance(ip)._dds_session is None


def test_metadata_magic_is_gone(ip: Any) -> None:
    assert "metadata" not in ip.magics_manager.magics["line"]


class _FakePushSession:
    """Just the signature `%dds push` exposes — never pushes."""

    def push(
        self, folder: str, *files: object, path: str | None = None, overwrite: bool = True
    ) -> str:
        return "pushed"


USAGE = "usage: push(folder, files..., path=None, overwrite=True)"


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (
            "push(F, x, path=p, True)",
            "argument 4 (True) has no name, but comes after path=p — once an argument is "
            "given by name, every argument after it needs a name too. Did you mean "
            "overwrite=True?",
        ),
        ('push(F, x, overwrite=False, "catalog.a")', 'Did you mean path="catalog.a"?'),
        ("push(F, x, path=p, flag)", "Did you mean overwrite=flag"),
        ('push("a, b", x, path=p, "c(,)")', 'argument 4 ("c(,)") has no name'),
        ("push(F,", "invalid syntax:"),
    ],
)
def test_syntax_error_names_the_argument_and_shows_the_signature(
    ip: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    line: str,
    expected: str,
) -> None:
    monkeypatch.setattr(magics_module, "_build_dds_session", lambda resolve: _FakePushSession())

    assert ip.run_line_magic("dds", line) is None

    out = capsys.readouterr().out
    assert out.startswith("dds error: ")
    assert expected in out
    assert USAGE in out


def test_syntax_error_on_an_unknown_method_has_no_usage(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(magics_module, "_build_dds_session", lambda resolve: _FakePushSession())

    ip.run_line_magic("dds", "nope(a=1, 2)")

    out = capsys.readouterr().out
    assert out.startswith("dds error: invalid syntax:")
    assert "usage:" not in out


class _FakeVersionSession:
    """`create_version`'s signature, failing the way `CatalogSession` does."""

    def create_version(
        self,
        source_path: str,
        version_name: str,
        tables: list[str] | None = None,
        target_path: str | None = None,
        *,
        overwrite: bool = False,
        create_target_folder: bool = True,
    ) -> Any:
        from eea_datalakehouse.catalog.session import CatalogSessionError

        if not version_name.strip():
            raise CatalogSessionError("version_name must not be empty")
        return "created"

    def get_context(self) -> str:
        return "catalog"

    def broken(self) -> None:
        raise TypeError("bug inside the method")


VERSION_USAGE = (
    "usage: create_version(source_path, version_name, tables=None, target_path=None, "
    "overwrite=False, create_target_folder=True)"
)


@pytest.mark.parametrize(
    ("line", "error"),
    [
        ("create_version('a.b', ' ')", "catalog error: version_name must not be empty"),
        (
            "create_version('a.b', 'v1', colour='red')",
            "got an unexpected keyword argument 'colour'",
        ),
        ("create_version('a.b')", "missing 1 required positional argument: 'version_name'"),
        ("create_version('a.b', tables=['x'], 'v1')", "argument 3 ('v1') has no name"),
    ],
)
def test_create_version_errors_show_its_usage(
    ip: Any, capsys: pytest.CaptureFixture[str], line: str, error: str
) -> None:
    _magics_instance(ip)._catalog_session = _FakeVersionSession()  # type: ignore[assignment]

    assert ip.run_line_magic("catalog", line) is None

    out = capsys.readouterr().out
    assert error in out
    assert out.rstrip().endswith(VERSION_USAGE)


def test_create_version_success_prints_no_usage(
    ip: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    _magics_instance(ip)._catalog_session = _FakeVersionSession()  # type: ignore[assignment]

    assert ip.run_line_magic("catalog", "create_version('a.b', 'v1')") == "created"
    assert "usage:" not in capsys.readouterr().out


def test_a_type_error_inside_a_method_keeps_its_traceback(ip: Any) -> None:
    _magics_instance(ip)._catalog_session = _FakeVersionSession()  # type: ignore[assignment]

    with pytest.raises(TypeError, match="bug inside the method"):
        ip.run_line_magic("catalog", "broken()")


# -- %gpkg ----------------------------------------------------------------------


@pytest.mark.parametrize("line", ["help", "help()"])
def test_gpkg_help_lists_export_without_building_a_session(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], line: str
) -> None:
    displayed = []
    monkeypatch.setattr(magics_module, "display", displayed.append)

    ip.run_line_magic("gpkg", line)

    out = capsys.readouterr().out
    assert "%gpkg methods" in out and "export examples:" in out and "Streaming:" in out
    table_html = displayed[0].data
    assert ">export<" in table_html
    assert (
        "path=None, tables=None, limit=None, srid=None, filename=None, output_dir=None, "
        "overwrite=True, workers=4" in table_html
    )
    assert _magics_instance(ip)._gpkg_session is None


def test_gpkg_help_examples_fit_the_signature() -> None:
    import ast
    import inspect
    import re

    from eea_datalakehouse.geopackage import GpkgSession

    calls = re.findall(r"%gpkg (export\(.*\))\s*$", magics_module._GPKG_HELP_NOTE, re.MULTILINE)
    assert len(calls) >= 6
    for call in calls:
        tree = ast.parse(call, mode="eval").body
        assert isinstance(tree, ast.Call)
        inspect.signature(GpkgSession.export).bind(
            None, *[None] * len(tree.args), **{k.arg: None for k in tree.keywords if k.arg}
        )


def test_gpkg_exports_the_catalog_context(ip: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str | None, dict[str, Any]]] = []

    class _FakeGpkgSession:
        def __init__(self, resolve_path: Any, catalog: Any) -> None:
            self._resolve_path = resolve_path

        def export(self, path: str | None = None, **kwargs: Any) -> str:
            calls.append((self._resolve_path(path), kwargs))
            return "written"

    fake = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake)
    monkeypatch.setattr(magics_module, "_build_gpkg_session", _FakeGpkgSession)

    ip.run_line_magic("catalog", 'use("catalog.water.bwd")')
    result = ip.run_line_magic("gpkg", 'export(tables=["stations"], limit=10, workers=8)')

    assert result == "written"
    assert calls == [("catalog.water.bwd", {"tables": ["stations"], "limit": 10, "workers": 8})]


def test_gpkg_without_dremio_settings_is_printed_not_raised(
    ip: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Any
) -> None:
    for var in ("DREMIO_BASE_URL", "DREMIO_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    _magics_instance(ip)._pending_context = "catalog.water.bwd"

    assert ip.run_line_magic("gpkg", f'export(output_dir="{tmp_path}")') is None

    assert "gpkg error: %catalog needs DREMIO_BASE_URL and DREMIO_TOKEN" in capsys.readouterr().out


def test_a_gpkg_export_can_be_pushed_with_dds(
    ip: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    import httpx
    import pyarrow as pa
    import respx
    from tests.geopackage.conftest import FakeSource

    from eea_datalakehouse.dds_documents.session import DdsSession
    from eea_datalakehouse.geopackage import GpkgController, GpkgSession

    dataset = "catalog.water.bwd"
    source = FakeSource({f"{dataset}.codes": [pa.record_batch({"code": ["a"]})]})
    monkeypatch.setattr(
        magics_module,
        "_build_gpkg_session",
        lambda resolve, catalog: GpkgSession(
            resolve_path=resolve, controller=GpkgController(source)
        ),
    )
    env = {"DDS_BASE_URL": "https://dds.test", "_DREMIO_USER": "u", "_DREMIO_PWD": "p"}
    monkeypatch.setattr(
        magics_module,
        "_build_dds_session",
        lambda resolve: DdsSession(resolve_path=resolve, env=env, dotenv_path=tmp_path / "none"),
    )
    monkeypatch.setattr(magics_module, "_notify_dds_pushed", lambda shell, path: None)
    _magics_instance(ip)._pending_context = dataset

    ip.user_ns["report"] = ip.run_line_magic("gpkg", f'export(output_dir="{tmp_path}")')
    with respx.mock:
        put = respx.put(
            "https://dds.test/api/v1/files/catalog/water/bwd/deliverables/bwd.gpkg"
        ).mock(return_value=httpx.Response(201))
        pushed = ip.run_line_magic("dds", "push(DELIVERABLES, report)")

    assert pushed.paths == ("catalog/water/bwd/deliverables/bwd.gpkg",)
    request = put.calls.last.request
    assert request.content == (tmp_path / "bwd.gpkg").read_bytes()
    assert request.headers["Content-Type"] == "application/geopackage+sqlite3"
    del ip.user_ns["report"]
