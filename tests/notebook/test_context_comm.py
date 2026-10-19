"""Pushing catalog context into a kernel invisibly, via a Jupyter Comm.

See `eea_datalakehouse/notebook/magics.py`'s `_register_context_comm`/
`_apply_context` and `docs/notebook-facade-for-data-scientists.md`,
"Pre-filling catalog context from a JupyterLab tree click".
"""

from __future__ import annotations

from typing import Any

import pytest

from eea_datalakehouse.notebook import magics as magics_module
from eea_datalakehouse.notebook.magics import EEALakehouseMagics, _register_context_comm


class _FakeCommManager:
    """Records `register_target` calls instead of touching a real kernel."""

    def __init__(self) -> None:
        self.targets: dict[str, Any] = {}

    def register_target(self, name: str, handler: Any) -> None:
        self.targets[name] = handler


class _FakeKernel:
    def __init__(self, comm_manager: _FakeCommManager) -> None:
        self.comm_manager = comm_manager


class _FakeComm:
    """Records the `on_msg` handler a comm_open callback installs."""

    def __init__(self) -> None:
        self._on_msg: Any = None

    def on_msg(self, handler: Any) -> Any:
        self._on_msg = handler
        return handler

    def deliver(self, path: object) -> None:
        assert self._on_msg is not None, "comm_open never called comm.on_msg"
        self._on_msg({"content": {"data": {"path": path}}})


def _magics_without_shell_init(shell: Any) -> EEALakehouseMagics:
    # EEALakehouseMagics.__init__ already calls _register_context_comm — build
    # one directly rather than via shell.register_magics so these tests can
    # supply a fake kernel without needing a real Jupyter one.
    return EEALakehouseMagics(shell=shell)


def test_register_context_comm_is_a_noop_without_a_real_kernel() -> None:
    class _ShellWithNoKernel:
        pass

    # Must not raise even though there's nothing to register a Comm target
    # with — a plain IPython shell (or this package's own test shell) has no
    # `.kernel` at all.
    _register_context_comm(_ShellWithNoKernel(), magics=object())  # type: ignore[arg-type]


def test_comm_message_reaches_apply_context(shell: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(EEALakehouseMagics, "_apply_context", lambda self, path: calls.append(path))
    comm_manager = _FakeCommManager()
    shell.kernel = _FakeKernel(comm_manager)
    try:
        _magics_without_shell_init(shell)
        comm_open = comm_manager.targets["eea_datalakehouse.catalog_context"]
        comm = _FakeComm()
        comm_open(comm, {"content": {"data": {}}})

        comm.deliver("bwd.reference")

        assert calls == ["bwd.reference"]
    finally:
        del shell.kernel


def test_comm_message_with_a_non_string_path_is_ignored(
    shell: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(EEALakehouseMagics, "_apply_context", lambda self, path: calls.append(path))
    comm_manager = _FakeCommManager()
    shell.kernel = _FakeKernel(comm_manager)
    try:
        _magics_without_shell_init(shell)
        comm_open = comm_manager.targets["eea_datalakehouse.catalog_context"]
        comm = _FakeComm()
        comm_open(comm, {"content": {"data": {}}})

        comm.deliver(None)

        assert calls == []
    finally:
        del shell.kernel


class _FakeCatalogSession:
    def __init__(self) -> None:
        self.context: str | None = None

    def set_context(self, path: str) -> None:
        self.context = path

    def commit(self, **kwargs: Any) -> str:
        return "committed"


def test_apply_context_sets_it_on_an_existing_session() -> None:
    magics = EEALakehouseMagics.__new__(EEALakehouseMagics)
    fake_session = _FakeCatalogSession()
    magics._catalog_session = fake_session  # type: ignore[assignment]
    magics._pending_context = None

    magics._apply_context("bwd.reference")

    assert fake_session.context == "bwd.reference"


def test_apply_context_defers_when_no_session_and_no_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        magics_module,
        "_build_catalog_session",
        lambda: (_ for _ in ()).throw(RuntimeError("no creds")),
    )
    magics = EEALakehouseMagics.__new__(EEALakehouseMagics)
    magics._catalog_session = None
    magics._pending_context = None

    magics._apply_context("bwd.reference")

    assert magics._catalog_session is None
    assert magics._pending_context == "bwd.reference"


def test_catalog_magic_applies_a_pending_context_once_it_builds_a_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_session = _FakeCatalogSession()
    monkeypatch.setattr(magics_module, "_build_catalog_session", lambda: fake_session)
    magics = EEALakehouseMagics.__new__(EEALakehouseMagics)
    magics._catalog_session = None
    magics._ingest_session = None
    magics._pending_context = "bwd.reference"

    class _FakeShell:
        user_ns: dict[str, Any] = {}

    magics.shell = _FakeShell()  # type: ignore[attr-defined]

    magics.catalog("commit")

    assert fake_session.context == "bwd.reference"
    assert magics._pending_context is None
