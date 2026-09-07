"""Registering `%catalog`/`%ingest` as an import side effect.

See `eea_datalakehouse/notebook/__init__.py`'s `_autoregister` and the "no
`%load_ext` line needed" behaviour documented in `magics.py`'s module
docstring.

A literal fresh `import eea_datalakehouse.notebook` can't be re-exercised
inside one test process — Python caches the module after its first import,
which happens at test-collection time, before any IPython shell exists here
— so these tests call `_autoregister()` directly instead. That function
*is* the logic a fresh import runs; calling it again is just re-invoking it
on demand, not a different code path.
"""

from __future__ import annotations

from typing import Any

import pytest

import eea_datalakehouse.notebook as notebook_pkg
from eea_datalakehouse.notebook import magics as magics_module
from eea_datalakehouse.notebook.magics import EEALakehouseMagics


def test_autoregister_is_a_noop_outside_ipython(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(notebook_pkg, "get_ipython", lambda: None)

    notebook_pkg._autoregister()  # must not raise with no shell to register against


def test_autoregister_registers_against_a_running_shell(shell: Any) -> None:
    shell.magics_manager.registry.pop("EEALakehouseMagics", None)

    notebook_pkg._autoregister()

    assert isinstance(shell.magics_manager.registry["EEALakehouseMagics"], EEALakehouseMagics)


def test_autoregister_does_not_replace_an_already_registered_instance(shell: Any) -> None:
    shell.magics_manager.registry.pop("EEALakehouseMagics", None)
    notebook_pkg._autoregister()
    first = shell.magics_manager.registry["EEALakehouseMagics"]
    first._catalog_session = "sentinel"  # stands in for real queued session state

    notebook_pkg._autoregister()  # calling it again must not clobber the above

    assert shell.magics_manager.registry["EEALakehouseMagics"] is first
    assert first._catalog_session == "sentinel"


def test_load_ext_after_autoregister_does_not_replace_it_either(shell: Any) -> None:
    # The two entry points (auto-import, explicit `%load_ext`) have to agree
    # with each other too, not just with themselves — using both in either
    # order must still never drop a session's already-queued state.
    shell.magics_manager.registry.pop("EEALakehouseMagics", None)
    notebook_pkg._autoregister()
    first = shell.magics_manager.registry["EEALakehouseMagics"]
    first._ingest_session = "sentinel"

    magics_module.load_ipython_extension(shell)

    assert shell.magics_manager.registry["EEALakehouseMagics"] is first
    assert first._ingest_session == "sentinel"
