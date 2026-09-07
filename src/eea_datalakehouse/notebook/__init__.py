"""The notebook-facing surface — `%catalog`/`%ingest` magics over
`CatalogSession`/`IngestSession` (see `docs/notebook-facade-for-data-
scientists.md`). Requires IPython, which the rest of this package does not
— install the `notebook` extra (`pip install "EEADataLakehouse[notebook]"`)
to get it.

Importing this package inside a running IPython shell registers both magics
as a side effect — a custodian only ever needs::

    import eea_datalakehouse.notebook

not a separate `%load_ext eea_datalakehouse.notebook.magics` line (that still
works too, e.g. from an IPython startup file that imports the module
directly rather than running a magic — see `magics.load_ipython_extension`).
Outside IPython (no shell running yet, or none at all) importing this
package is simply a no-op registration-wise.
"""

from __future__ import annotations

from IPython import get_ipython


def _autoregister() -> None:
    shell = get_ipython()
    if shell is None:
        return  # not running inside IPython (yet) — nothing to register with
    if "EEALakehouseMagics" in shell.magics_manager.registry:
        return  # already registered — see magics.load_ipython_extension's matching guard

    from .magics import EEALakehouseMagics

    shell.register_magics(EEALakehouseMagics)


_autoregister()
