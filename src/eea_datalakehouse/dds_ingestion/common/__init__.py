"""Shared helpers used by more than one dataflow.

Per the repository CLAUDE.md: reusable Python that serves a single dataflow
lives in that dataflow's `lib/`; anything a second one needs moves here.

This file is deliberately present rather than relying on an implicit namespace
package. A namespace package resolves only if nothing else named `common` sits
earlier on `sys.path`, which is not a thing a notebook can guarantee — an
explicit package is predictable in a JupyterHub kernel that has other things
installed.
"""
