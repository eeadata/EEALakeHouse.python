"""Dremio identity for the ingest notebooks — making `%init` work everywhere.

`%init` is the EEA convention for getting a user's Dremio credentials into a
notebook. The JupyterLab Dremio extension (`jupyter_dremio`) registers it as a
line magic; running it binds what the Hub injected into the environment as
kernel globals:

    DREMIO_USERNAME   the logged-in user
    DREMIO_TOKEN      their PAT — the bearer token both DDS and Dremio expect
    DREMIO_URL        the Dremio this kernel belongs to
    DREMIO_PASSWORD   only for interactive password-auth logins (Arrow Flight)

It also exports the platform service endpoints, so notebooks reach a service by
variable rather than by hardcoded host:

    DDS_BASE_URL      the Document Service (the name the ingest client reads)
    SCHEDULER_URL     the Notebook Scheduler, including its mount prefix

Outside a Hub kernel those come from the environment instead — see `.env`, where
`DDS_BASE_URL` must be *this* environment's Document Service, not the
`dds-server` container name the local stack advertises.

Nothing is typed into the notebook and no credential is stored in it. That is
the whole point of the convention, and it is why the notebooks here use it
rather than reading `os.environ` themselves.

The catch: the magic ships *with the Hub extension*. This project's own
container does not have it — deliberately, since the acquire and prepare steps
must run with no stack at all (see the repository CLAUDE.md). A bare `%init`
cell there fails with "Line magic function %init not found", which would make
these notebooks Hub-only and break the documented local workflow.

`ensure_init_magic()` closes that gap:

* In a Hub kernel the real magic is already registered and this does nothing —
  it never shadows the extension.
* Otherwise it registers a fallback under the same name that reads the same
  four variables from the environment, plus the local aliases this project's
  `docker-compose.yml` sets (`_DREMIO_USER` / `_DREMIO_PWD` / `DREMIO_BASE_URL`).

Either way the notebook cell is the same `%init` and the globals it leaves
behind are the same, so one notebook runs unchanged in both places.

No function here prints, logs or returns a secret — only whether one is present.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path

# The ingest client now ships in the shared EEA library `EEADataLakehouse`
# (repo EEALakeHouse.python, import root `eea_datalakehouse`); what used to be
# the standalone `dds_ingest` package is its `dds_ingestion` subpackage. One
# message for a missing install, so every notebook says the same thing.
INSTALL_HINT = (
    "eea_datalakehouse is not installed in this kernel.\n"
    "  In the project container it is installed at start from the "
    "/opt/eea_datalakehouse mount — check `docker compose logs transfer`.\n"
    "  In a JupyterHub kernel install it and restart the kernel:\n"
    '    %pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@development"'
)


# ---------------------------------------------------------------------------
# The JupyterLab "Dremio Catalog" settings panel — the authoritative source.
#
# This is where a user actually configures the extension (Settings → Dremio
# Catalog), and it is written to
#     ~/.jupyter/lab/user-settings/jupyter-dremio/plugin.jupyterlab-settings
# The schema is Components/ExtensionJptrHubDremio/schema/plugin.json.
#
# It matters that this is consulted FIRST. `%init` binds from the kernel
# *environment*, and the environment is not what the panel writes to — so a URL
# configured in the panel is invisible to the magic. Reading the panel here is
# what makes "the setting I typed into JupyterHub" the value the notebook uses.
#
# `ddsServerUrl` is first in this map deliberately: the Document Service URL is
# the one that is configured per-deployment and got this wrong twice.
# ---------------------------------------------------------------------------
SETTINGS_PACKAGE = "jupyter-dremio"
SETTINGS_FILENAME = "plugin.jupyterlab-settings"

SETTINGS_MAP = {
    "ddsServerUrl": "DDS_BASE_URL",
    "dremioUrl": "DREMIO_URL",
    "username": "DREMIO_USERNAME",
    "accessToken": "DREMIO_TOKEN",
}

# The names the Hub extension exports, in its own order. Kept identical to
# `_SECRET_VARS` in jupyter_dremio/schedule_magic.py: if that list grows, this
# one follows, so the fallback never surfaces less than the real magic.
SECRET_VARS = ("DREMIO_USERNAME", "DREMIO_TOKEN", "DREMIO_URL", "DREMIO_PASSWORD")

# Platform service endpoints. Not credentials, but surfaced the same way so a
# notebook addresses a service through a variable instead of a hardcoded host —
# which is what makes one notebook portable across the local stack, a JupyterHub
# deployment and a scheduled run.
#
# Mirrors `_SERVICE_VARS` in jupyter_dremio/schedule_magic.py. The extension
# exports both of these itself, so in a Hub kernel the real magic binds them and
# this fallback is never consulted; outside the Hub they come from the
# environment (see .env).
SERVICE_VARS = ("DDS_BASE_URL", "SCHEDULER_URL")

# Everything the fallback binds — the extension's `_INJECTED_VARS`.
BOUND_VARS = SECRET_VARS + SERVICE_VARS

# What the same value is called locally. The Hub sets the canonical names; this
# project's compose file sets `_DREMIO_USER` / `_DREMIO_PWD` (the names the
# ingest client reads, matching what JupyterHub injects into a real kernel) and
# `DREMIO_BASE_URL`. Canonical wins; these are only consulted when it is absent.
LOCAL_ALIASES = {
    "DREMIO_USERNAME": ("_DREMIO_USER", "DREMIO_USER"),
    "DREMIO_TOKEN": ("_DREMIO_PWD",),
    "DREMIO_URL": ("DREMIO_BASE_URL",),
}

# Loaded in order; the first that registers `init` wins. The extension is tried
# before any fallback so a Hub kernel always gets the real thing.
_EXTENSIONS = ("jupyter_dremio", "jupyter_dremio.schedule_magic", "jupyterscheduler_magic")


def settings_paths() -> list[Path]:
    """Where JupyterLab keeps the "Dremio Catalog" settings, most specific first."""
    rel = Path("lab") / "user-settings" / SETTINGS_PACKAGE / SETTINGS_FILENAME
    roots = []
    if os.environ.get("JUPYTERLAB_SETTINGS_DIR"):
        # This one points at .../lab/user-settings directly.
        roots.append(Path(os.environ["JUPYTERLAB_SETTINGS_DIR"])
                     / SETTINGS_PACKAGE / SETTINGS_FILENAME)
    if os.environ.get("JUPYTER_CONFIG_DIR"):
        roots.append(Path(os.environ["JUPYTER_CONFIG_DIR"]) / rel)
    roots.append(Path.home() / ".jupyter" / rel)
    roots.append(Path("/home/jovyan/.jupyter") / rel)
    return roots


def _strip_jsonc(text: str) -> str:
    """JupyterLab writes JSON with comments. Drop them, keep the JSON.

    Only whole-line `//` comments and `/* */` blocks are removed — a naive
    replace would eat the `//` in `http://…`, which is exactly the value we are
    here to read.
    """
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"^[ \t]*//.*$", "", text, flags=re.M)


def read_lab_settings() -> dict[str, str]:
    """The Dremio Catalog panel, mapped onto the canonical variable names.

    Returns {} when the panel has never been saved, when the file is
    unreadable, or when running somewhere that has no JupyterLab profile — such
    as this project's container, whose filesystem does not carry the Hub user's
    settings. Never raises: a missing panel is a fallback, not an error.
    """
    for path in settings_paths():
        try:
            raw = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        try:
            data = json.loads(_strip_jsonc(raw))
        except (json.JSONDecodeError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        return {var: str(data[key]).strip()
                for key, var in SETTINGS_MAP.items()
                if data.get(key)}
    return {}


def endpoint(name: str, aliases: tuple[str, ...] = ()) -> str:
    """Resolve one endpoint, most authoritative source first.

    1. the JupyterLab "Dremio Catalog" panel — what the user actually configured
    2. whatever `%init` bound into the kernel namespace
    3. the process environment (this project's `.env`), including `aliases`

    Returns "" when nothing supplies it, and always without a trailing slash so
    callers can concatenate paths safely.
    """
    value = read_lab_settings().get(name)

    if not value:
        try:
            from IPython import get_ipython
            ip = get_ipython()
            if ip is not None:
                value = ip.user_ns.get(name)
        except ImportError:
            pass

    if not value:
        for var in (name, *aliases):
            value = os.environ.get(var)
            if value:
                break

    return (value or "").rstrip("/")


def dds_credentials(username: str | None = None, token: str | None = None):
    """`DremioCreds` for the ingest client, built from the resolved identity.

    `eea_datalakehouse.dds_ingestion.load_creds()` reads `_DREMIO_USER` /
    `_DREMIO_PWD` from the process environment. A JupyterHub kernel does not
    set those — it exports
    `DREMIO_USERNAME` / `DREMIO_TOKEN` — so the default path raises
    `MissingCredentialsError` in exactly the place where the transfer starts.
    Passing `creds=` explicitly is the supported way round it, and it also means
    the transfer uses the identity from the settings panel rather than whatever
    the environment happens to carry.

    Pass `username`/`token` to use values already resolved by the caller;
    otherwise they are resolved here, panel first.
    """
    # Imported late: the library is an optional dependency — the acquire and
    # prepare steps must keep working in a kernel that has no ingest client.
    try:
        from eea_datalakehouse.dds_ingestion import DremioCreds
    except ModuleNotFoundError as exc:  # pragma: no cover — install guidance
        raise ModuleNotFoundError(INSTALL_HINT) from exc

    ident = resolve()
    user = username or ident.get("DREMIO_USERNAME")
    pat = token or ident.get("DREMIO_TOKEN")
    if not user or not pat:
        missing = "username" if not user else "token"
        raise RuntimeError(
            f"no Dremio {missing} — set it in JupyterLab under Settings → Dremio "
            "Catalog (username / accessToken), or run the %init cell in a kernel "
            "whose environment provides DREMIO_USERNAME / DREMIO_TOKEN"
        )
    return DremioCreds(username=user, _password=pat)


def resolve() -> dict[str, str]:
    """The identity and endpoints, panel first, then the environment.

    Returns values; nothing here prints them.
    """
    found: dict[str, str] = dict(read_lab_settings())
    for var in BOUND_VARS:
        if found.get(var):
            continue
        value = os.environ.get(var)
        if not value:
            for alias in LOCAL_ALIASES.get(var, ()):
                value = os.environ.get(alias)
                if value:
                    break
        if value:
            found[var] = value
    return found


def present() -> dict[str, bool]:
    """Which parts are set — safe to print, carries no values."""
    found = resolve()
    return {var: var in found for var in BOUND_VARS}


def _fallback_init(ip):
    """The stand-in `%init`, registered only when the extension is absent.

    Mirrors `ScheduleMagics._apply`: bind everything in `BOUND_VARS` that is
    present into the user namespace and say nothing about the rest. The parameter
    injection half of the real magic (a JSON body on `%%init`) is not
    reproduced — these notebooks keep their parameters in the User Parameters
    cell, which is this repo's convention.
    """

    def init(line: str = "") -> None:  # noqa: ARG001 — the line form takes no argument
        found = resolve()
        ip.user_ns.update(found)
        if "DREMIO_TOKEN" in found:
            who = found.get("DREMIO_USERNAME", "<no username>")
            print(f"[init] fallback (no Dremio extension here) — identity for {who}")
        else:
            # Not fatal: the acquire and prepare steps need no credential at
            # all, and preflight is where a missing token becomes a problem.
            print("[init] fallback — no DREMIO_TOKEN in the environment; "
                  "transfer cells will fail preflight until one is set")

    return init


def ensure_init_magic(verbose: bool = True) -> str:
    """Guarantee that `%init` resolves in this kernel.

    Returns "extension" when the real Hub magic is in play, "fallback" when the
    local stand-in was registered. Call it before the `%init` line — registering
    earlier in the same cell is enough, since the magic is looked up at run time.
    """
    try:
        from IPython import get_ipython
    except ImportError as exc:  # pragma: no cover — notebooks always have IPython
        raise RuntimeError("ensure_init_magic() is for notebooks; no IPython here") from exc

    ip = get_ipython()
    if ip is None:
        raise RuntimeError("ensure_init_magic() must run inside an IPython kernel")

    if ip.find_line_magic("init") is not None:
        if verbose:
            print("[init] Dremio extension present — using the real %init")
        return "extension"

    for ext in _EXTENSIONS:
        try:
            ip.run_line_magic("load_ext", ext)
        except Exception:  # noqa: BLE001 — absent or not an extension; try the next
            continue
        if ip.find_line_magic("init") is not None:
            if verbose:
                print(f"[init] loaded {ext} — using the real %init")
            return "extension"

    ip.register_magic_function(_fallback_init(ip), "line", "init")
    if verbose:
        print("[init] no Dremio extension — registered the local fallback %init")
    return "fallback"
