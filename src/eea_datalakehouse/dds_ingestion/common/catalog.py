"""Make sure a catalog path exists before ingest writes into it.

Ingest does not create its target. `commit` runs `CREATE TABLE` + `COPY INTO`,
and Dremio refuses both if the containing namespace is absent:

    Namespace does not exist: water_management_resources.bathing_water.bwd.draft.bw_assessment

So the folders have to be there first. This module walks the target path and
creates what is missing through the **Document Service** — never Dremio's API
directly, because DDS is what owns catalog structure in this platform (it is
also what writes the folder's wiki page for meta tags).

Two things worth knowing about how it decides.

**Existence is established by listing the parent, not by probing the path.**
A missing folder does not answer 404: DDS returns 502 wrapping Dremio's
"catalog lookup failed (400)". Treating an error as "missing" would turn a DDS
or Dremio outage into a burst of folder creation. Listing the parent only ever
acts on a 200, so an outage surfaces as an error instead of a write.

**Taxonomy levels are never created.** The path is
`{source}/{domain}/{subdomain}/{dataflow}/...`; the source, domain and subdomain
must already exist, and a missing one raises rather than being invented — per
the repository CLAUDE.md: "Don't invent catalog paths. If the domain/subdomain
isn't in lakehouse_structure.md, that is a taxonomy question to raise, not a
folder to create." Only the dataflow level and below (`draft`, the dataset
folder, `reference`) are created here, which is exactly what a new dataflow
legitimately owns.
"""
from __future__ import annotations

import httpx

# Segments before this index are taxonomy (source / domain / subdomain) and are
# required to exist. From this index down is the dataflow's own space.
DATAFLOW_DEPTH = 3


def _children(base_url: str, token: str, path: str, timeout: float) -> list[str]:
    """Names directly under `path`. Raises unless the listing genuinely succeeds."""
    r = httpx.get(
        f"{base_url}/api/v1/catalog/{path}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
    )
    if r.status_code != 200:
        raise RuntimeError(
            f"cannot list {path!r} ({r.status_code}): {r.text[:200]}"
        )
    return [c["name"] for c in r.json().get("children", [])]


def _create(base_url: str, token: str, path: str, timeout: float) -> None:
    r = httpx.post(
        f"{base_url}/api/v1/catalog/{path}/folder",
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
    )
    # 409 means someone else got there first — the post-condition still holds.
    if r.status_code not in (200, 201, 409):
        raise RuntimeError(
            f"could not create folder {path!r} ({r.status_code}): {r.text[:300]}"
        )


def ensure_catalog_path(
    base_url: str,
    token: str,
    path: str,
    *,
    dry_run: bool = False,
    timeout: float = 120.0,
    dataflow_depth: int = DATAFLOW_DEPTH,
) -> list[tuple[str, str]]:
    """Ensure every level of `path` exists, creating the dataflow levels.

    Returns one `(path, action)` per level, where action is ``exists``,
    ``created`` or ``would create`` (under `dry_run`). Raises if a taxonomy
    level is missing, or if the catalog cannot be listed at all.
    """
    segments = [s for s in path.strip("/").split("/") if s]
    if not segments:
        raise ValueError("empty catalog path")

    report: list[tuple[str, str]] = []
    walked = segments[0]                     # the Dremio source itself
    kids = _children(base_url, token, walked, timeout)   # also proves it exists
    report.append((walked, "exists"))

    for depth, name in enumerate(segments[1:], start=1):
        current = f"{walked}/{name}"
        if name in kids:
            report.append((current, "exists"))
            kids = _children(base_url, token, current, timeout)
        elif depth < dataflow_depth:
            level = {1: "domain", 2: "subdomain"}.get(depth, "taxonomy level")
            raise RuntimeError(
                f"{level} {name!r} does not exist under {walked!r}. This is a "
                "taxonomy question to raise, not a folder to create — see "
                "lakehouse_structure.md and the repository CLAUDE.md."
            )
        elif dry_run:
            report.append((current, "would create"))
            kids = []                        # nothing below it can exist either
        else:
            _create(base_url, token, current, timeout)
            report.append((current, "created"))
            kids = _children(base_url, token, current, timeout)
        walked = current

    return report


def print_report(report: list[tuple[str, str]]) -> None:
    """One line per level, indented by depth so the tree is readable."""
    for path, action in report:
        depth = path.count("/")
        mark = {"exists": " ", "created": "+", "would create": "~"}.get(action, "?")
        print(f"  {mark} {'  ' * depth}{path.split('/')[-1]:32} {action}")
