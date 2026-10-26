"""Refresh the ISO 19115-3 XSDs bundled in ``eea_datalakehouse/sdi/schemas``.

Downloads ``mdb.xsd`` and every schema it imports or includes, transitively
(GML 3.2.1, xlink, ISO 19157-2, ISO 19110...), into
``schemas/<host>/<path>``. ``eea_datalakehouse.sdi.validation`` maps each
``http(s)://<host>/<path>`` schema location onto that file, so validating
never needs the network. Run it from the repository root::

    python scripts/fetch_iso19115_3_schemas.py
"""

from __future__ import annotations

import pathlib
import sys
import urllib.parse
import urllib.request

from lxml import etree

ROOT_SCHEMA = "https://schemas.isotc211.org/19115/-3/mdb/2.0/mdb.xsd"
OUT = pathlib.Path(__file__).resolve().parents[1] / "src/eea_datalakehouse/sdi/schemas"
_XS = "{http://www.w3.org/2001/XMLSchema}"


def fetch(root_url: str, out: pathlib.Path) -> set[str]:
    seen: set[str] = set()
    todo = [root_url]
    while todo:
        url = todo.pop()
        if url in seen:
            continue
        seen.add(url)
        with urllib.request.urlopen(url, timeout=60) as response:  # noqa: S310 — fixed hosts
            data = response.read()
        parts = urllib.parse.urlsplit(url)
        dest = out / parts.netloc / parts.path.lstrip("/")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        parser = etree.XMLParser(resolve_entities=False, no_network=True)
        schema = etree.fromstring(data, parser)
        for ref in schema.iter(f"{_XS}import", f"{_XS}include", f"{_XS}redefine"):
            location = ref.get("schemaLocation")
            if location:
                todo.append(urllib.parse.urljoin(url, location))
    return seen


if __name__ == "__main__":
    urls = fetch(ROOT_SCHEMA, OUT)
    print(f"{len(urls)} schema(s) under {OUT}", file=sys.stderr)
