"""Dataset paths in the two shapes DDS and Dremio use.

- Catalog format: ``catalog.water.bwd``, Dremio SQL's dotted path, with a name
  that is not a plain identifier in double quotes (``catalog."v1.0"``).
- Storage format: ``catalog/water/bwd``, the folders as DDS stores them.

Every function here accepts either form.
"""

from __future__ import annotations

import re


def path_segments(path: str) -> list[str]:
    """The folder names in ``path``, given in catalog or storage format.

    - A catalog path (``a.b.c``) is split on ``.``.
    - A path with a ``/`` outside quotes is storage format: it is split on
      ``/`` instead, so its dots are kept (``a/v1.0``).
    - A segment in Dremio's double quotes (``"a.b"``, as SQL writes names with
      dots, spaces or brackets) is one segment whatever it contains; the quotes
      are removed and an escaped ``""`` becomes ``"``.

    Raises ``ValueError`` on an unclosed quote.
    """
    text = path.strip()
    separator = "/" if "/" in _unquoted(text) else "."
    segments: list[str] = []
    current: list[str] = []
    quoted = False
    i = 0
    while i < len(text):
        char = text[i]
        if char == '"':
            if quoted and text[i + 1 : i + 2] == '"':  # "" inside quotes is a literal "
                current.append('"')
                i += 2
                continue
            quoted = not quoted
        elif char == separator and not quoted:
            segments.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        i += 1
    if quoted:
        raise ValueError(f"unclosed quote in path {path!r}")
    segments.append("".join(current).strip())
    return [segment for segment in segments if segment]


def to_storage_path(path: str) -> str:
    """``path`` in DDS's data storage format: folder names joined with ``/``.

    ``catalog."bathing water"."v1.0"`` becomes ``catalog/bathing water/v1.0``,
    the folders as they exist in the Dremio catalog; see :func:`path_segments`.
    """
    return "/".join(path_segments(path))


def _unquoted(text: str) -> str:
    """``text`` with every double-quoted part removed."""
    out: list[str] = []
    quoted = False
    for char in text:
        if char == '"':
            quoted = not quoted  # an escaped "" toggles twice, which is a no-op here
        elif not quoted:
            out.append(char)
    return "".join(out)


_PLAIN_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def to_catalog_path(path: str) -> str:
    """``path`` in the Dremio catalog path structure: folder names joined with ``.``.

    ``catalog/water_management_resources/bwd`` becomes
    ``catalog.water_management_resources.bwd``. A name that is not a plain
    identifier (it has a dot, space, hyphen, bracket, ...) is double-quoted,
    as Dremio SQL writes it: ``catalog/bathing water/v1.0`` becomes
    ``catalog."bathing water"."v1.0"``.
    """
    return ".".join(
        segment if _PLAIN_NAME.fullmatch(segment) else '"' + segment.replace('"', '""') + '"'
        for segment in path_segments(path)
    )
