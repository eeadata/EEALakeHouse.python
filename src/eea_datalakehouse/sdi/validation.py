"""Validate a metadata file against the ISO 19115-3 XML schema (XSD).

The schema is ``mdb.xsd`` (``mdb`` 2.0, the version SDI serves) with every
schema it imports — GML 3.2.1, xlink, ISO 19157-2, ISO 19110 — bundled under
``schemas/<host>/<path>`` (refreshed by ``scripts/fetch_iso19115_3_schemas.py``).
A resolver maps each ``http(s)://<host>/<path>`` schema location onto that
copy, so validating never touches the network.

Only the XSD is checked: element order, cardinality and types. ISO 19115-3's
Schematron rules (conditional constraints the XSD cannot express) are not.

The file is parsed with entities, DTDs and network access off, so it cannot
make the parser read local files or fetch URLs; an entity reference it would
have expanded is reported as an issue instead.
"""

from __future__ import annotations

import functools
import os
import re
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lxml import etree

STANDARD = "ISO 19115-3"
SCHEMA_DIR = Path(__file__).parent / "schemas"
ROOT_SCHEMA = "schemas.isotc211.org/19115/-3/mdb/2.0/mdb.xsd"
ROOT_ELEMENT = "{http://standards.iso.org/iso/19115/-3/mdb/2.0}MD_Metadata"

_CLARK_NAME = re.compile(r"\{([^}]*)\}")
_PARSER_POSITION = re.compile(r", line \d+, column \d+$")


@dataclass(frozen=True)
class SchemaIssue:
    """One way the file breaks the schema, where libxml2 found it."""

    line: int
    message: str
    path: str | None = None

    def __str__(self) -> str:
        where = f"line {self.line}" if self.line else "file"
        at = f"\n      at {self.path}" if self.path else ""
        return f"{where}: {self.message}{at}"


@dataclass(frozen=True)
class SdiValidation:
    """Result of :func:`validate`: truthy when ``source`` is schema-valid."""

    source: str
    issues: tuple[SchemaIssue, ...] = ()

    @property
    def valid(self) -> bool:
        return not self.issues

    def __bool__(self) -> bool:
        return self.valid

    def __repr__(self) -> str:
        if self.valid:
            return f"{self.source} is valid {STANDARD} (XSD)"
        lines = "\n".join(f"  {issue}" for issue in self.issues)
        return f"{self.source} is NOT valid {STANDARD} (XSD): {len(self.issues)} issue(s)\n{lines}"


def validate(source: Any) -> SdiValidation:
    """Check ``source`` against the ISO 19115-3 XSD.

    ``source`` is a local file path (``str`` or path-like, e.g. a ``%dds get``
    download), XML ``bytes``, or a record with them (an :class:`SdiMetadata`
    from ``get_xml``, a :class:`DdsFile`). Not well-formed XML, a root other
    than ``mdb:MD_Metadata``, entity references and the schema errors come
    back as issues; a missing file raises :class:`FileNotFoundError`.
    libxml2 reports the first content error inside each element, so fixing one
    can reveal the next.
    """
    name, xml = _read(source)
    parser = etree.XMLParser(
        resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False
    )
    try:
        doc = etree.fromstring(xml, parser)
    except etree.XMLSyntaxError as exc:
        message = _PARSER_POSITION.sub("", exc.msg)
        return SdiValidation(
            name, (SchemaIssue(exc.lineno or 0, f"not well-formed XML: {message}"),)
        )

    if doc.tag != ROOT_ELEMENT:
        found = _short(str(doc.tag), _prefixes(doc))
        message = f"root element is {found}, expected mdb:MD_Metadata ({STANDARD})"
        return SdiValidation(name, (SchemaIssue(doc.sourceline or 0, message),))

    # libxml2's validator fails internally on an entity it was told not to expand.
    entities = tuple(
        SchemaIssue(ref.sourceline or 0, f"entity {ref.text} is not allowed (it is not expanded)")
        for ref in doc.iter(etree.Entity)
    )
    if entities:
        return SdiValidation(name, entities)

    schema = _schema()
    if schema.validate(doc):
        return SdiValidation(name)
    prefixes = _prefixes(doc)
    issues = tuple(
        SchemaIssue(error.line, _short(error.message, prefixes), error.path or None)
        for error in schema.error_log
    )
    return SdiValidation(name, issues)


def _read(source: Any) -> tuple[str, bytes]:
    """(a name for messages, the XML bytes) of whatever :func:`validate` accepts."""
    if isinstance(source, bytes | bytearray):
        return "XML", bytes(source)
    if isinstance(source, str | os.PathLike):
        path = Path(source)
        if path.is_dir():
            raise IsADirectoryError(f"{str(path)!r} is a folder, not a metadata file")
        if not path.is_file():
            raise FileNotFoundError(f"no file {str(path)!r}")
        return str(path), path.read_bytes()
    for attr, label in (("xml", "uuid"), ("data", "name")):
        value = getattr(source, attr, None)
        if isinstance(value, bytes):
            return str(getattr(source, label, type(source).__name__)), value
    raise TypeError(
        "validate takes a file path, XML bytes or a %sdi get_xml record, "
        f"not {type(source).__name__}"
    )


class _BundledSchemas(etree.Resolver):  # type: ignore[misc]  # lxml has no stubs
    """Serve ``http(s)://<host>/<path>`` schema locations from :data:`SCHEMA_DIR`."""

    def resolve(self, url: str, pubid: str | None, context: Any) -> Any:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme not in ("http", "https"):
            return None  # a relative include, already a file under SCHEMA_DIR
        local = SCHEMA_DIR / parts.netloc / parts.path.lstrip("/")
        if not local.is_file():
            raise LookupError(
                f"schema {url} is not bundled; run scripts/fetch_iso19115_3_schemas.py"
            )
        return self.resolve_filename(str(local), context)


@functools.cache
def _schema() -> etree.XMLSchema:
    """The compiled ISO 19115-3 schema, built once per process."""
    parser = etree.XMLParser(resolve_entities=False, no_network=True)
    parser.resolvers.add(_BundledSchemas())
    return etree.XMLSchema(etree.parse(str(SCHEMA_DIR / ROOT_SCHEMA), parser))


def _prefixes(doc: Any) -> dict[str, str]:
    """Namespace URI -> the prefix the document itself uses for it."""
    return {uri: prefix for prefix, uri in doc.nsmap.items() if prefix}


def _short(text: str, prefixes: dict[str, str]) -> str:
    """``{uri}name`` -> ``prefix:name`` wherever the document gives ``uri`` a prefix."""

    def swap(match: re.Match[str]) -> str:
        prefix = prefixes.get(match.group(1))
        return f"{prefix}:" if prefix else match.group(0)

    return _CLARK_NAME.sub(swap, text)
