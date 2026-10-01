"""SDI metadata controller: SDI catalogue → DDS ``metadata`` folder.

Two public methods do the work, and the calling code always passes the UUID:

=================================  ================================================
:meth:`SdiController.get_xml`  download a record's ISO 19115-3 XML from SDI
:meth:`SdiController.push_to_dds`  upload it to ``{dds_path}/metadata/{uuid}.xml``
=================================  ================================================

plus :meth:`~SdiController.resolve_series`, which turns a series UUID into its
current release's UUID for callers that only know the series.

The XML goes to DDS as a **document upload**, never through the ingest API:
ingest creates datasets in the catalog, this controller only moves metadata.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from types import TracebackType
from typing import Literal

from ..catalog.errors import CatalogOperationError, EngineStartingError
from ..catalog.rest import CatalogRestClient
from ..dds_documents import DocumentsClient
from . import iso
from .catalogue import SdiCatalogue
from .errors import CatalogPathNotFound, DdsCopyConflict, NotCurrentError, NotIso19115_3, SdiError
from .iso import IsoRecord

DEFAULT_FOLDER = "metadata"
XML_CONTENT_TYPE = "application/xml"
# What Dremio's own catalog source answers (a 400, not a 404) for a by-path
# lookup of a nested item that doesn't exist — seen on the EEA instance, and
# described in catalog/rest.py. An existing path answers 200 as usual.
_MISSING_IN_CATALOG_SOURCE = "Can not get internal item from non-filesystem source"

PushAction = Literal["uploaded", "unchanged", "replaced"]


@dataclass(frozen=True)
class SdiMetadata:
    """One ISO 19115-3 record as SDI served it.

    ``xml`` holds the exact bytes, so what lands in DDS is byte-for-byte what
    SDI returned. ``record`` is the parsed summary (title, edition, dates).
    """

    uuid: str
    xml: bytes = field(repr=False)
    record: IsoRecord = field(repr=False)

    def __repr__(self) -> str:
        return (
            f"SdiMetadata(uuid={self.uuid!r}, title={self.record.title!r}, "
            f"edition={self.record.edition!r}, xml=<{len(self.xml)} bytes>)"
        )

    def provenance_tags(self) -> list[dict[str, str]]:
        """Tags in :meth:`Catalog.setmeta2wiki`'s shape, recording which SDI record this is.

        For people reading the wiki only; nothing reads them back to pick a
        record.
        """
        stamp = self.record.date_stamp
        values = {
            "sdi_record_uuid": ("SDI record UUID", self.uuid),
            "sdi_edition": ("SDI edition", self.record.edition or ""),
            "sdi_date_stamp": ("SDI metadata date", stamp.isoformat() if stamp else ""),
        }
        return [
            {"tag_name": name, "tag_value": value, "tag_title": title}
            for name, (title, value) in values.items()
        ]


@dataclass(frozen=True)
class PushResult:
    """What :meth:`SdiController.push_to_dds` did."""

    dds_path: str
    action: PushAction
    uuid: str


class SdiController:
    """Moves ISO 19115-3 metadata records from SDI into DDS.

    The DDS client is optional at construction: :meth:`get_xml` needs only
    SDI, and :meth:`push_to_dds` builds a :class:`DocumentsClient` from the
    environment (``DDS_BASE_URL``, ``_DREMIO_USER`` / ``_DREMIO_PWD``) on first
    use if none was given.
    """

    def __init__(
        self,
        catalogue: SdiCatalogue | None = None,
        documents: DocumentsClient | None = None,
        catalog: CatalogRestClient | None = None,
    ) -> None:
        self._catalogue = catalogue or SdiCatalogue()
        self._documents = documents
        self._owns_documents = documents is None
        # When given, push_to_dds first checks dds_path exists in the Dremio catalog.
        self._catalog = catalog

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> SdiController:
        """SDI settings from ``SDI_API_URL`` (+ optional ``SDI_USERNAME`` / ``SDI_PASSWORD``).

        DDS settings are read later, by :meth:`push_to_dds`, so extracting
        works without them.
        """
        return cls(SdiCatalogue.from_env(env))

    def __repr__(self) -> str:
        return f"SdiController(catalogue={self._catalogue!r}, documents={self._documents!r})"

    @property
    def catalogue(self) -> SdiCatalogue:
        """The SDI client this controller reads from."""
        return self._catalogue

    @property
    def documents(self) -> DocumentsClient | None:
        """The DDS client this controller pushes with, if one is set yet."""
        return self._documents

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        self._catalogue.close()
        if self._documents is not None and self._owns_documents:
            self._documents.close()

    def __enter__(self) -> SdiController:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # -- public API -------------------------------------------------------

    def get_xml(self, uuid: str) -> SdiMetadata:
        """Download the ISO 19115-3 XML of the SDI record ``uuid``.

        Raises :class:`SdiNotFound` if SDI has no such record,
        :class:`SdiAuthError` if it is not public, :class:`SdiApiError` on any
        other failure, :class:`NotIso19115_3` if the response is not an
        ``mdb:MD_Metadata`` document and :class:`UuidMismatch` if its
        identifier is not ``uuid``.
        """
        uuid = _require_uuid(uuid)
        xml = self._catalogue.get_xml(uuid)
        try:
            record = iso.check(xml, uuid)
        except NotIso19115_3 as exc:
            raise NotIso19115_3(f"SDI record {uuid!r}: {exc}") from exc
        return SdiMetadata(uuid=uuid, xml=xml, record=record)

    def push_to_dds(
        self,
        metadata: SdiMetadata,
        dds_path: str,
        *,
        folder: str = DEFAULT_FOLDER,
        force: bool = False,
        check_catalog: bool = True,
    ) -> PushResult:
        """Upload ``metadata`` to DDS as ``{dds_path}/{folder}/{uuid}.xml``.

        ``dds_path`` is the dataset's DDS location, in catalog format
        (``a.b.c``, converted to ``a/b/c``) or storage format (``a/b/c``);
        ``folder`` is the last path segment, ``metadata`` by default, always
        lowercase.

        If the controller was given a Dremio ``catalog`` client (and
        ``check_catalog`` is left on), ``dds_path`` (without the ``folder``)
        must first exist in the Dremio catalog, otherwise
        :class:`CatalogPathNotFound` is raised and nothing is uploaded.

        Compared with what DDS already holds there:

        - nothing: upload, ``"uploaded"``;
        - the same bytes: no upload, ``"unchanged"``;
        - different bytes and an older metadata date: overwrite, ``"replaced"``;
        - different bytes otherwise (someone edited the DDS copy): raise
          :class:`DdsCopyConflict`, unless ``force=True``.

        The XML is re-checked first, so a hand-built :class:`SdiMetadata`
        cannot put one record under another's UUID.
        """
        iso.check(metadata.xml, metadata.uuid)
        target = metadata_path(dds_path, metadata.uuid, folder=folder)
        if check_catalog and self._catalog is not None:
            self.check_catalog_path(dds_path)
        documents = self._documents_client()

        existing = documents.get(target)
        if existing is None:
            documents.put(target, metadata.xml, content_type=XML_CONTENT_TYPE)
            return PushResult(target, "uploaded", metadata.uuid)
        if existing == metadata.xml:
            return PushResult(target, "unchanged", metadata.uuid)
        if not force:
            _refuse_unless_older(existing, metadata, target)
        documents.put(target, metadata.xml, content_type=XML_CONTENT_TYPE, overwrite=True)
        return PushResult(target, "replaced", metadata.uuid)

    def check_catalog_path(self, dds_path: str) -> str:
        """Raise :class:`CatalogPathNotFound` unless ``dds_path`` exists in the Dremio catalog.

        ``dds_path`` is converted to the catalog path structure first (``/``
        separators become ``.``; see :func:`to_catalog_path`). Returns that
        catalog path. A 404, or the 400 Dremio's own catalog source gives for a
        missing nested item, means absent; any other failure (another 400, a
        timeout) raises :class:`SdiError` rather than reading as "absent".
        """
        if self._catalog is None:
            raise SdiError("no Dremio catalog client configured to check the path against")
        catalog_path = to_catalog_path(dds_path)
        try:
            entity = self._catalog.get_entity(path_segments(dds_path))
        except CatalogOperationError as exc:
            if _MISSING_IN_CATALOG_SOURCE not in str(exc):
                raise SdiError(
                    f"could not check {catalog_path} in the Dremio catalog: {exc}"
                ) from exc
            entity = None
        except EngineStartingError as exc:
            raise SdiError(f"could not check {catalog_path} in the Dremio catalog: {exc}") from exc
        if entity is None:
            raise CatalogPathNotFound(
                f"{catalog_path} does not exist in the Dremio catalog; nothing was uploaded"
            )
        return catalog_path

    def resolve_series(self, series_uuid: str) -> str:
        """The UUID of the one release of ``series_uuid`` that is not superseded.

        Raises :class:`NotCurrentError`, listing every release, if there are
        zero or several, rather than guessing by date.
        """
        series_uuid = _require_uuid(series_uuid)
        series = self.get_xml(series_uuid)
        candidates = self._catalogue.search_by_uuid(list(series.record.children))
        current = [c for c in candidates if c.status != "superseded"]
        if len(current) != 1:
            raise NotCurrentError(series_uuid, candidates)
        return current[0].uuid

    # -- internals --------------------------------------------------------

    def _documents_client(self) -> DocumentsClient:
        if self._documents is None:
            self._documents = DocumentsClient.from_env()
        return self._documents


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


def metadata_path(dds_path: str, uuid: str, *, folder: str = DEFAULT_FOLDER) -> str:
    """``{dds_path}/{folder}/{uuid}.xml``, with slashes normalised.

    ``dds_path`` may be in catalog format (``a.b.c``) or storage format
    (``a/b/c``); see :func:`to_storage_path`.
    """
    parent = to_storage_path(dds_path).strip("/")
    sub = folder.strip().strip("/").lower()
    if not parent:
        raise ValueError("dds_path must not be empty")
    if not sub:
        raise ValueError("folder must not be empty")
    return f"{parent}/{sub}/{_require_uuid(uuid)}.xml"


def _require_uuid(uuid: str) -> str:
    uuid = uuid.strip()
    if not uuid:
        raise ValueError("uuid must not be empty")
    if "/" in uuid:
        raise ValueError(f"invalid SDI UUID {uuid!r}")
    return uuid


def _refuse_unless_older(existing: bytes, metadata: SdiMetadata, target: str) -> None:
    """Raise :class:`DdsCopyConflict` unless the DDS copy is older than SDI's."""
    try:
        theirs = iso.parse(existing).date_stamp
    except NotIso19115_3:
        raise DdsCopyConflict(
            f"{target} exists but is not an ISO 19115-3 record; pass force=True to replace it"
        ) from None
    ours = metadata.record.date_stamp
    if theirs is not None and ours is not None and ours > theirs:
        return
    raise DdsCopyConflict(
        f"{target} differs from SDI record {metadata.uuid!r} and is not older "
        f"(DDS {theirs.isoformat() if theirs else 'undated'}, "
        f"SDI {ours.isoformat() if ours else 'undated'}); it may have been edited in DDS. "
        "Pass force=True to replace it."
    )
