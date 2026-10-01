# SDI integration — development plan — **PROPOSED**

> Status: proposed, 2026-09-30. **Steps 1–3 built 2026-10-01** (`sdi/`,
> `dds_documents/`, with tests); step 0 (confirming the DDS document endpoints)
> and step 4 (a run against a DDS test instance) are still open.
>
> Scope (2026-10-01): **extraction from SDI and upload to DDS only.**
> Publication to SDI is deferred; see [Not in this plan](#not-in-this-plan).

"SDI" here means the EEA Spatial Data Infrastructure catalogue: the "EEA
geospatial data catalogue", GeoNetwork **4.4.9**, at
`https://sdi.eea.europa.eu/catalogue`. It holds the ISO 19115-3 metadata record
for each dataset.

## The short version

Add an **SDI controller** to `eea_datalakehouse` with one route, **Extract**
(SDI → DDS):

- It takes a dataset's SDI UUID from the calling code.
- It downloads that record's ISO 19115-3 XML from the SDI REST API.
- It uploads the XML to the dataset's `metadata/` folder in DDS.

The SDI REST API base URL is read from `SDI_API_URL`, e.g.
`SDI_API_URL=https://sdi.eea.europa.eu/catalogue`. The client appends
`/srv/api/...`.

The controller sits on three pieces:

- **`sdi/catalogue.py`**: a read-only SDI REST client.
- **`dds_documents/`**: a DDS documents client, which this library doesn't
  have yet. The existing DDS client, `dds_ingestion`, only covers
  `/api/v1/ingest`.
- **`sdi/iso.py`**: a small ISO 19115-3 reader for the few fields the
  controller checks (UUID, title, edition, dates).

## Where things stand in this repo

- **Before this plan, no SDI code.** `src/eea_datalakehouse/` had `catalog`,
  `dds_ingestion` and `notebook`, and nothing talked to SDI.
- **Unused dependencies.** `pyproject.toml` carries `lxml` ("ISO 19115-3
  metadata records"), `beautifulsoup4` ("SDI 'direct download' is an HTML
  landing page") and `xlrd` ("some older SDI deliveries"). Nothing imports
  any of them.
- **The DDS connection already exists.** `dds_ingestion/credentials.py`
  resolves `DDS_BASE_URL` and the Dremio credentials (`_DREMIO_USER` /
  `_DREMIO_PWD`). `dds_ingestion/client.py` sends the PAT as
  `Authorization: Bearer`. The documents client reuses both.
- **Wiki metadata can already be written.** `catalog.setmeta2wiki()`,
  `getmetafromwiki()` and `settagsto()` are there to record provenance on a
  Dremio entity. No SDI data feeds them yet.
- **No SDI configuration yet.** `debugger/.env.example` has no SDI entries.

## Verified against the live SDI catalogue (2026-09-30)

These are public, read-only calls, using the bathing water series `c3858959…`
and its release `070d9baa…` as the example.

- **`GET {SDI_API_URL}/srv/api/records/{uuid}/formatters/xml`**
  - Returns `200 application/xml`, an `mdb:MD_Metadata` in the ISO 19115-3
    namespaces: about 32 KB for a release, 56 KB for a series.
  - No authentication is needed for public records.
  - This is the route Extract uses.
- **`GET {SDI_API_URL}/srv/api/site`** reports `EEA geospatial data catalogue`,
  version `4.4.9`.
- **A series record lists its releases** as `mri:associatedResource` with
  `associationType = isComposedOf` (21 of them for bathing water).
  - A release record doesn't point back to its series.
  - GeoNetwork's `/related?type=children` and a `parentUuid` search both
    return nothing.
- **`POST {SDI_API_URL}/srv/api/search/records/_search`** with
  `{"query": {"terms": {"uuid": [...]}}}` returns each record's:
  - `cl_status` (`superseded` or absent)
  - `resourceEdition`
  - `publicationDateForResource`
  - `resourceTemporalExtentDetails`
  - `resourceTitleObject`

  Its `*DateRange` fields are shifted to UTC (31 December reads
  `…-12-30T23:00Z`). Use the plain-date fields instead.

## DDS interface assumptions (confirm with the DDS team)

This repo only calls DDS's ingest API. Extract needs its document endpoints.
The plan assumes the following. `dds_documents/client.py` is written against
these assumptions, each held in a module constant, and is adjusted once the DDS
team confirms them:

- **Upload:** `PUT {DDS_BASE_URL}/api/v1/files/{path}`, where the request body
  is the file, stored as a document (never promoted to a Dremio table).
  Requires write access on the parent folder.
- **Overwrite is explicit.** An existing file is rejected unless the request
  says to overwrite it. Assumed: `?overwrite=true|false`, and **409** on
  conflict (`DocumentExistsError`).
- **Download:** `GET {DDS_BASE_URL}/api/v1/files/{path}`, used to compare with
  what's already there (404 when absent). Listing is assumed to be
  `GET /api/v1/files?prefix={folder}`, returning paths.
- **A `metadata/` document folder at each catalog node.** What `{path}` is for
  a dataset's metadata folder: the catalog path joined with `/`, then
  `metadata/`, then the file name? Is the path case-folded? Does it include
  the Dremio source prefix? Is the folder created on first upload?
- **Authentication** is the same `Authorization: Bearer <Dremio PAT>` the
  ingest client sends.
- **Size limit** for an upload. ISO records are tens of KB, so any sane limit
  is fine, but the client should report it clearly.

## Design

### Layout

```
src/eea_datalakehouse/
  sdi/
    __init__.py      # public API: SdiController, SdiMetadata, PushResult, errors
    config.py        # SDI_API_URL (+ optional SDI_USERNAME / SDI_PASSWORD), from env
    catalogue.py     # read-only SDI REST client: get_xml(), search(), site()
    iso.py           # ISO 19115-3 read: uuid, title, edition, dates, hierarchy level
    controller.py    # SdiController.get_xml(), push_to_dds(), resolve_series()
    errors.py        # SdiError, SdiNotFound, SdiAuthError, UuidMismatch, NotIso19115_3, NotCurrentError
  dds_documents/
    __init__.py
    client.py        # DocumentsClient: put(), get(), exists(), list()
```

- **`dds_documents` is its own package**, next to `dds_ingestion`, and the XML
  reaches DDS through the **DDS REST API, the same way `IngestClient` does**:

  | | `IngestClient` today | `DocumentsClient` |
  |---|---|---|
  | Base URL | `DDS_BASE_URL` | same |
  | Auth | `Authorization: Bearer <PAT>` (`_DREMIO_PWD`), never logged or in `repr` | same `DremioCreds` |
  | Shape | one method per endpoint, typed results in `models.py` | same |
  | Errors | `IngestApiError(status, message, where=…)`, error body capped | `DocumentsApiError`, same shape |
  | Transport | one `httpx.Client`, context manager | same |
  | Tests | `respx` | `respx` |

  `DremioCreds` and the `resolve_*` helpers move out of
  `dds_ingestion/credentials.py` into a shared module, so both clients import
  them from one place.

  **The XML is a document upload, not an ingest** (item 3 in
  [Decide first](#decide-first)).
  - Ingest (`/api/v1/ingest/*`) creates datasets in the catalog: `commit`
    registers the upload as a Dremio table, and `DataFormat` is
    `parquet | csv | json`.
  - The SDI controller only moves metadata, so it never calls the ingest
    API. `DocumentsClient` shares `IngestClient`'s conventions, not its
    endpoints.
- **Transport is `httpx`**, like `dds_ingestion`, so everything is tested with
  `respx` as the rest of the suite is.
- **ISO parsing uses `xml.etree.ElementTree`** with explicit namespaces. The
  controller reads a handful of elements and doesn't need `lxml`.
- **Configuration comes from environment variables**, as `DDS_BASE_URL` does
  today:
  - `SDI_API_URL` (required).
  - `SDI_USERNAME` / `SDI_PASSWORD` (optional), sent as basic auth, only for
    reading non-public records. GeoNetwork 4.4 has no API-key mechanism.
  - `DDS_BASE_URL`, `_DREMIO_USER`, `_DREMIO_PWD`, as today.

  All three are in `debugger/.env.example`.

### Public API

Two public methods, as built. The UUID always comes from the calling code:

```python
from eea_datalakehouse.sdi import SdiController

with SdiController.from_env() as sdi:   # SDI_API_URL; DDS settings read on first push
    metadata = sdi.get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")
    metadata.record   # IsoRecord(uuid, title, edition, hierarchy_level, created, revised, children)

    res = sdi.push_to_dds(
        metadata,
        "catalog/water_management_resources/bathing_water/bwd",  # the dataset's DDS path
        # folder="metadata"  (the default last segment)
        # force=False        (True replaces a DDS copy that was edited)
    )
res.dds_path      # "catalog/.../bwd/metadata/070d9baa-448d-4168-8514-7dadb3ad876d.xml"
res.action        # "uploaded" | "unchanged" | "replaced"
```

The full DDS path above the `metadata` folder is still to be agreed;
`push_to_dds` takes it `/`-joined from the caller, so nothing in the library
fixes it yet.

**The destination is the `metadata` folder by default** (item 1 in
[Decide first](#decide-first)):

- The file goes to `{dds_path}/metadata/{uuid}.xml` (`metadata_path()`).
- `folder=` overrides the sub-folder name. It is a keyword argument with the
  default `"metadata"`, held in one constant (`DEFAULT_FOLDER`), so it is
  changed in one place.
- The name is written lowercase. DDS may fold case (see
  [DDS interface assumptions](#dds-interface-assumptions-confirm-with-the-dds-team)),
  so `METADATA` and `metadata` must resolve to the same folder.

### Extract (SDI → DDS)

1. **Fetch** `GET {SDI_API_URL}/srv/api/records/{uuid}/formatters/xml`, for
   the `uuid` the calling code passed in.
   - A 404 raises `SdiNotFound`.
   - A 401/403 on a non-public record raises `SdiAuthError`, with the hint to
     set `SDI_USERNAME` / `SDI_PASSWORD`.
2. **Check that it's the right record.** `iso.py` parses the XML:
   - The root must be `mdb:MD_Metadata`, otherwise `NotIso19115_3`. That covers
     a 19139 `gmd:MD_Metadata` or an HTML error page.
   - `mdb:metadataIdentifier/…/mcc:code` must equal the requested UUID,
     otherwise `UuidMismatch`.
3. **Name the file** `{folder}/{uuid}.xml` under the dataset's DDS path, where
   `folder` defaults to `metadata`.
   - The UUID in both the file name and the content is what ties a DDS file to
     its SDI record, so no separate link has to be stored.
   - One file per record means a new release (new UUID) lands next to the old
     one instead of overwriting it.
   - Whether to keep only the current release is in
     [Decide first](#decide-first).
4. **Compare** with what's already in DDS (a 404 means new).
   - The bytes are equal: `unchanged`, no upload.
   - They differ and the SDI record's revision date (`mdb:dateInfo`) is newer:
     upload with overwrite and report `replaced`.
   - They differ and the DDS copy is not older (newer, same date, undated or
     not ISO at all), meaning someone edited it in DDS: raise
     `DdsCopyConflict`. Extract never silently overwrites local edits.
     `force=True` overrides.
5. **Upload** it as `application/xml` through `DocumentsClient.put()`.
6. **Return** a `PushResult(dds_path, action, uuid)`. Provenance on the wiki is
   the caller's choice: `metadata.provenance_tags()` returns
   `sdi_record_uuid` / `sdi_edition` / `sdi_date_stamp` in the `tags=` shape
   of the existing `catalog.setmeta2wiki()`; the controller writes nothing to
   Dremio itself.

### Series resolution (Extract by series)

A dataset is often a **series**, with one record per release. Letting Extract
take a series UUID means callers don't have to track which release is
current:

- **`resolve_series(series_uuid)`**:
  - Read the series XML and collect its `isComposedOf` children.
  - Make one `_search` call for their status.
  - The current release is the one child that isn't `superseded`.
  - If there are zero or several, raise `NotCurrentError` listing each
    candidate's UUID, title, edition, publication date and status, rather
    than guessing by date.
- The caller then passes the returned UUID to `get_xml()`, so the UUID
  that is extracted is still one the calling code handed over.
- **Live check (2026-10-01):** the bathing water series `c3858959…` has
  **two** releases that are not superseded (`070d9baa…`, 2025 v1.0, and
  `6b7b7f62…`, "Bathing water quality in Europe", 2020), so
  `resolve_series` raises `NotCurrentError` for it. That is the intended
  behaviour; pass the release UUID directly.

### Notebook surface

Two magics in `notebook/magics.py`, next to `%catalog` / `%ingest`, one session
per kernel each, built on first use from the kernel environment:

- **`%sdi`** (`SdiSession`) reads SDI: `get_xml`, `resolve_series`.
- **`%metadata`** (`MetadataSession`) pushes metadata files to DDS:
  `push_to_dds`, `dds_base_url`.

```
%sdi help
%sdi get_xml("070d9baa-448d-4168-8514-7dadb3ad876d")
%sdi resolve_series("c3858959-90da-4c1b-b9ca-492db0e514df")

%metadata help
%metadata dds_base_url()
%metadata push_to_dds("catalog/water_management_resources/bathing_water/bwd")
```

- Each call runs immediately, like `%catalog`; there is nothing to commit.
- `%metadata push_to_dds` uploads the record `%sdi get_xml` fetched last
  (looked up at push time) unless `metadata=` is given.
- Every failure prints one `sdi error: …` / `metadata error: …` line instead
  of a traceback.
- `DDS_BASE_URL` is read, when `%metadata` builds its session, from the first
  `.env` in the notebook's folder or a parent; the kernel environment's value
  applies only if no `.env` sets it. `%metadata dds_base_url()` shows which is
  in use.
- The DDS upload's Dremio identity is `_DREMIO_USER` / `_DREMIO_PWD` (as
  `%ingest`), falling back to `DREMIO_USERNAME` / `DREMIO_TOKEN` (as
  `%catalog`).

## Steps

Each step lands with its tests, mypy clean and ruff clean.

0. **Confirm the [DDS interface](#dds-interface-assumptions-confirm-with-the-dds-team)**
   with the DDS team.
1. ✅ **`sdi/config.py`, `sdi/catalogue.py`, `sdi/iso.py`.**
   - Test records are built in `tests/sdi/conftest.py` with the same structure
     the live catalogue serves.
   - `respx` mocks the HTTP calls.
   - One live test, skipped unless `SDI_NETWORK_TESTS=1`.
2. ✅ **`dds_documents/client.py`**, built like `IngestClient`: `put` / `get` /
   `exists` / `list` over the document endpoints. It makes no ingest calls.
   - Written against the assumed contract; each endpoint, the `overwrite`
     query parameter and the 409 conflict are module constants, to adjust
     once step 0 confirms them. The size limit and path case wait for step 0.
   - The credential code stays in `dds_ingestion/credentials.py` for now and
     is imported from there.
3. ✅ **`SdiController.get_xml()` / `push_to_dds()`**, all of Extract, plus
   `resolve_series()`.
   - Cases: new, unchanged, replaced, refused because the DDS copy was edited,
     `force`, `UuidMismatch`, not ISO 19115-3, and zero or several current
     releases.
   - `debugger/debug_run.py` has `run_sdi_extract()` for the bathing water
     release (extracts for real; uploads only with `DRY_RUN = False`).
4. **Prove Extract against a DDS test instance.** Extract the bathing water
   release, and check the file appears in the dataset's `metadata/` folder.
5. ✅ **`%sdi` and `%metadata` magics** over `sdi/session.py`'s `SdiSession`
   (`get_xml`, `resolve_series`) and `MetadataSession` (`push_to_dds`, which
   defaults to the last `%sdi` record, and `dds_base_url`). Built on first use
   from the kernel environment like `%catalog` / `%ingest`.
   Worked example: `debugger/sdi_session_example.ipynb`. The README
   section, Layout rows, docstrings and `.env.example` entries are done.
6. **Dependencies.**
   - Drop `lxml`, since `iso.py` uses ElementTree.
   - Drop `beautifulsoup4`, which nothing here needs.
   - Correct `xlrd`'s comment.

## Decide first

1. ✅ **Decided (2026-09-30): the destination is the `metadata` folder by
   default**, i.e. `{dds_path}/metadata/{uuid}.xml`.
   - The calling code passes `dds_path`, so it picks the catalog node
     (dataflow, table, and so on). The full path convention is still to be
     agreed.
   - `folder=` overrides the sub-folder.

   Still open, and it doesn't block building: one file per SDI record
   (`{uuid}.xml`, several releases side by side, the default in this plan), or
   a single `current.xml` per dataset?
2. ✅ **Decided (2026-09-30): the SDI UUID is always a parameter from the
   calling code.**
   - `get_xml(uuid)` / `resolve_series(series_uuid)` take it explicitly.
   - The library never looks it up: not from the catalog, not from a wiki
     key, not from a file name.
   - `provenance_tags()` only records the UUID for people to read. Nothing
     reads it back to pick a record.
3. ✅ **Decided (2026-09-30): the XML goes to DDS as a document upload, not
   through ingest.**
   - Ingest is for creating datasets in the catalog. The SDI controller is
     only about metadata.
   - `DocumentsClient` uses DDS's document endpoints (`PUT` /
     `GET /api/v1/files/{path}`). Their exact contract is still to confirm,
     see
     [DDS interface assumptions](#dds-interface-assumptions-confirm-with-the-dds-team).

## Not in this plan

- **Publication to SDI (DDS → SDI).** Deferred (2026-10-01). When it is taken
  up, it needs its own decisions first: SDI write credentials (an editor
  account; an unauthenticated `PUT /srv/api/records` returns 403, and
  GeoNetwork 4 also needs its XSRF cookie/header pair), EEA SDI's editorial
  workflow, and who may publish. The Extract design leaves room for it:
  `catalogue.py` can gain a write side, and the `{uuid}.xml` naming already
  ties each DDS file to its SDI record.
- **Editing metadata.** Editing the XML itself is done in DDS by people. The
  controller only moves validated ISO 19115-3 into DDS.
- **Full schema validation** against the ISO 19115-3 XSDs or INSPIRE rules.
  GeoNetwork has already validated what it serves.
- **ISO 19139 (`gmd:`) records.** The controller refuses them rather than
  converting them.
- **Downloading the data** behind an SDI record. This plan covers metadata
  only.
- **Search and harvesting** by keyword or title.
- **CSW and OGC API Records.** The GeoNetwork REST API is enough.
- **Changes to DDS.** The plan uses DDS's existing document endpoints, as
  confirmed.

## Sources

- This repo:
  - `src/eea_datalakehouse/dds_ingestion/client.py` and `credentials.py`
  - `src/eea_datalakehouse/catalog/operations.py` (`setmeta2wiki`,
    `getmetafromwiki`)
  - `src/eea_datalakehouse/notebook/magics.py`
  - `docs/dev-notes.md`
  - `pyproject.toml`
- The live SDI catalogue, 2026-09-30 (see
  [Verified](#verified-against-the-live-sdi-catalogue-2026-09-30))
- DDS: the assumptions above, to be confirmed with the DDS team
