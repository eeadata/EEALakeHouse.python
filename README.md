# EEADataLakehouse

Two main class domains for the EEA data lakehouse:

- `eea_datalakehouse.dds_ingestion` — notebook-side client for the Dremio Document
  Service (DDS) Ingest API (DI-8.4/8.5). `FolderIngest` transfers a folder of
  data files to S3 (via DDS-issued presigned URLs only — never an S3 SDK or
  S3 credentials) and registers it as a Dremio table, coordinated entirely
  through the DDS REST API.
- `eea_datalakehouse.catalog` — Dremio catalog operations once data has
  landed: converting a table to a view, copying/moving/deleting catalog
  entries, managing folders, wiki text and tags, and listing what's there.
  `datacopy`/`datamove` always run over Arrow Flight SQL; everything else
  runs over Dremio's REST SQL Jobs API (folders/wiki/tags have no SQL
  equivalent at all, so those go through Dremio's own catalog REST API
  directly) — and every operation treats a stalled call as a Dremio engine
  cold-starting rather than a failure, so it can be remembered and retried
  later instead of blocking.

## Install

[![main](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fapi.github.com%2Frepos%2Feeadata%2FEEALakeHouse.python%2Freleases&query=%24%5B%3F%28%40.prerelease%3D%3Dfalse%29%5D.tag_name&label=main&color=blue)](https://github.com/eeadata/EEALakeHouse.python/releases/latest)
[![staging](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fapi.github.com%2Frepos%2Feeadata%2FEEALakeHouse.python%2Freleases&query=%24%5B%3F%28%40.prerelease%3D%3Dtrue%29%5D.tag_name&label=staging&color=orange)](https://github.com/eeadata/EEALakeHouse.python/releases)

These badges are live — each one queries the GitHub API directly and always shows whatever tag
is *currently* released for that branch, updating on its own every time `main`/`staging` cuts a
new release (see [Releasing a new version](#releasing-a-new-version)).

`@main`/`@staging` always installs whatever was most recently released for that branch — a
floating tag sharing the branch's own name, moved forward to the latest release automatically
each time one is cut, so there's nothing to look up or keep in sync yourself:

```bash
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@main"       # latest stable
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@staging"    # latest early access
```

# staging's latest release (early access) — pin to the tag the "staging" badge above shows
pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.11-staging"
```

## Usage

Dremio credentials are read from the injected kernel env vars `_DREMIO_USER` /
`_DREMIO_PWD`, and the service URL from `DDS_BASE_URL`. None of these are ever
logged or printed.

```python
from eea_datalakehouse.dds_ingestion import FolderIngest

outcome = FolderIngest(
    folder="./my_data",
    target_catalog_path="biodiversity.uploads",
    data_format="parquet",      # one of parquet | csv | json
    intent="read_only",         # or "editable"
    conflict_mode="fail",
    parallelism=4,               # concurrent uploads (default 4)
).run()

print(outcome.commit.table_path, outcome.commit.record_count)
```

`run()` performs `begin → upload(all files) → commit`. Re-running the same
session resumes by skipping files the server reports as already uploaded.

```python
from eea_datalakehouse.catalog import Catalog, EngineStartingError

# base_url/token are Dremio's own (not DDS). `username` is only needed for
# datacopy/datamove (Arrow Flight's basic-auth handshake) — everything else
# runs over Dremio's REST SQL Jobs API regardless of EEA_CATALOG_TRANSPORT.
with Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME) as catalog:
    try:
        catalog.table2view(
            "bwd.consumer", "bwd.draft.bw_assessment", idempotency_key="bwd-v2025_1"
        )
    except EngineStartingError:
        # A cold Dremio engine looks like a stalled call, not a failure — the
        # attempt is remembered under its idempotency_key; call it again later:
        catalog.retry_pending("bwd-v2025_1")

    info = catalog.gettableitemsfrom("bwd.versions.v2025_1.assessments", idempotency_key="check-1")
    print(info.schema, info.row_count)             # schema + row count, no rows fetched

    catalog.gettablesfrom("bwd", idempotency_key="list-1")   # recurses into every subfolder
# `with` calls catalog.close() on exit, disposing the REST/Flight session(s).
# If you don't use `with`, call catalog.close() yourself when done — a
# process-exit/SIGTERM fallback covers a caller that forgets either way.
```

Every operation takes `idempotency_key=` (keyword-only). On `EngineStartingError` — a stalled
call, most likely a cold-starting Dremio engine, not a real failure — the attempt is remembered
under that key; call `catalog.retry_pending(idempotency_key)` later to pick it back up with the
exact same arguments.

### Catalog operations

**Table/view lifecycle**
- `table2view(view_path, source_path, create_target_folder=True)` — the one-time `DROP TABLE` +
  `CREATE VIEW` transition for a location that started as a physical table (straight off ingest)
  and is moving to being a view. Checks `source_path` exists first; by default also creates
  `view_path`'s containing folder if missing.
- `draft2version(draft_path, version_path)` / `publishversion(consumer_view_path, version_path)`
  — **not implemented yet** (both raise `NotImplementedError`); promoting a draft table into a
  permanent version and repointing a consumer-facing view at it.
- `deleteview(view_path)` — `DROP VIEW IF EXISTS`.

**Copying/moving data** — always over Arrow Flight SQL, never REST, regardless of
`EEA_CATALOG_TRANSPORT` (needs `username=` on `Catalog(...)` for the Flight auth handshake):
- `datacopy(source_path, target_path, overwrite=False, create_target_folder=False)` — `CREATE
  TABLE ... AS SELECT`. If `target_path` is an existing *folder* rather than a specific
  table/view path, the source's own name is appended to it (`cp source dest/` semantics).
  `overwrite=False` (the default) raises if the (possibly folder-adjusted) target already
  exists; `overwrite=True` detects whatever is actually there — it might be a view, not a table
  — and drops it with the matching verb first.
- `datamove(source_path, target_path, entry_type=None, overwrite=False,
  create_target_folder=False)` — copies then drops the source. `entry_type` (`"TABLE"`/`"VIEW"`)
  is auto-detected via `INFORMATION_SCHEMA` when not given, rather than trusting a caller to get
  it right. Same folder-append and `overwrite` behavior as `datacopy`. After the move, verifies
  `target_path` actually exists in the catalog before declaring success, rather than trusting a
  silent CREATE/DROP.

**Discovery**
- `gettablesfrom(schema_path)` — every table/view under `schema_path`, recursing into
  subfolders; returns full dot-separated paths.
- `gettableitemsfrom(table_path)` — one table/view's schema (`{column: DATA_TYPE}`) and row
  count, without fetching any actual rows.

**Wiki & tags** (Dremio's catalog collaboration API — REST-only, no SQL/Flight equivalent):
- `getwikifrom(path)` / `setwikito(path, text, tags=None)` / `deletewiki(path)` — read,
  create/overwrite, or clear the wiki text on a catalog entity. `deletewiki` is idempotent — a
  missing `path`, or one with no wiki at all, is a no-op, not an error (Dremio's collaboration
  API has no separate delete-wiki endpoint, so this clears the text to empty). `tags`, if given
  to `setwikito`, is a list of `{"tag_name", "tag_value", "tag_title"}` dicts rendered into a
  `# Meta Data` section appended to `text` (both a human-readable `title : value` line per tag
  and the same data as `<meta><tag name=... value=... title=.../>...</meta>`) — Dremio's wiki is
  plain markdown with no structured-metadata concept of its own, so this is embedded directly in
  the text.
- `setmeta2wiki(path, tags=None, overwrite=True)` / `getmetafromwiki(path, tag_name=None,
  field=None)` — **folders only** (raises `CatalogOperationError` on a table/view — those have
  Dremio's own tags/labels for this instead). `setmeta2wiki` updates just the `# Meta Data`
  section of the wiki already at `path`, keeping whatever text comes before it untouched:
  `overwrite=True` (the default) replaces the whole section with one built fresh from `tags`;
  `overwrite=False` merges `tags` into whatever tags are already there (parsed back out of the
  existing `<meta>` block), appended after them, with no deduplication. If `path` has no wiki
  yet, starts from empty base text rather than raising. `getmetafromwiki` reads it back:
  without `tag_name` (or if it doesn't match one there), returns every tag as a list; with a
  matching `tag_name`, returns a single `{"tag_name", ...}` dict instead — both `tag_value` and
  `tag_title` if `field` isn't given, or just the one `field` (`"tag_value"`/`"tag_title"`) asks
  for.
- `gettagsfrom(path)` / `settagsto(path, tags)` / `deletetags(path, tags)` — **tables/views
  only** (the mirror image of `setmeta2wiki`/`getmetafromwiki` — raises
  `CatalogOperationError` on a folder, which has no Dremio tags/labels concept of its own). Read
  the full tag list; replace it wholesale; or remove just the given tags, leaving the rest
  untouched.

**Folders** (REST-only, idempotent — an already-there/already-gone folder is not an error):
- `createfolder(path, create_parents=False)` — `create_parents=False` (the default) raises if
  the parent is missing rather than creating a deep new path; `create_parents=True` creates
  every missing level.
- `deletefolder(path, cascade=False)` — `cascade=False` (the default) raises if the folder still
  has contents; `cascade=True` deletes every table/view and subfolder inside first, depth-first.

**Retrying**
- `retry_pending(idempotency_key)` — re-attempt whatever last stalled on an `EngineStartingError`,
  using the same arguments it was originally called with.

**Lifecycle**
- `catalog.close()` (or `with Catalog(...) as catalog:`) — disposes the REST/Flight session(s).
  Idempotent, and covered by a process-exit/SIGTERM fallback if you forget.

## Layout

| Path | Purpose |
|---|---|
| `dds_ingestion/credentials.py` | env-var creds + redacted `DremioCreds` |
| `dds_ingestion/models.py` | typed request/response models for the DDS ingest contract |
| `dds_ingestion/client.py` | thin, unit-testable HTTP client (`IngestClient`) |
| `dds_ingestion/progress.py` | tqdm progress bar with graceful fallback |
| `dds_ingestion/folder.py` | `FolderIngest` orchestration (scan/parallel/resume) |
| `catalog/client.py` | `Catalog` — two connections (REST + Flight), every operation as a method |
| `catalog/operations.py` | the operations themselves (table2view, datacopy, createfolder, ...), as functions taking an executor and/or a `CatalogRestClient` |
| `catalog/sql.py` | `SqlExecutor` protocol + REST/Flight implementations, `resolve_executor()` (transport env var) |
| `catalog/rest.py` | `CatalogRestClient` — Dremio's native `/api/v3/catalog` for folders, wiki, and tags (none of which have a SQL/Flight equivalent) |
| `catalog/retry_state.py` | persisted memory of stalled attempts, for `retry_pending()` |
| `catalog/errors.py` | `CatalogOperationError`, `EngineStartingError` |

## Development

```bash
pip install -e ".[dev]"
pytest
```

## Releasing a new version

1. Update `version` in `pyproject.toml` (e.g. `0.2.0`).
2. Commit and push to `main`.
3. The `Release` GitHub Actions workflow runs on every push to `main`: it reads
   the version from `pyproject.toml`, builds the package, and publishes a
   `v0.2.0` GitHub Release (creating the matching tag automatically), with
   notes auto-generated from merged PRs since the previous release
   (categorized via `.github/release.yml`).

   Pushing to `main` without bumping the version re-publishes the release for
   the current version with the latest build artifacts.

**Only one release/tag exists per branch at a time** — `main` always has exactly one `vX.Y.Z`
release, `staging` exactly one `vX.Y.Z-staging` prerelease. Each new release deletes its branch's
previous release *and* tag first, so **tags aren't permanent** — pin to whatever the
[Install](#install) badge shows *now*, not to an old tag number, since it won't exist once a
newer release replaces it.

**A separate floating tag literally named `main`/`staging`** always points at that branch's
latest release — the workflow force-moves it (`git tag -f`, force-push) once the real release
above succeeds. It deliberately shares its name with the branch: git resolves the ambiguity
deterministically (a tag always wins over a same-named branch), so `@main`/`@staging` in an
install command means "latest release," not "current branch tip" — expect (and ignore) a
"refname is ambiguous" warning from git/pip when that happens.

The workflow also rewrites this README's `pip`/`%pip install ...@vX.Y.Z[-staging]` example lines
to the version it just released, committing that change back to the branch (`[skip ci]`, so it
doesn't re-trigger itself) — so the examples above never go stale, without anyone having to
remember to update them by hand.

## Install in JupyterLab

Run this in a notebook cell — `@main`/`@staging` always resolves to whatever was most recently
released for that branch (see [Install](#install) above):

```python
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@main"       # latest stable
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@staging"    # latest early access
```

To pin to one specific release instead, use the exact tag the live badges under
[Install](#install) show (kept in sync automatically, see
[Releasing a new version](#releasing-a-new-version)):

```python
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.10"

# staging's latest release (early access)
%pip install "git+https://github.com/eeadata/EEALakeHouse.python.git@v0.1.11-staging"
```

Use the `%pip` magic rather than `!pip` — it installs into the kernel the
notebook is actually running on, instead of whatever `pip` happens to be
first on `PATH`. After installing, restart the kernel (**Kernel > Restart
Kernel...**) so the import below picks up the newly installed package:

```python
from eea_datalakehouse.dds_ingestion import FolderIngest
```

Alternatively, download the wheel attached to the [GitHub Release page](https://github.com/eeadata/EEALakeHouse.python/releases/latest)
for that tag and install the local file instead of pulling from git:

```python
%pip install /path/to/EEADataLakehouse-<version>-py3-none-any.whl
```
