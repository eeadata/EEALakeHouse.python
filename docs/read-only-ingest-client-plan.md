# Read-only ingest — the client half (DI-11.9) — **DONE**

> Landed: `sub_path` (DI-11.12), the retry guard and `placement` (DI-11.7),
> `CommitResult.storage_path` and the docs below (DI-11.9). mypy clean, 52 tests
> pass. What is still open is recorded at the end of this file.

Companion note for **this** repository. The full design lives with the server
work it depends on:

```
EEALakeHouse/Components/DremioDocumentService/devplans/2026-08-21/
  Development Plan- Read-Only Ingest to Permanent S3 (DI-11).md
```

Read that first — this file only records what is built **here**, and when.

## The short version

`FolderIngest` already has the attribute: `intent="read_only" | "editable"`
(`src/eea_datalakehouse/dds_ingestion/folder.py:103`, `models.py:13`), sent to
`POST /ingest/begin` (`client.py:121`). On EEA production's managed catalog the
server currently ignores it for placement — every ingest stages to a temp prefix,
is copied into the catalog's Iceberg storage, and the upload is deleted.

DI-11 makes `read_only` mean *store the files permanently* under
`local_s3/dh-prod-data/read/<catalog mirror>/<table>/` and register them.
**Where the bytes land is chosen by the server**, in `begin`'s presigned targets,
so this package needs no new parameter and no new call.

## What changed here

0. ✅ **`sub_path` — the one new parameter** (DI-11.12). `FolderIngest(...,
   sub_path="2026")`, passed straight through to `begin`; the server validates,
   normalises and scopes on it. It is what lets a read-only table accumulate:

   ```python
   FolderIngest(folder="./bw_2026", target_catalog_path="…/bwd/reference",
                data_format="parquet", intent="read_only",
                table_name="water_temperature", sub_path="2026").run()
   # → read/…/water_temperature/2026/*.parquet, alongside 2024/ and 2025/
   ```

   A custodian whose local folder *already* has the structure needs nothing —
   `scan_folder` (`folder.py:80-92`) preserves sub-folders and the server keeps
   the original `rel_path`. `sub_path` is for filing a **flat** folder under a
   name they choose. Worth a worked example in `README.md`, and note that
   `replace` with a `sub_path` clears only that year.
1. ✅ **Docs and docstrings** — `folder.py` class docstring + the `intent`
   parameter, `dds_ingestion/README.md:38`, `__init__.py:16`: say what each
   intent now does (permanent raw folder + catalog view vs. Iceberg copy).
2. ✅ **`CommitResult.storage_path`** (`models.py`) — optional, additive; the
   physical Dremio path the server reports for a permanent ingest, so a notebook
   can print where the files actually are. Parse permissively as everywhere else
   (absent ⇒ `None`), so it works against an older server.
3. ✅ **Resume/retry note** (`folder.py:305-322`, `retry()`): `_already_done`'s
   docstring says a re-run is "an S3 overwrite of the identical key, which is
   harmless". Against a permanent folder in `append` mode that is only true if
   the server reuses the keys instead of numbering them. Align the wording with
   whatever DI-11.7 settles, and steer users to `attach()` + `retry()` rather
   than re-running `run()`.
4. ✅ **Tests** — `tests/dds_ingestion/`: `storage_path` parsing, and a
   respx-mocked `begin` whose `key_prefix` is a permanent `read/...` prefix,
   asserting the client uploads to the issued targets unchanged.

## What does not change

- No new constructor argument, no new method, no new endpoint.
- `editable` behaviour, the upload/parallelism/progress machinery, credentials.
- Nothing has to change for the client to keep working while the server runs
  with `DDS_INGEST_READ_MODE=staged` (its default).

## When

DI-11 **step 4** ("Prove") in the rollout table — after the server feature exists
behind its flag, and it is the first client pointed at a `permanent` server. The
JupyterLab extension is deliberately left alone until step 5.

## Noted for later: deleting a read-only table

Because read-only files are permanent, dropping the view or dataset from the
catalog leaves the objects in S3. The agreed home for that cleanup is a
**delete-table operation in this package** — `eea_datalakehouse.catalog` has
`deleteview` and `deletefolder` today, but nothing that removes a table together
with its backing data. **Not implemented as part of DI-11**, recorded here (and
in §8.5 of the DI-11 plan) so it is a known gap rather than a surprise.

When it is picked up: this package holds no S3 credentials by design, so the
delete needs a DDS-side endpoint for the read folder to call — not boto3 in a
notebook.

## Related: `common/catalog.py` moves server-side

`dds_ingestion/common/catalog.py` (on `development`) walks a catalog path and
creates the missing dataflow levels, refusing to invent a domain or subdomain.
That walk is exactly what a read-only ingest needs before its view can be
created, so **DI-11.2 ports it into DDS**, where the extension and any REST
client get it too and the taxonomy rule is enforced in one place. Once that
lands, the client-side copy is either a thin convenience wrapper over
`POST /api/v1/catalog/{path}/folder` or redundant — decide when DI-11.2 is done,
not before.

This is a *catalog* concern only: the `read/` prefixes in S3 need no creating at
all — see §3.6 of the DI-11 plan.

## One thing to settle first

There are two copies of this client: this repo
(`src/eea_datalakehouse/dds_ingestion/`) and a snapshot in the DDS repo
(`clients/dds_ingest/dds_ingest/`). They have already drifted — `folder.py`,
`client.py` and `models.py` differ; `credentials.py` and `progress.py` are
identical. **This repo is where the client is developed.** Whether the DDS-side
copy is re-synced, thinned to a test fixture, or dropped is a separate decision,
not part of DI-11 — but decide it before editing both by hand again.
