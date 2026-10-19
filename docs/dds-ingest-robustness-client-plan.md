# DDS ingest robustness — the client half (DI-12) — plan

> Status: **plan only — nothing implemented yet.** Date: 2026-10-16.

Companion plan for **this** repository. The design, the server work and the
release schedule live with DDS:

```
EEALakeHouse/Components/DremioDocumentService/devplans/2026-10-16/
  Development Plan- Ingest Robustness and Transparency (DI-12).md
```

Read that first. This file covers only what changes in
`src/eea_datalakehouse/dds_ingestion/` (`FolderIngest`, `IngestClient`), which
server task each change needs, and in which DDS release (R0–R6) it ships.

---

## 1. Backward compatibility — the rule for every task here

Existing notebooks and scripts must keep working without a change:

1. **Existing signatures stay as they are.** `FolderIngest(...)`,
   `IngestClient.begin/commit/retry/cancel/stage/upload_file`: no parameter is
   removed, renamed, reordered or given a new meaning.
2. **New parameters are keyword-only and default to `None` / `False`.** With the
   defaults, the client sends exactly the request bodies it sends today. Each task
   adds a **golden test** that pins today's bodies.
3. **New server fields are used only when present.** The client works against
   the DDS release before the one a task needs, and keeps today's behaviour there.
4. **Exceptions keep their types.** `IngestApiError`, `StorageUnavailableError`
   and `S3UploadError` stay; new information goes into new attributes, not new
   exception classes callers would have to catch.

**Changes that are not fully backward compatible** (deliberate, and each task
repeats the ⚠ note):

| Task | What changes for an existing script | Mitigation |
|---|---|---|
| PY-1 | with `upload_mode=auto`, a network failure on the presigned upload now retries through DDS (`/stage`) instead of raising | only where the script fails today; `upload_mode=direct` on the server keeps the old behaviour |
| PY-2 | an upload that fails part-way now cancels the session (its landed files are deleted) before the exception is raised | nothing is lost: an incomplete upload cannot be retried anyway (`retry` re-runs only the load) |
| PY-6 | CSV/JSON ingests without `table_definition` now send locally inferred columns | the inferred schema is printed; `infer_schema=False` restores today's behaviour |

---

## 2. Tasks

### PY-1 Honour `upload_mode`, with automatic `/stage` fallback — R1

**Today:** the client ignores the `upload_mode` that `begin` returns and has no
automatic fallback (G10 in `Documentation/data-digestion.md`).

- Implement the contract's upload-mode rule, the same text the extension's
  `uploadOne` implements: `proxy` → always `IngestClient.stage`; `direct` →
  presigned only; `auto` → presigned, then `stage` on a **network** error
  (connection, TLS, DNS). An S3 HTTP error is not retried through the proxy.
- Multipart files in proxy mode go through `stage` as a whole file; this needs
  the DDS fix DI-12.10a (R1) and the `/stage` size cap applies.
- **⚠ Compatibility:** see §1 table.
- **Server:** no new field (`upload_mode` exists); DI-12.10a for large files.
- **Verify:** tests with an unreachable presigned URL under each mode; a file over
  the multipart threshold in proxy mode.

### PY-2 Cancel when an upload fails; small fixes — R1

- `FolderIngest.run()` (`folder.py:218-234`) has no failure handling. On an
  upload-stage failure, call `IngestClient.cancel(session_id)` before raising
  (best-effort: if the cancel fails, say so in the exception and leave the rest to
  the DDS sweep).
- Fix the stale comment in `IngestClient.upload_file` that calls the PUT branch
  "legacy pre-signed PUT": since contract v0.7 it is the normal path.
- `.ndjson` stays accepted (`folder.py:45`); the server starts accepting it in R1
  (DI-12.9). Against an older server it is still refused at begin, as today.
- **⚠ Compatibility:** see §1 table.
- **Server:** DI-12.26 (R1) for the server-side sweep; none needed for the cancel.
- **Verify:** an upload that fails on file 3 of 5 leaves no session and no objects
  (against a mocked DDS that records the `DELETE`).

### PY-3 Optional table definition — R2

- A `TableDefinition` / `ColumnDef` dataclass in `dds_ingestion/models.py`,
  matching the contract object of DI-12.1b (`version`, `columns` with optional
  `nullable` / `description`, `partition_by`, `sort_by`). A plain dict is accepted
  wherever a `TableDefinition` is.
- `FolderIngest(..., table_definition: TableDefinition | dict | None = None)`.
- `IngestClient.begin(..., definition=None)` (new keyword) and the existing
  `IngestClient.commit(definition=...)` accept either type. `IngestClient.retry`
  gets `definition=None` for DI-12.6. One name, `definition`, at the
  `IngestClient` level; `table_definition` at the `FolderIngest` level, as the
  more descriptive user-facing name.
- Sending (DI-12.1b compatibility rule 2): send the definition on **begin**; if
  the begin response has no `plan.definition_applied`, the server is older and
  ignored it, so send it again on **commit**. Serialise only the fields the
  caller set.
- Print `plan.definition_ignored` as a warning when present.
- **Server:** DI-12.1a, DI-12.1b, DI-12.2 (R2); DI-12.6 (R3) for `retry`.
- **Verify:** golden test — no definition → bodies identical to today's; with a
  definition → only set fields sent; against a server without `definition_applied`
  → the definition goes on commit.

### PY-4 Show the plan, and `dry_run` — R2

- When `begin` returns a `plan` (DI-12.2), print it before uploading: placement,
  table path, storage path, conflict mode, and the definition report.
- `FolderIngest(..., dry_run=True)` and `IngestClient.begin(..., dry_run=True)`:
  send `dry_run: true`, print the plan, upload nothing. Against a server without
  `dry_run` support (no `plan` in the response) raise a clear error instead of
  silently opening a real session; cancel that session first.
- **Server:** DI-12.2, DI-12.3 (R2).
- **Verify:** plan printed when present, nothing printed when absent; `dry_run`
  against old and new server.

### PY-5 Failure text, `retryable`, `cleanup_on_failure` — R3

- Show the server's failure fields verbatim: `error`, `failed_stage`,
  `retryable`, and the error body's `message`, in the same format as the
  extension's Transfers panel (DI-12.14). Add them as attributes on
  `IngestApiError` (`error_slug`, `failed_stage`, `retryable`); the exception
  types stay the same.
- `FolderIngest(..., cleanup_on_failure=False)`: on a commit failure with
  `retryable: true`, keep the session for `retry` (today's behaviour) unless
  `cleanup_on_failure=True`, then cancel. With `retryable: false` the server has
  already cleaned up.
- **Server:** DI-12.5 (R3).
- **Verify:** attributes populated from a 4xx and from a failed poll; the
  `cleanup_on_failure` matrix.

### PY-6 CSV/JSON options, inferred columns, local Parquet check — R3/R4

- One format table in `dds_ingestion/` (extensions, options builder, local
  schema reader), replacing `folder.py:45` plus the scattered checks; the client
  mirror of the server's DI-12.0b handlers.
- `FolderIngest(..., csv_options=None, json_options=None)`, sent at begin (the
  server already accepts both on `BeginRequest`).
- **CSV/JSON without `table_definition`:** infer columns locally with pyarrow over
  the **whole** file set, print them, and send them as the table definition, so a
  scripted CSV ingest into an editable table no longer dies with the schema error
  after the upload (G1). `infer_schema=False` turns this off.
- **Parquet with `table_definition` columns:** read the footers locally and run
  the same comparison as the server's DI-12.1d before uploading anything, so a
  mismatch fails in seconds instead of after the upload. The server check stays
  the authority.
- **⚠ Compatibility:** see §1 table (inferred columns are sent).
- **Server:** works against any DDS (columns go on commit when begin does not take
  them, PY-3); DI-12.1d (R3) for the matching server check.
- **Verify:** inference on mixed-type CSV and JSON lines; the Parquet mismatch
  caught locally; one end-to-end editable CSV run against the real-Dremio
  `local-stack` profile (DI-12.15) that used to hit the 501.

### PY-7 `FolderIngest.diagnose()` — R4

- Checks, in order, and prints each result in plain language: DDS reachable,
  credential valid, WRITE on the target, the storage self-test verdict
  (`POST /storage-selftest`), and a begin `dry_run` for the folder (PY-4).
- Against a server without `dry_run`, skip that step and say why.
- **Server:** DI-12.3 (R2).
- **Verify:** one test per failing step.

---

## 3. Order

| DDS release | Tasks here | Needs from DDS |
|---|---|---|
| R1 | PY-1, PY-2 | DI-12.10a (large files via proxy), DI-12.9 (`.ndjson`), DI-12.26 |
| R2 | PY-3, PY-4 | DI-12.1a, DI-12.1b, DI-12.2, DI-12.3 |
| R3 | PY-5, PY-6 | DI-12.5, DI-12.6, DI-12.1d |
| R4 | PY-7 | DI-12.3 |

Each task can be released as soon as it is ready: by rule 3 it also works against
the DDS release before the one it needs, with today's behaviour.

## 4. Definition of done

- Every existing call sends the same request bodies as before (golden tests),
  except the changes in the §1 table.
- `FolderIngest` honours `upload_mode`, cancels on upload failure, takes an
  optional `table_definition`, `csv_options`, `json_options`, `dry_run` and
  `cleanup_on_failure`, prints the begin plan, and offers `diagnose()`.
- CSV/JSON scripted ingests into editable tables work without hand-written
  columns; Parquet columns are checked locally before upload.
- `docs/python-client-guide.md` describes each new option, with one example with
  and one without a table definition. mypy clean; the test suite passes.
