# DDS ingest robustness — the client half (DI-12) — plan

> Status: **PY-1 … PY-7 implemented** (2026-10-16). Reviewed against the code on
> 2026-10-19 (§5); the follow-up tasks **PY-8, PY-9, PY-10, PY-12 and PY-13
> implemented** the same day. Open: the §5.1 repair reaching `main`, and PY-11,
> which is Oskar's decision. Date: 2026-10-16.

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
| PY-1 | when the server says `upload_mode: "auto"` (DDS's default) and `FolderIngest` keeps its default `upload_mode="auto"`, a network failure on the presigned upload now retries through DDS (`/stage`) instead of raising | only where the script fails today; `FolderIngest(upload_mode="direct")`, or `direct` on the server, keeps the old behaviour |
| PY-2 | an upload that fails part-way now cancels the session (its landed files are deleted) before the exception is raised | nothing is lost: an incomplete upload cannot be retried anyway (`retry` re-runs only the load) |
| PY-6 | CSV/JSON ingests without `table_definition` now send locally inferred columns | the inferred schema is printed; `infer_schema=False` restores today's behaviour |

---

## 2. Tasks

### PY-1 Honour `upload_mode`, with automatic `/stage` fallback — R1 — **DONE**

**Before this task** (corrected 2026-10-19): `FolderIngest` already honoured the
server's `upload_mode: "proxy"` hint and took its own
`upload_mode="auto" | "direct" | "proxy"` — Oskar Esparza, `774691a`, 2026-10-08.
This plan was written on a branch that did not have that commit yet. What was
missing was the automatic fallback (G10 in `Documentation/data-digestion.md`).

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

> Landed `9d924f7`. The merge with `774691a` on `main` (`101b09c`) dropped part of
> Oskar's code; repaired on `eea_bliki` on 2026-10-19 (§5.1). As built, Oskar's
> design is kept: `BeginResult.upload_mode` is `None` when the server sends none and
> is read from the top level or from inside `s3`; `FolderIngest(upload_mode=…)`
> decides, and its default `"auto"` follows the server. The fallback is added on
> top: only when both `FolderIngest` and the server say `"auto"`
> (`_falls_back_to_proxy`). DDS sends `upload_mode` on every begin, `"auto"` unless
> configured, so the fallback is on by default. The `/stage` size cap is checked
> before uploading since PY-10.

### PY-2 Cancel when an upload fails; small fixes — R1 — **DONE**

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

> Landed `9d924f7`. As built: any upload failure cancels the session, and the
> uploads still queued are cancelled at the first failure; the PUT comment is fixed;
> `.ndjson` is in the format table (`formats.py`). Under
> `IngestSession(retry=True)` the cancel hid the upload error until PY-8; the
> client guide describes it since PY-12.

### PY-3 Optional table definition — R2 — **DONE**

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

> Landed `6a4faaf`. `IngestClient.retry(definition=…)` for DI-12.6 followed in
> PY-9.

### PY-4 Show the plan, and `dry_run` — R2 — **DONE**

- When `begin` returns a `plan` (DI-12.2), print it before uploading: placement,
  table path, storage path, conflict mode, and the definition report.
- `FolderIngest(..., dry_run=True)` and `IngestClient.begin(..., dry_run=True)`:
  send `dry_run: true`, print the plan, upload nothing. Against a server without
  `dry_run` support (no `plan` in the response) raise a clear error instead of
  silently opening a real session; cancel that session first.
- **Server:** DI-12.2, DI-12.3 (R2).
- **Verify:** plan printed when present, nothing printed when absent; `dry_run`
  against old and new server.

> Landed `6a4faaf`. As built: the dry run is a separate `FolderIngest.dry_run()` /
> `IngestClient.dry_run()` returning a `DryRunResult`, not a `dry_run=` flag (a dry
> run has no session id, and `BeginResult.session_id` stays a `str`). Against a
> server without dry runs it raises `UnsupportedByServerError`, a subclass of
> `IngestApiError`, so callers that catch `IngestApiError` need no change (rule 4).

### PY-5 Failure text, `retryable`, `cleanup_on_failure` — R3 — **DONE**

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

> Landed `a7f99e9`. As built: `IngestApiError.error_slug` and `.retryable`; a failed
> commit carries the session's own failure text (`StatusResult.describe_failure`) as
> an exception note; `cleanup_on_failure` as planned. `failed_stage` became an
> attribute in PY-13.

### PY-6 CSV/JSON options, inferred columns, local Parquet check — R3/R4 — **DONE**

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

> Landed `a0faf15`. As built: `formats.py` (`FORMATS`, `local_columns`,
> `compare_columns`); `csv_options` / `json_options` are sent only for their own
> format; columns are inferred only for CSV/JSON into an **editable** table, the one
> case where the server builds the table from them; a JSON array cannot be inferred
> and says so. Open: the end-to-end editable CSV run against `local-stack`
> (DI-12.15) has not been done from this client.

### PY-7 `FolderIngest.diagnose()` — R4 — **DONE**

- Checks, in order, and prints each result in plain language: DDS reachable,
  credential valid, WRITE on the target, the storage self-test verdict
  (`POST /storage-selftest`), and a begin `dry_run` for the folder (PY-4).
- Against a server without `dry_run`, skip that step and say why.
- **Server:** DI-12.3 (R2).
- **Verify:** one test per failing step.

> Landed `c1149d5`. As built: four steps — DDS reachable; credential accepted
> (`GET /ingest/limits`, or `GET /ingest` on a server without it); storage self-test;
> dry run of the folder. WRITE on the target is covered by the dry run rather than
> checked on its own, so against a server without dry runs (before R2) it is skipped.

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

The follow-up tasks from the 2026-10-19 review are ordered in §5.5.

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

> 2026-10-19: met — the client guide since PY-12 (`f512d2f`). `dry_run` is a
> method rather than an option (PY-4). mypy is clean for the code these tasks
> touched; the remaining errors are in `dds_ingestion/common/dremio_identity.py`.

---

## 5. Review against the code — 2026-10-19

Every task was checked against `eea_bliki` after it was brought up to date with
`main` — which now carries Oskar Esparza's `%catalog` / `%ingest` work and
`774691a` — and against the DDS side: the DI-12 plan's §7.2 progress notes and the
server code. Oskar's work takes priority where the two meet.

### 5.1 First: `main` cannot ingest

`101b09c`, which merged `eea_bliki` into `main` on 2026-10-19 in GitHub's conflict
editor, dropped twelve lines of `774691a`:

- `BeginResult.from_json` raises `NameError: s3` on every begin, so
  `FolderIngest.run()` fails before uploading anything. The release workflow moved
  the `main` and `v0.1.30` tags to that merge, so `pip install …@main` and
  `…@v0.1.30` install it. `@staging` (`v0.1.31-staging`) does not contain
  PY-1 … PY-6 and is not affected.
- `FolderIngest(upload_mode="proxy")` reported a proxy upload while uploading to S3.
- 12 tests fail on `main`.

Repaired on `eea_bliki` on 2026-10-19: Oskar's lines restored as he wrote them, PY-1's
fallback added on top, and only PY-1's own test changed (`upload_mode` is `None` when
the server sends none, as in `774691a`). The whole suite passes. This has to reach
`main` before any task below.

### 5.2 Where each task stands

| Task | State | Found in the review | Follow-up |
|---|---|---|---|
| PY-1 | done; built on `774691a` | `/stage` size cap not checked; not in the client guide | PY-10, PY-12 — done |
| PY-2 | done | hides the upload error under `IngestSession(retry=True)`; not in the guide | PY-8, PY-12 — done |
| PY-3 | done | `retry` cannot send a corrected definition | PY-9 — done |
| PY-4 | done, as a method | — | — |
| PY-5 | done | `failed_stage` only in the note text | PY-13 — done |
| PY-6 | done | end-to-end editable CSV run against `local-stack` not done | §5.7 — open |
| PY-7 | done | — | — |
| §4 | met | `docs/python-client-guide.md` described none of the new options | PY-12 — done |

Beyond the tasks: `IngestSession.ingest`, behind `%ingest`, passes `FolderIngest` a
fixed set of options, so none of the options above reaches a notebook user (PY-11,
open).

### 5.3 Follow-up tasks

#### PY-8 `retry()` after a cancelled upload — **DONE**

- **Today:** PY-2 cancels the session after a failed upload, and
  `FolderIngest.retry()` then raises `IngestStateError("no session to retry — call
  run() first")`. `IngestSession.commit(retry=True)` re-raises that error, so
  `%ingest` shows it instead of the upload error: an S3 403 reads "no session to
  retry" (checked 2026-10-19).
- Remember the upload error that led to the cancel. `retry()` then:
  - re-runs `run()` under a new session when that error was transient (a network
    error, or a 5xx from S3 or DDS) **and** the cancel succeeded, dropping the
    `idempotency_key` as its "anything else" branch already does;
  - otherwise raises `IngestStateError` whose message quotes the upload error. A
    failed cancel may have left files in place, and re-running a read-only ingest
    stored permanently would add a second copy.
- No change to `IngestSession`.
- **⚠ Compatibility:** `retry()` after a failed upload used to raise; after a
  transient failure it now uploads again. The cancel deleted whatever had landed,
  so nothing is duplicated.
- **Server:** none.
- **Verify:** `IngestSession(retry=True)` after an S3 403 names the 403; after a
  network error in `direct` mode the next attempt uploads and commits; after a
  failed cancel nothing is re-uploaded.

> Landed `7dad450`. As built: `run()` records the upload failure, and `retry()`
> looks at it before anything else — also when the cancel failed and a session id
> is left, because that session never got all its files and asking the server
> about it would only say "uploading". "Can pass" (`_may_succeed_on_retry`) is a
> network error, or an `IngestApiError` whose `retryable` is true or, without
> one, whose status is 5xx.

#### PY-9 `retry` with a corrected table definition (rest of PY-3) — **DONE**

- `IngestClient.retry(session_id, *, definition=None)` and
  `FolderIngest.retry(*, table_definition=None)`: with a definition, the resumed
  load uses it (DI-12.6), and a re-upload under a new session sends it on begin.
  Without one, the request is exactly today's (golden test).
- For a commit that failed with `schema_required`, retryable since DI-12.5: a retry
  with the columns fixes it without uploading again.
- **Server:** DI-12.6 (done, R3).
- **Verify:** `schema_required` → `retry(table_definition=…)` → done; the request
  without a definition is unchanged; against a server before DI-12.6 the retry
  behaves as today.

> Landed `9b4f429`. As built: without a definition, `FolderIngest.retry()` still
> calls `client.retry(session_id)` with no keyword, so a client written against
> the old signature — two test fakes in `IngestSession`'s tests are — keeps
> working. The behaviour against a server before DI-12.6 is from that server's
> code (its endpoint takes no body), not a test.

#### PY-10 Check file sizes against the server's limits (rest of PY-1) — **DONE**

- DDS has two caps. `begin` refuses a file over the caller's cap (DI-12.13), but
  `POST /ingest/stage` has its own `stage_max_bytes` and refuses a file only when
  that file arrives — after other files have uploaded, which PY-2 then cancels. The
  two caps are equal unless the server sets a separate stage cap.
- Read `GET /ingest/limits?runner=notebook` once per run. In proxy mode, refuse
  before uploading anything when a file exceeds `stage_max_bytes`. With the
  fallback, do not fall back for such a file: raise the network error with a note
  saying why. A 404 (a server before DI-12.11) keeps today's behaviour.
- `diagnose()` already calls `limits()`: show the caps there and flag the files
  over them.
- **Server:** DI-12.11, DI-12.13 (done, R4).
- **Verify:** proxy mode with one file over the stage cap uploads nothing; the
  fallback is not attempted for such a file; an older server is unchanged.

> Landed `2a554b2`. As built: proxy mode asks for the limits before uploading; the
> fallback asks only when a file actually falls back, so a plain direct upload
> makes no extra request. Best-effort: a 404, or any failure of the limits
> request, leaves DDS's own check as the only one. `diagnose()` gained a "File
> sizes" step after the credential step: a file over `max_bytes` fails it, one
> over `stage_max_bytes` fails it only with `upload_mode="proxy"`.

#### PY-11 The new options in `%ingest` — Oskar's decision

- `IngestSession.ingest` (`dds_ingestion/session.py`) passes `FolderIngest` seven
  options: `intent`, `conflict_mode`, `table_name`, `sub_path`, `parallelism`,
  `multipart`, `show_progress`. Not `table_definition`, `csv_options`,
  `json_options`, `infer_schema`, `cleanup_on_failure` — nor `upload_mode`.
- Proposal: add those six to `IngestSession.ingest` and to `%ingest help`, and
  decide whether `dry_run()` and `diagnose()` belong in the notebook layer.
- Why it matters: without `table_definition`, a CSV/JSON ingest into an editable
  table whose columns cannot be inferred has no way through `%ingest`. It fails at
  commit today, and at begin once `DDS_INGEST_SCHEMA_AT_BEGIN` is on.
- **Owner:** Oskar Esparza — it is his module; nothing changes there without him.
- **Server:** none.

#### PY-12 Client guide — the open part of §4 — **DONE**

- `docs/python-client-guide.md` describes: `upload_mode` (`FolderIngest`'s choice,
  the server's hint, the fallback); cancel on upload failure; `table_definition`,
  with one example with and one without; `csv_options` / `json_options` /
  `infer_schema`; `cleanup_on_failure`; `dry_run()`; `diagnose()`; reading
  `error_slug`, `retryable` and the failure note.
- The DI-12 plan (§7.2) lists the `upload_mode` and cancel parts as open since R1.

> Landed `f512d2f`, covering PY-8, PY-9, PY-10 and PY-13 as well. As built: Part 1's
> reference tables also caught up with PY-3 … PY-7 (models, `IngestClient`
> endpoints, errors), and the "S3 is unreachable" recipe uses
> `upload_mode="proxy"` instead of driving `/stage` by hand.

#### PY-13 Small fixes — **DONE**

- `failed_stage` as an attribute on the exception from a failed commit (PY-5 named
  it; today it is only in the note).
- `IngestOutcome.files_staged`: how many files went through DDS, so a fallback is
  visible. `IngestOutcome.upload_mode` stays as it is.

> Landed `a5900d8`.

### 5.4 Not planned yet

- **Dremio job ids in the failure text** (DI-12.18, R5): only together with the
  extension, which does not show them either — DI-12.14 keeps one failure text for
  both clients.
- **A local schema-drift warning** before a read-only append, like PY-6's Parquet
  check: after DI-12.22 (R6b).
- **Undo for `IngestSession`** once DDS can delete a read-only table (DI-12.21, R6b);
  its module docstring notes that it is missing.

### 5.5 Order

| Step | What | Why first | State |
|---|---|---|---|
| 1 | §5.1 to `main`, with Oskar | every Python ingest from `@main` fails today | on `eea_bliki` (`60cad54`), not yet on `main` |
| 2 | PY-8, PY-12, PY-13 | fix and document what is already shipped | done 2026-10-19 |
| 3 | PY-9, PY-10 | use server features that are already deployed | done 2026-10-19 |
| 4 | PY-11 | when Oskar agrees | open |

Steps 2 and 3 were built in the order PY-8, PY-13, PY-9, PY-10, PY-12, so the guide
documents all of them at once.

### 5.6 Coordination

- Oskar Esparza reviews the §5.1 repair, which restores his `774691a`, and decides
  PY-11.
- The DI-12 plan (§7.2) cites PY-7 as `8d79542`; that commit was rebased before it
  was pushed and is `c1149d5`.

### 5.7 Release notes

- Before `DDS_INGEST_SCHEMA_AT_BEGIN` is turned on (DI-12 plan §7.3), the kernels
  also need a Python release with PY-3 and PY-6, from after the §5.1 repair: a script
  that sends its columns only on commit (`IngestClient.commit(definition=…)`) is
  refused at begin.
- After each deploy: one ingest per format through this client into the production
  test area (DI-12 plan §7.1 rule 4), and PY-6's editable-CSV run against
  `local-stack`.
