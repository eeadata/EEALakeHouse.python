# Dev notes — findings & ideas

Scratchpad for observations and ideas about `eea_datalakehouse`, collected while
the development team is away. **Nothing here has been applied to the code** —
every entry is a proposal to discuss when the team is back.

Started: 2026-08-18 · Branch at time of writing: `chore/tidy-dds-ingestion-copyover`

## How to use this file

- One entry per finding, newest at the bottom of its section.
- Keep entries small and self-contained: what was observed, why it matters, what
  we'd suggest. No entry implies a decision has been made.
- Status: `open` (needs discussion) · `agreed` (team said yes, not yet done) ·
  `rejected` (discussed, deliberately not doing) · `done` (implemented elsewhere).

### Entry template

```markdown
### <short title>

- **Status:** open
- **Where:** `path/to/file.py:42`
- **Observation:** what is actually there today.
- **Why it matters:** the concrete consequence.
- **Suggestion:** the smallest change that would address it.
- **Open question:** anything we need the team to decide.
```

## Findings

### Dangling link to `docs/python-client-guide.md`

- **Status:** open
- **Where:** `src/eea_datalakehouse/dds_ingestion/README.md` (last line of
  "Managing the transfer")
- **Observation:** the README points at
  [`docs/python-client-guide.md`](../../docs/python-client-guide.md) as the
  "Full class reference". That file does not exist in the repository, and
  `git log` shows it was never committed and later deleted — it has simply never
  been there.
- **Why it matters:** the link is the only pointer to a full API reference for
  `FolderIngest`, so a notebook author following the README hits a 404 on
  GitHub. It also reads as if documentation exists that nobody can find.
- **Suggestion:** either write the guide, or drop the link and inline the short
  method list that is already in that README.
- **Open question:** was the guide drafted somewhere outside this repo (wiki,
  Confluence, the DDS repo) and just needs copying over?

### The API is shaped for a developer, not for a custodian writing a one-off script

- **Status:** open
- **Where:** `src/eea_datalakehouse/catalog/` (on `development`/`main` — **not** on
  `chore/tidy-dds-ingestion-copyover`, so all line numbers below are as of
  `development`), plus `pyproject.toml`
- **Premise:** the people writing this code are data custodians, working in a
  command-line / batch-scripting style — short scripts, run once, per dataflow.
  The library currently asks them to work like application developers instead.

The concrete things that force the developer style, numbered below. Each is
small on its own; together they are why a five-line job takes a page of Python.
Sub-sections `1a`, `2a` and `7` are proposals rather than observations.

#### 1. Credentials are threaded through every call site — and don't need to be

`Catalog.__init__` (`catalog/client.py:125-136`) takes `base_url` and `token` as
required positional arguments, plus an optional `username`. Nothing resolves
them: every caller supplies all three, on every construction.

The library's own debug harness shows the cost — `debugger/debug_run.py` repeats

```python
catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
```

at **20+ separate call sites** (lines 143, 159, 186, 210, 220, 235, 245, 252,
259, 280, 290, 311, 330, 341, 353, 368, 387, 401, 414, …). If the authors' own
script looks like that, a custodian's will too.

**The machinery to avoid this already exists in this same library, twice:**

- `dds_ingestion/folder.py:136-143` — `FolderIngest` resolves its own
  credentials from the environment when the caller doesn't inject a client, via
  `load_creds()` / `load_base_url()` (`dds_ingestion/credentials.py:48-76`).
  So one half of the library auto-resolves and the other half refuses to.
- `dds_ingestion/common/dremio_identity.py` — a full identity resolver
  (`resolve()`, `endpoint()`, `dds_credentials()`) that already knows the
  precedence a custodian needs: the JupyterLab "Dremio Catalog" settings panel
  first, then whatever `%init` bound into the kernel, then the process
  environment including the local `_DREMIO_USER` / `_DREMIO_PWD` /
  `DREMIO_BASE_URL` aliases (`dremio_identity.py:177-205`, `242-260`,
  `115-119`). It resolves exactly the four values `Catalog` demands:
  `DREMIO_USERNAME`, `DREMIO_TOKEN`, `DREMIO_URL`, `DDS_BASE_URL`.

So: **no, credentials do not need to be passed.** `Catalog` simply never calls
the resolver that the package next door already ships.

- **Suggestion:** make every argument to `Catalog()` optional and fall back to
  `dremio_identity.resolve()`, so `Catalog()` with no arguments works in a Hub
  kernel, in the local stack, and in a cron job — with explicit arguments still
  winning when someone needs to override. Same treatment for the `DREMIO_URL`
  vs `DDS_BASE_URL` split, which a custodian should never have to think about.
- **Open question:** is `dremio_identity` deliberately kept inside
  `dds_ingestion.common` (i.e. ingest-only), or should it move up to
  `eea_datalakehouse.common` and become the one identity path for ingest,
  catalog and anything later?

#### 1a. Answering it directly: yes, `%init` is enough — with two gaps

A real `%init` in this environment exposes:

```
DREMIO_USERNAME  adm_bliki
DREMIO_TOKEN     set (hidden, 64 chars)
DREMIO_URL       https://dremio.eea.europa.eu:9047
DREMIO_PASSWORD  (not defined)
DDS_BASE_URL     http://localhost:8000/
SCHEDULER_URL    http://gpu02.pdmz.eea:8080/scheduler
```

Lined up against what `Catalog(base_url, token, username=...)` demands:
`DREMIO_URL` → `base_url`, `DREMIO_TOKEN` → `token`, `DREMIO_USERNAME` →
`username`. **All three are already there.** Nothing needs to be typed, and
`dremio_identity.resolve()` already returns exactly this dict
(`BOUND_VARS = SECRET_VARS + SERVICE_VARS`, `dremio_identity.py:95-109`).

`DREMIO_PASSWORD (not defined)` is not a blocker: the Flight handshake uses
`authenticate_basic_token(username, token)` — the PAT as the password, not
`DREMIO_PASSWORD` (`catalog/sql.py:255-262`, `319-321`). And the trailing slash
on `DDS_BASE_URL` is harmless — `RestSqlExecutor`, `CatalogRestClient` and
`IngestClient` all `rstrip("/")` their base URL.

Two things do need deciding before an argument-less `Catalog()` would work
everywhere:

**Gap 1 — `resolve()` does not read the kernel globals `%init` binds.**
`endpoint()` looks in three places: the settings panel, then `ip.user_ns` (what
`%init` bound), then the environment (`dremio_identity.py:187-201`).
`resolve()` looks in only two: the panel and the environment
(`dremio_identity.py:242-260`) — no `ip.user_ns`. It works today because the Hub
injects those variables into the *environment* and `%init` merely re-exports
them as globals, so the environment lookup happens to find the same values. But
anything that exists only as a kernel global — a value the extension binds
without exporting, or one a user reassigns in a cell — is invisible to
`resolve()` while being visible to `endpoint()`. Two resolvers, two different
precedence chains.

**Gap 2 — `%init` exposes no Flight location, and `datacopy` is Flight-only.**
`datacopy` / `datamove` always run over Arrow Flight (`catalog/client.py:49-51`,
`205-245`), which needs a `grpc://host:port`, not the REST URL. None of the six
`%init` variables carries one. With nothing given, `_default_flight_location`
derives it from `DREMIO_URL`: `https://dremio.eea.europa.eu:9047` →
`grpc+tls://dremio.eea.europa.eu:32010` (`catalog/sql.py:69-87`) — and that
code's own comment says port 32010 is Dremio's documented default,
"unverified against this project's actual deployment". So making credentials
automatic would make `datacopy` silently depend on a guess; if the guess is
wrong it fails as `Socket closed`, which reads like a network fault rather than
a misconfiguration.

- **Suggestion:** one resolver, used by everything. Fold `endpoint()`'s
  three-source precedence into `resolve()` so both agree, then let `Catalog()`
  and `FolderIngest()` default every connection argument from it — explicit
  arguments still win. Add `DREMIO_FLIGHT_LOCATION`
  (`catalog/sql.py:43`) to whatever `%init` exports, or verify 32010 for this
  deployment and drop the "unverified" caveat.
- **Open question:** is `%init` ours to extend? If the variable list is owned by
  the `jupyter_dremio` Hub extension, adding the Flight location is a
  cross-repo change, and until then the library needs a documented default.

#### 2. `idempotency_key` is mandatory everywhere and reaches nothing

Every one of the 19 `Catalog` methods declares `*, idempotency_key: str` with
**no default** (`catalog/client.py:174-332`) — including the pure reads:
`gettablesfrom`, `gettableitemsfrom`, `getwikifrom`, `gettagsfrom`,
`getmetafromwiki`.

Two observations about what that key actually does:

- **It is never sent to Dremio.** The REST executor posts `{"sql": sql}` and
  nothing else (`catalog/sql.py:165`); no header carries it. Its only job is to
  be a lookup key in a local JSON file at
  `~/.cache/eea_datalakehouse/catalog_retry_state.json`
  (`catalog/retry_state.py:23-26`), written only when a call raises
  `EngineStartingError`.
- **The executor layer already treats it as optional** —
  `SqlExecutor.execute(sql, *, idempotency_key: str | None = None)`
  (`catalog/sql.py:62`). The layer users touch is stricter than the layer that
  consumes it.

The predictable result is in the example you sent: `idempotency_key="343242dsrew"`.
A keyboard mash is the rational response to a required argument whose purpose
isn't visible at the call site — but it is also the handle you would need to
type back into `catalog.retry_pending(...)` later, so the mash quietly
forfeits the one feature the argument exists for.

The name also over-promises. For `datacopy`, re-running with the same key does
**not** make the operation a no-op: with the default `overwrite=False` the second
run raises `CatalogOperationError: target ... already exists — pass
overwrite=True to replace it` (`catalog/operations.py:402-408`). "Idempotency
key" reads as "safe to re-run"; it isn't.

- **Suggestion:** make it optional and fill it in behind the scenes — see 2a.
- **Open question:** was a server-side idempotency contract ever intended (a
  header Dremio or DDS would honour), or is local retry bookkeeping the whole
  design?

#### 2a. Proposed: make `idempotency_key` optional and prefill it

**Position to put to the team** (proposed here, not yet discussed with them):
the idempotency key is a sound concept for larger, multi-step transactions —
that is where knowing "this is the same unit of work as before" earns its
keep — but it is overkill for one-time execution code. It should be an
**optional** parameter, filled in behind the scenes when the caller doesn't
supply one.

The signature change is backwards compatible and stops at the operations layer:
`idempotency_key: str | None = None` on the 19 `Catalog` methods and their
`operations.*` counterparts. Nothing below needs touching — `SqlExecutor`
already declares it optional (`catalog/sql.py:62`), and every existing caller
that passes a key keeps working unchanged.

**How to prefill — the one real design choice.**

*Option A: a fresh random key per call (uuid4).* Simplest to implement, but the
key is the handle you need for `catalog.retry_pending(key)`, and with a random
one the custodian never sees it. It would have to be surfaced somehow — returned
on the result, exposed as `catalog.last_idempotency_key`, or `retry_pending()`
with no argument meaning "the most recent pending one". Every stalled run also
leaves a *new* orphan entry in the retry-state file, which nothing prunes.

*Option B (recommended): derive it deterministically* from the operation name
plus the arguments that identify the work — for `datacopy`, the resolved source
and target paths. Re-running the same script regenerates the same key, which
matters because **re-running the script is what a custodian actually does** after
a cold-engine stall. They never have to learn `retry_pending` exists:

- first run stalls → `retry_state.record(...)` writes the derived key, attempts 1
  (`catalog/retry_state.py:82-91`);
- the custodian re-runs the same script → same derived key → attempts 2 under the
  same entry, not a second orphan;
- it succeeds → `retry_state.clear(key)` removes it
  (`catalog/operations.py`, end of `_run_steps` / `_run_actions`).

The retry-state file stays bounded by the number of distinct operations, not by
the number of attempts.

One caution worth stating so nobody trips on it later: with a derived key, two
genuinely separate runs with identical arguments (copy, delete the target, copy
again) share a key. Since the key is only local bookkeeping and is cleared on
success, that is harmless — but it is a real consequence of B and should be a
deliberate choice, not a surprise.

**Where the parameter should stay explicit**

- The read-only operations should not take one at all — `gettablesfrom`,
  `gettableitemsfrom`, `getwikifrom`, `gettagsfrom`, `getmetafromwiki`. There is
  nothing to resume; today `_fetch_step` records them anyway.
- Genuinely multi-step operations keep the explicit parameter available, because
  that is the case the concept was designed for — notably `datamove`, whose own
  module docstring flags the CREATE-succeeded-then-DROP-stalled gap that a blind
  retry cannot resolve (`catalog/operations.py:30-37`).
- Anything a scheduler drives, where the caller wants a stable handle it chose
  itself rather than one the library derived.

- **Open question:** should `retry_state` gain an age-out or a cap regardless?
  `list_pending()` exists (`catalog/retry_state.py:114`) but nothing ever prunes
  the file, and with auto-filled keys entries will be created more often than
  they are today.

#### 3. There is no command line

No `[project.scripts]` in `pyproject.toml` (neither branch), no `__main__.py`,
and no `argparse` / `click` / `typer` anywhere under `src/`. "Command-line /
batch scripting style" therefore means: write a Python file, import the right
class, construct it with credentials, call a method, print the result.

The good news is the groundwork is already right: `catalog/operations.py`
exposes every operation as a plain module-level function taking an executor
(`operations.table2view(executor, ...)`, `datacopy(executor, ...)`, …), and
`Catalog`'s methods are thin delegations to them (`client.py:174-332`). A CLI
would be an adapter over that layer, not a rewrite.

- **Suggestion:** one console entry point, one subcommand per verb, arguments
  in the same order as the Python functions:

  ```bash
  eea-catalog datacopy SRC DST --create-target-folder
  eea-catalog gettablesfrom bwd --json
  eea-ingest ./my_data biodiversity.uploads --format parquet --parallelism 4
  ```

  with credentials resolved per #1 (nothing on the command line), a non-zero
  exit code on failure, and `--json` for anything a shell script needs to parse.
- **Open question:** do custodians run these on their own machine, in the
  project container, or as a scheduled Hub notebook? That decides whether the
  CLI or a still-simpler Python one-liner is the primary surface.

#### 4. The verbs that match the custodians' vocabulary are the unimplemented ones

`draft2version` and `publishversion` both raise `NotImplementedError`
(`catalog/operations.py:326` and `:340`). Those are precisely the two
domain-level actions in the draft → version → publish workflow.

Your example is that exact workflow — promoting
`…bwd.draft.bw_assessment.assessments` to
`…bwd.versions.2025_6.bw_assessment.assessments` — done by hand with the
generic `datacopy` and two fully spelled-out paths. The custodian ends up
performing the plumbing that the domain verb was meant to hide.

- **Suggestion:** treat `draft2version` / `publishversion` as the priority, and
  let them own the path arithmetic (see #5): `draft2version("bw_assessment",
  version="2025_6")` rather than two 7-segment strings.
- **Open question:** what is the intended semantics — is a version a physical
  copy (today's `datacopy` behaviour), or a view repoint? `publishversion`'s
  docstring says the consumer view repoint, but the draft→version step is
  undecided in code.

#### 5. Catalog paths are opaque strings, though the taxonomy is known

`datacopy` takes two free-form dotted strings. In your example they are seven
segments long and differ only in the middle:

```
catalog.water_management_resources.bathing_water.bwd.draft.bw_assessment.assessments
catalog.water_management_resources.bathing_water.bwd.versions.2025_6.bw_assessment.assessments
```

Everything before and after `draft` / `versions.2025_6` is repeated by hand,
and nothing validates the shape — `_quote_path` just splits on `.` and quotes
each segment (`catalog/operations.py:90-96`). A typo in segment 2 surfaces as
a Dremio error, not a local one.

Meanwhile the library already encodes this taxonomy elsewhere:
`dds_ingestion/common/catalog.py` documents the path as
`{source}/{domain}/{subdomain}/{dataflow}/...` and hard-codes
`DATAFLOW_DEPTH = 3`, refusing to create anything above that level.

- **Suggestion:** a small path type or builder shared by both packages, so the
  custodian names the dataflow and the stage and the library assembles the
  string — and so a wrong number of segments fails locally, before any call.
- **Open question:** is `lakehouse_structure.md` (referenced from
  `dds_ingestion/common/catalog.py`) the authoritative taxonomy? If so it could
  drive validation directly.

#### 6. Batch-unfriendly output

- `logger = logging.getLogger("eea_datalakehouse.dds_ingestion")`
  (`dds_ingestion/folder.py:39`) — no handler is configured anywhere in the
  package, so in a plain script every `logger.info` about resuming or
  re-uploading goes nowhere unless the custodian configures logging first.
- Progress is a tqdm bar (`dds_ingestion/progress.py:37-43`), sensible in a
  notebook; in a cron log it writes carriage returns, and the no-tqdm fallback
  prints one line per file.
- Results come back as Python objects (`IngestOutcome`, `SqlResult`,
  `TableInfo`), so reporting what happened requires more Python.

- **Suggestion:** as part of the CLI in #3 — a default log line per step to
  stderr, results to stdout as JSON on request, and progress that detects a
  non-TTY and degrades to periodic lines.

#### 7. Proposed: a session context, so paths stop being absolute

**Position to put to the team** (proposed here, not yet discussed with them):
let the user set the session "context" once — the SQL `USE <schema>` idea — so
the library has a default schema to resolve short paths against. This is the
direct fix for #5, and it is what makes the command-line mental model work:
`cd` once, then use relative names.

On your own example it collapses two 7-segment strings into two short ones:

```python
catalog.use("catalog.water_management_resources.bathing_water.bwd")
catalog.datacopy("draft.bw_assessment.assessments",
                 "versions.2025_6.bw_assessment.assessments")
```

Everything the two paths had in common moves into the `use()` line, said once.
What is left is exactly what differs — which is the part the custodian is
actually thinking about.

**The important finding: this cannot be Dremio's own context.**

Dremio's `POST /api/v3/sql` does accept a `context` array alongside `sql`, and
the client currently sends only `sql` (`catalog/sql.py:165`) — so passing it
through looks like the obvious one-line implementation. It isn't, because only
one of the three ways this library names things would be affected by it:

1. **SQL DDL** — `CREATE TABLE {target} AS SELECT * FROM {source}`
   (`catalog/operations.py:419-424`). A server-side context *would* apply here.
2. **INFORMATION_SCHEMA lookups** — `_entry_exists` and `_entry_kind`
   (`catalog/operations.py:190-232`) and `gettablesfrom`
   (`catalog/operations.py:613-650`) compare `"TABLE_SCHEMA"` against the path
   as a **string literal**. `TABLE_SCHEMA` always holds the full dotted path, so
   a session context does not rewrite it — a relative path simply fails to
   match, `_entry_exists` returns `False`, and `datacopy` reports
   `source ... does not exist` for a source that is right there. Silent and
   confusing.
3. **Dremio's catalog REST API** — `_lookup_by_path` does
   `"/".join(path.split("."))` against `/api/v3/catalog/by-path/…`
   (`catalog/rest.py:55-60`) and `create_folder` posts `path.split(".")`
   (`catalog/rest.py:261`). That API has no context concept at all; it needs the
   absolute path. Folders, wiki and tags all go through it.

So the context has to be **resolved client-side** — joined onto the front of a
relative path before anything else happens — which is also the better answer:
it keeps one code path for all three mechanisms, it works identically over REST
and Flight, and it lets a wrong path fail locally instead of as a Dremio error.
There is already precedent for the library resolving paths on the caller's
behalf: `_resolve_target_path` implements `cp source dest/` semantics
(`catalog/operations.py:234-249`).

**Where the context should come from** — the same three-source pattern as the
credentials in #1, so a script can set it without touching code:

- `catalog.use("…")` — mirrors the SQL the custodians already know, and can be
  called more than once in a script that touches two dataflows;
- `Catalog(context="…")` — for the one-shot case;
- an environment variable (say `EEA_CATALOG_CONTEXT`), so a batch job sets it
  alongside the credentials and the script body carries no paths at all. Worth
  asking whether `%init` should export it too.

**Where to put the boundary.** `dds_ingestion/common/catalog.py` already draws
one: `DATAFLOW_DEPTH = 3` — segments 0-2 (`source` / `domain` / `subdomain`)
must already exist and are never created, and from index 3 down is the
dataflow's own space. That gives a principled default context of the first
three segments (`catalog.water_management_resources.bathing_water`), leaving
`bwd.draft.…` relative. Your example suggests the more useful day-to-day
boundary is one deeper — the dataflow itself (`….bathing_water.bwd`) — since
that is what the two paths actually share. Both are defensible; the team should
pick one rather than leaving it to each script.

**The one thing that must not be fudged: relative vs absolute.** Guessing —
"try it relative, fall back to absolute" — is the wrong answer here, because
the failure mode is silent misresolution into a real but wrong location. Most
operations would merely error, but `deletefolder(cascade=True)`
(`catalog/rest.py:340`) deletes a subtree depth-first, and `createfolder`
would quietly build a new one in the wrong place. Suggested rule: with a
context set, **every path is relative to it**, and an absolute path is written
with an explicit marker (a leading `.`, or an `absolute("…")` helper) — the
same unambiguous split a shell makes between `foo` and `/foo`. Whatever the
rule, destructive operations should echo the fully resolved path before acting.

- **Open question:** should the context also apply to
  `FolderIngest(target_catalog_path=…)`, so ingest and catalog take paths the
  same way? They are the two halves of the same job and currently share no path
  handling at all.
- **Open question:** one context, or a source/target pair? A draft→version
  promotion has a common prefix; a copy between two dataflows does not, and
  would need one of the two paths spelled out in full.

#### 8. Proposed: `datacopy` should accept a folder and copy everything in it

**Position to put to the team** (proposed here, not yet discussed with them):
a custodian should be able to point `datacopy` at a dataset folder and have
every table inside it copied, rather than making one call per table.

**Today it doesn't just refuse — it refuses with the wrong reason.**
`datacopy`'s first step is `check_source_exists`, which calls `_entry_exists`
(`catalog/operations.py:190-206`): an `INFORMATION_SCHEMA."TABLES"` lookup for
`TABLE_SCHEMA = <parent>` and `TABLE_NAME = <leaf>`. A folder has no row there,
so the call fails with

```
CatalogOperationError: source '…bwd.draft.bw_assessment' does not exist
```

for a folder that plainly does exist. The library can already tell the
difference — `CatalogRestClient.is_folder()` (`catalog/rest.py:76-97`) — and it
already uses it, but only on the *other* side: `_resolve_target_path` asks
`is_folder` about the **target** to implement `cp source dest/` semantics
(`catalog/operations.py:234-249`). Folder-aware on the target, blind on the
source. Even without the feature, the message should say "is a folder" rather
than "does not exist".

**Every building block for the real thing already exists:**

| Need | Already there |
|---|---|
| detect a folder source | `CatalogRestClient.is_folder` (`rest.py:76`) |
| enumerate its contents | `gettablesfrom` — every table/view under a schema, **at any depth, in one query** (`operations.py:613-650`) |
| create target subfolders | `ensure_folder_path` — creates missing levels, tolerates already-there (`rest.py:275-312`) |
| tell a table from a view | `_entry_kind` (`operations.py:209-232`) |
| land inside a folder | `_resolve_target_path` (`operations.py:234-249`) |

So this is wiring existing parts together, not new machinery.

**And it is what `draft2version` needs anyway.** Per the taxonomy in
`dds_ingestion/common/catalog.py`, the dataset folder is
`…bwd.draft.bw_assessment` and `assessments` is a table inside it. "Copy a
dataset folder" and "promote a draft to a version" are the same operation at
the same level — so #8 is the mechanism #4 is missing, not a separate feature.

**What the team has to decide** — none of these have an obvious default:

- **Views.** `gettablesfrom` returns tables *and* views, but deliberately does
  not select `TABLE_TYPE` (its own docstring says so). Two problems follow.
  Copying a view with `CREATE TABLE … AS SELECT *` silently **materialises it
  into a table** — the copy is a different kind of thing from the original. And
  copying it *as* a view leaves its definition pointing at the original source
  tables, so the copied folder isn't self-contained. `datamove` already treats
  table/view confusion as a bug class worth auto-detecting
  (`operations.py:447-457`). Cheap first step: add `TABLE_TYPE` to
  `gettablesfrom`'s SELECT, so a folder copy knows per entry without N extra
  round-trips.
- **Depth.** `gettablesfrom` is already recursive — its `LIKE 'schema.%'`
  matches across dots, so it returns the whole subtree. That may be exactly
  right for a dataset folder, but it should be a stated choice (`cp -r`), not
  inherited by accident from the helper.
- **Partial failure.** One CTAS becomes N. `_run_actions` records
  `step k/N` and `retry_pending` re-dispatches the operation **from the
  beginning** (`operations.py:1167-1195`) — so with the default
  `overwrite=False`, a resumed folder copy fails on every table that already
  made it. It needs skip-what-exists, or a per-table record of what succeeded.
  There is a precedent in this same library: `FolderIngest._already_done()`
  skips files the server reports as already uploaded
  (`dds_ingestion/folder.py:322-341`), and `retry_state` already persists a
  free-form `params` dict that could carry a done-list
  (`catalog/retry_state.py:40`).
- **`overwrite=True` at folder scale.** Does it mean "replace each colliding
  table" or "replace the target folder"? Very different blast radius, and the
  current wording doesn't extend cleanly.
- **Parallelism.** `FolderIngest` copies 4-way with a `ThreadPoolExecutor`
  (`dds_ingestion/folder.py:363`). Tempting here, but `datacopy` is Flight-only
  and `FlightSqlExecutor` holds one `FlightClient` with a lazily-cached
  `_auth_header` — that lazy initialisation is unguarded, and this repo has not
  verified concurrent statement execution on one Flight client. N concurrent
  CTAS may also just queue on the Dremio engine. Worth measuring before
  assuming it helps.
- **Return type.** `datacopy` returns one `SqlResult`. A folder copy needs a
  per-table summary — the ingest side already has the shape for this in
  `IngestOutcome` (`copied` / `skipped` / per-item results,
  `dds_ingestion/folder.py:57-71`).

- **Caveat for whoever implements it:** `is_folder` is best-effort by design —
  it returns `False` on *any* lookup failure, because some Dremio source types
  (the internal Arctic/Nessie-backed ones) answer a nested by-path lookup with
  a 400 rather than a 404 (`catalog/rest.py:84-96`). On such a source, folder
  detection silently degrades back to today's "does not exist". That needs a
  decision too: fail loudly, or fall back to treating the path as a table.
- **Open question:** should `datamove` get the same treatment? It carries the
  identical single-entry assumption, and moving a dataset folder is an equally
  natural request — but copy-then-drop across N entries has a much worse
  partial-failure story, which the module docstring already flags for the
  single-entry case (`operations.py:30-37`).

#### 8a. Proposed: split it into two verbs rather than overloading `datacopy`

**Position to put to the team:** instead of `datacopy` detecting what it was
pointed at, have two explicit verbs — one for a single table, one for a whole
folder.

This is the better shape, and not only on taste. Four concrete reasons:

- **Detection is best-effort, so overloading is a silent-behaviour-change
  risk.** `is_folder` returns `False` on any lookup failure, including the 400
  that Arctic/Nessie-backed sources answer with (`catalog/rest.py:84-96`). An
  overloaded `datacopy` would quietly run the *single-table* path on a
  false negative and fail with the misleading "does not exist". With the intent
  declared in the verb, the same false negative becomes a clear error — "this
  is not a folder" — because the library knows what the caller meant.
- **The return types genuinely differ.** A single copy returns `SqlResult`; a
  folder copy needs a per-entry summary (see #8). Overloading forces a union
  return that every caller has to narrow.
- **The parameters don't overlap.** Depth/recursion, per-entry parallelism and
  a done-list only make sense for the folder verb; `overwrite` means different
  things at the two scales. Overloading means parameters that are silently
  ignored half the time — the same trap `Catalog(username=…)` already sets,
  where the argument matters only if you happen to call a Flight operation.
- **It matches the naming already in use.** The existing verbs are lowercase
  concatenations — `gettablesfrom`, `gettableitemsfrom`, `draft2version`,
  `createfolder`. `tablecopy` / `datasetcopy` sit in that style without
  introducing a new convention.

**The one thing to settle first: "dataset" is an overloaded word here.**

- In this project's taxonomy it means the folder — `common/catalog.py` calls
  `bw_assessment` "the dataset folder", one level below `draft`.
- In **Dremio's** own vocabulary a *dataset* is a table or a view — that is what
  PDS (physical dataset) and VDS (virtual dataset) stand for.

So to a Dremio-fluent reader `datasetcopy()` would suggest copying exactly one
table, which is the opposite of the intent. Three ways out, in the order I'd
rank them:

1. `tablecopy()` / `foldercopy()` — "folder" is already this library's own word
   for the thing (`createfolder`, `deletefolder`, `ensure_folder_path`,
   `is_folder`), so it stays internally consistent and collides with nothing.
2. `tablecopy()` / `datasetcopy()` as suggested, with the EEA meaning stated
   loudly in the docstring — fine if the custodians' vocabulary is what matters
   and Dremio's is not.
3. Keep one `datacopy` with an explicit `recursive=True` — the `cp -r` model,
   which fits the command-line framing of #7, but keeps the union return type.

- **Open question:** if this splits, what happens to `datacopy`/`datamove`?
  Both are exported (`catalog/__init__.py:29-30`), used throughout
  `debugger/debug_run.py`, and documented in the README — so it's an alias or a
  deprecation, not a rename. And does `datamove` split the same way
  (`tablemove` / `foldermove`), or stay single-entry given its worse
  partial-failure story?

#### 9. Proposed: settle the naming convention — `tableToView`, `datasetToViews`

**Position to put to the team:** replace the `2`-as-"to" names with spelled-out
word separation — `table2view` → `tableToView` — and add the folder-level
counterpart `datasetToViews()`, matching the `tablecopy`/`datasetcopy` split in
#8a.

**The underlying problem is bigger than the `2`: this module currently uses
three naming styles at once.**

| Style | Names |
|---|---|
| lowercase run-together | `datacopy`, `datamove`, `deleteview`, `createfolder`, `deletefolder`, `deletetags`, `deletewiki`, `gettablesfrom`, `gettableitemsfrom`, `gettagsfrom`, `getwikifrom`, `getmetafromwiki`, `settagsto`, `setwikito`, `publishversion` |
| digit-for-word | `table2view`, `draft2version`, `setmeta2wiki` |
| snake_case | `retry_pending` |

All 19 are public, all in the same `__all__` (`catalog/__init__.py:56-88`) and
the same class — including `retry_pending`, which is snake_case sitting beside
`gettablesfrom` in `Catalog` (`catalog/client.py:250`, `:327`).

And the layer immediately below is *entirely* snake_case. `Catalog.createfolder`
→ `operations.createfolder` → `catalog_rest.create_folder` and
`ensure_folder_path` (`catalog/operations.py:1104-1109`). The same concept is
spelled two different ways one line apart. Same for `deletefolder` →
`delete_folder`, and for `is_folder` / `is_table_or_view` / `get_wiki` /
`set_wiki` / `get_tags` / `set_tags`, plus `load_creds`, `load_base_url`,
`scan_folder`, `ingest_folder`, `resolve_executor`, `list_pending` elsewhere in
the library.

So this is one decision covering all 19 names, not a fix for three of them.

**Which convention.** The instinct is right either way — `gettableitemsfrom` is
hard to read and `2` is not a word. The two candidates:

- **camelCase** (`tableToView`, `datasetToViews`, `getTablesFrom`) — as
  proposed. Reads well, and it matches the JSON world these operations talk to:
  Dremio's own fields are camelCase (`entityType`, and the settings keys
  `ddsServerUrl` / `accessToken` in `dremio_identity.py:85-90`).
- **snake_case** (`table_to_view`, `dataset_to_views`, `get_tables_from`) —
  PEP 8, and it matches the ~20 functions in this library that already use it,
  including the exact methods these verbs delegate to. It is also the only
  option that can be *enforced*: ruff's `N` (pep8-naming) rules would keep it
  from drifting again, and `select` currently omits `N`
  (`pyproject.toml:53`) so nothing catches the drift today. Choosing camelCase
  means `N` can never be turned on without a wall of `noqa`.

My recommendation is snake_case, for the enforceability and for consistency
with the layer underneath — but the important thing is that one of them is
chosen for all 19 rather than the styles continuing to mix. Either beats what
is there now.

**The verb matrix, if #8a and this both go ahead** (camelCase spelling shown
as proposed; substitute snake_case throughout if that wins):

| single entry | whole folder |
|---|---|
| `tableToView` | `datasetToViews` |
| `tableCopy` | `datasetCopy` |
| `tableMove` | `datasetMove` |

- Note the plural in `datasetToViews` — deliberate, since the operation yields
  many views, but it makes the set irregular next to `datasetCopy`. Worth
  deciding whether plurals track the output or the names stay uniform.
- `draft2version` and `setmeta2wiki` are the other two `2` names and should move
  in the same sweep (`draftToVersion`, `setMetaToWiki`).
- The "dataset" vs Dremio's PDS/VDS meaning of the word, raised in #8a, applies
  here too: `datasetToViews` reads to a Dremio-fluent user as "turn one dataset
  into several views".

**Migration.** These names are already released — tags up to `v0.1.10`, and the
README tells notebooks to install from `@main` / `@staging`, so custodians'
existing scripts import them. They are also used throughout
`debugger/debug_run.py` and documented in both READMEs. So: rename in one sweep,
keep the old names as thin aliases emitting a `DeprecationWarning` for one
release, then drop them. Worth doing now precisely because adoption is still
small — the cost only grows.

- **Open question:** does the rename extend to the `Catalog` class's own
  parameters (`target_catalog_path`, `conflict_mode`) and to `dds_ingestion`'s
  public surface, or is it scoped to the catalog verbs?

#### 10. Proposed: `createfolder` / `deletefolder` should take more than one folder

**Position to put to the team:** both should accept a set of folders, not a
single path — `mkdir -p a b c` and `rm -r a b c` take multiple operands, and
that is the shell model these verbs are already imitating (their own docstrings
say "`rmdir` vs `rm -r`", `catalog/operations.py:1131`).

Today both are strictly single-path: `createfolder(catalog_rest, path, *,
create_parents=False, …)` (`catalog/operations.py:1070`) and
`deletefolder(catalog_rest, path, *, cascade=False, …)`
(`catalog/operations.py:1119`). Setting up a dataflow means one call per folder.

**This is the cheapest of the batch features to add** (cf. #8), because both
underlying operations are already idempotent:

- `create_folder` tolerates a 409 and reports whether it newly created the
  folder (`catalog/rest.py:255-273`);
- `delete_folder` returns early when the folder is already gone
  (`catalog/rest.py:352-354`).

So a partially-completed batch can simply be re-run — which matters, because a
batch shares one idempotency key and `retry_pending` re-dispatches the operation
**from the start** (`catalog/operations.py:1167-1195`). For #8's copy that is a
real problem; here it is harmless.

**What the team has to decide:**

- **Ordering is not the order the user typed, and it differs by verb.**
  - Deleting without `cascade` must go **deepest-first**, or removing a parent
    before its child fails with "folder is not empty"
    (`catalog/rest.py:359-364`).
  - Creating without `create_parents` must go **shallowest-first**, or creating
    `a.b.c` before `a.b` fails with "parent does not exist"
    (`catalog/operations.py:1098-1102`).

  Opposite sorts, and a naive `for p in paths:` loop gets both wrong. Sorting by
  depth inside the batch verb makes a list of related folders "just work" —
  which is most of the value of the feature.
- **Report what happened.** Both currently return `None`, and
  `createfolder(create_parents=True)` **throws away information it already
  has**: `ensure_folder_path` returns the list of levels it actually created
  (`catalog/rest.py:275-312`) and `create_folder` returns whether the folder was
  new — then `createfolder` discards both. A batch needs a per-path result
  (created / already existed / failed) and the pieces are already there.
- **Fail-fast or continue-on-error?** For creates, continuing and reporting is
  probably right. For deletes it is not obviously right, and the default should
  be a deliberate choice rather than whatever falls out of the loop.
- **Echo the resolved paths before deleting.** Especially once #7's context
  makes the written paths relative — a batch `deletefolder(cascade=True)` is the
  single most destructive call in this library.

**On API shape — this one can safely accept both, unlike #8a.** The argument
against overloading `datacopy` was that folder-vs-table detection is
best-effort and can silently pick the wrong path. Here there is no detection:
`str` vs `Sequence[str]` is unambiguous at runtime, so `path: str |
Sequence[str]` keeps every existing call working and needs no new verb. If the
team prefers explicit plural verbs anyway (`createfolders` / `deletefolders`),
that is a naming call under #9, not a correctness one.

- **Note for whoever implements it:** `deletefolder` is the only operation that
  bypasses the shared `_run_steps` / `_run_actions` helpers and hand-rolls its
  own record-and-clear (`catalog/operations.py:1135-1142`). A batch version
  should go through the shared helper so step numbering and retry recording
  behave like everything else.
- **Note:** the `_OPERATIONS` registry that `retry_pending` dispatches through
  (`catalog/operations.py:1145-1163`) is keyed by operation name — any new or
  renamed verb from #9 or #8a has to be added there too, or its retries break
  with a `KeyError`.

#### What "simple" could look like

Sketch to argue about, not a proposal to implement — the same job as your
example, with #1, #2, #4, #5 and #7 addressed. Each line that disappears is one
of the findings above:

```python
from eea_datalakehouse.catalog import Catalog

# no URL, no token, no username — resolved from the panel / %init / env  (#1)
with Catalog() as catalog:
    catalog.use("catalog.water_management_resources.bathing_water.bwd")   # (#7)
    catalog.datacopy("draft.bw_assessment.assessments",                   # (#2: no key)
                     "versions.2025_6.bw_assessment.assessments")
```

and once the domain verb exists (#4), with the context supplying everything
above the dataflow:

```python
with Catalog() as catalog:
    catalog.use("catalog.water_management_resources.bathing_water.bwd")
    catalog.draft2version("bw_assessment", version="2025_6")
```

or, with #3, nothing in Python at all:

```bash
export EEA_CATALOG_CONTEXT=catalog.water_management_resources.bathing_water.bwd
eea-catalog draft2version bw_assessment --version 2025_6
```

Compare against what the same job takes today — full credentials, a hand-made
idempotency key, and the taxonomy typed twice:

```python
with Catalog(DREMIO_URL, DREMIO_TOKEN, username=DREMIO_USERNAME) as catalog:
    catalog.datacopy(
        "catalog.water_management_resources.bathing_water.bwd.draft.bw_assessment.assessments",
        "catalog.water_management_resources.bathing_water.bwd.versions.2025_6.bw_assessment.assessments",
        create_target_folder=True,
        idempotency_key="343242dsrew",
    )
```

## Ideas / parking lot

_Larger or fuzzier things that aren't defects — worth a conversation, not a ticket yet._

## Questions for the team

_Things we can't resolve from the code alone._

- Is `docs/` intended to become a real documentation folder in this repo, or is
  documentation hosted elsewhere?
