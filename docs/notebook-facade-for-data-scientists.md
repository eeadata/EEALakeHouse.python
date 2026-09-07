# Making the catalog API approachable for data scientists in JupyterLab

Idea note, not a plan — captures a discussion, nothing here is agreed or scheduled.

**Prototype exists.** `CatalogSession`/`IngestSession`
(`src/eea_datalakehouse/catalog/session.py`,
`src/eea_datalakehouse/dds_ingestion/session.py`) implement "Queue calls, then
commit" and "Two sessions, not one" below; `%catalog`/`%ingest`
(`src/eea_datalakehouse/notebook/magics.py`) implement "Two magics, two
sessions" and "Loading the magics without typing a magic to do it" over them;
relative-path context (a leading `.`, auto-inferred from usage, plus a Comm
channel for an integration to push it invisibly) implements "Pre-filling
catalog context" — see `docs/notebooks/catalog_session_example.ipynb` and
`docs/notebooks/ingest_session_example.ipynb` for worked examples. Still just
a prototype behind an opt-in `notebook` extra, not reviewed or adopted — the
rest of this file is the design reasoning behind it, kept as written.

## The facade's surface, end to end

A consolidated view of what all the sections below amount to — every other
section is the reasoning behind one piece of this; this one is just the
result, in one place.

**What it includes:**

- `CatalogSession`/`IngestSession` — the engine: queue-then-commit, all-or-
  nothing rollback on the catalog side (ingestion has none — a documented
  gap, not an oversight), auto-derived idempotency keys, context inference.
- `%catalog`/`%ingest` — the surface a custodian actually touches: two
  IPython magics, each a thin dispatcher onto one of the sessions above.
- Auto-registration — `import eea_datalakehouse.notebook` is the only setup
  step; no `%load_ext`, no magic syntax to learn just to turn it on.
- A Comm channel — the receiving half of "click a catalog leaf, get a
  working path," for whenever the JupyterLab extension's UI side exists.

**What it exposes** — a small, curated verb set, not the full developer API:

| catalog | ingestion |
| --- | --- |
| `copy`, `move` | `ingest` |
| `tag`, `untag` | |
| `set_wiki`, `delete_wiki`, `set_meta` | |
| `create_folder`, `delete_folder` | |
| `commit(retry=True)` | `commit(retry=True)` |

Every call takes a path and the arguments a custodian actually thinks in
(`overwrite=True`, a list of tag strings, a folder path) — never an
`idempotency_key`, never a raw `SqlExecutor`/`CatalogRestClient`, never a
`Catalog`/`FolderIngest` object to manage. A path can be relative
(`.water_temperature`) once context exists, which it usually already does by
the second call in a session.

**How a custodian uses it:**

```
import eea_datalakehouse.notebook

%ingest ingest(folder="./bw_2026", target_catalog_path="bwd.reference", data_format="parquet")
%ingest commit(retry=True)

%catalog tag(".water_temperature", ["reviewed"])
%catalog set_meta(".", tags=[{"tag_name": "owner", "tag_value": "bw-team", "tag_title": "Owner"}])
%catalog commit(retry=True)
```

Ingest first and commit it fully; only then touch the catalog side, in a
separate `%catalog` batch — that ordering is enforced by convention (two
sessions), not by code. Nothing reaches Dremio until a `commit`; a failure
prints a short message and (catalog side) undoes whatever it safely can,
rather than a traceback.

## The problem

`eea_datalakehouse.catalog` is built the way a developer library should be:
typed exceptions, explicit `idempotency_key`s, retry state, `overwrite`/`cascade`
flags with precise semantics. That is the right shape for the people writing
this package. It is the wrong shape for a data scientist in the EEA Lakehouse
JupyterLab (`eeadata/EEALakeHouse`) who wants to copy a table or tag a folder
once, in a notebook cell, without first learning what an idempotency key is or
why an operation can raise `CatalogOperationError`.

This is the same gap `dev-notes.md`'s "The API is shaped for a developer, not
for a custodian writing a one-off script" entry already names for
`dds_ingestion` — worth reading together with this note, since a fix likely
wants to cover both packages the same way rather than twice.

## Options considered

1. **Thin notebook facade over the existing `Catalog` client** — a layer that
   auto-generates `idempotency_key`s, catches the typed exceptions and prints a
   short human-readable message instead of a traceback, and exposes a small set
   of high-level verbs as plain functions or IPython `%magic` commands.
   `Catalog`/`operations.py` stay exactly as they are underneath — the facade is
   additive, not a fork.
2. **A full widget-based UI** (ipywidgets file-browser style) embedded in
   JupyterLab — more discoverable for someone who has never touched the API at
   all, but a much bigger build and an ongoing maintenance surface (widget
   state, layout, JupyterLab version compatibility).

**Leaning towards (1).** It is the smaller build, keeps one source of truth for
behaviour (the facade never reimplements retry/copy logic, only hides its
ceremony), and can ship incrementally — one friendly wrapper at a time — rather
than as a big-bang UI project. The tradeoff is real, though: the friendly layer
necessarily gives up some fine-grained control (custom idempotency keys, raw
exception handling) in exchange for fewer things to learn, so it has to stay a
layer *in front of* the developer API, never a replacement for it.

## Session context lives in the Python process, not in Dremio

The library is used from JupyterLab, so it already has a natural session
boundary: the kernel. The facade should keep its state there — in the Python
process, scoped to that one kernel's lifetime — rather than trying to model or
fetch any notion of "session" from Dremio itself, which has no such concept for
what we'd need (Dremio's REST API is stateless per call). Concretely, a
module-level or singleton facade object, created once per kernel, could hold:

- a per-session id to seed auto-derived `idempotency_key`s from, so retries
  within one notebook session are naturally scoped and stable without the
  caller inventing anything;
- a "current" catalog path/context (e.g. the last folder touched) so repeated
  calls in the same session can take a relative path instead of the full one
  each time;
- cached best-effort lookups (`is_folder`/`is_table_or_view`) for the session's
  lifetime, since re-checking Dremio on every call is pure overhead for a
  script that touches the same handful of paths repeatedly.

This resets cleanly when the kernel restarts — which matches how a data
scientist already thinks about a notebook session — and needs nothing new on
the Dremio side.

`idempotency_key` in particular must be **fully encapsulated** — a custodian
should never see the concept, let alone pass one in. It's session-derived
plumbing (previous bullet), generated and threaded through entirely inside the
facade; it has no business appearing in a signature a data scientist calls.

## Two sessions, not one — ingestion and catalog are sequential, not peers

Catalog operations can't run before their target exists — a `datacopy`, tag or
wiki update on a table that hasn't been ingested yet has nothing to act on. So
ingestion and catalog aren't two halves of one atomic batch; they're two
phases, strictly ordered: an ingest fully lands and commits, *then* — and only
then — catalog steps referencing what it produced can be queued and committed.
That argues for **two independent sessions with two independent commits**,
each scoped to its own package, rather than one shared queue trying to unify
things that were never real peers:

```python
ingest = lakehouse.ingest_session()
ingest.ingest(folder="./bw_2026", target_catalog_path="…/bwd/reference", ...)
ingest.commit(retry=True)          # must fully succeed before anything below runs

catalog = lakehouse.catalog_session()
catalog.copy("draft.raw_2026", "bwd.reference.water_temperature", overwrite=True)
catalog.tag("bwd.reference.water_temperature", reviewed_by="jdoe")
catalog.commit(retry=True)          # all-or-nothing, but only across catalog steps
```

Each is still "queue calls, then commit" (the ergonomic point that started this
idea — one call that runs everything, not one call per REST request narrated
by hand), and each commit is still **all-or-nothing within its own package**:
every queued step succeeds, or `commit()` rolls back everything that package's
session already did, using that package's own compensating actions. What this
split removes is the harder problem from the earlier draft: a *cross-package*
saga, where an ingest step's rollback had to be composable with a catalog
step's rollback in one undo sequence. That's no longer needed — a catalog
session's rollback only ever touches catalog steps.

**`catalog_session().commit()`'s own rollback** still needs the table below —
this part is unchanged from before, just scoped to catalog alone now:

| step | compensating action |
| --- | --- |
| `datacopy`/`createfolder` | delete what was just created (`deletefolder`/a
  table-and-data delete) |
| `settagsto`/`setwikito`/`setmeta2wiki` | restore the previous tags/wiki text
  (captured before the step ran), or clear them if there was none |

**`ingest_session().commit()`'s own rollback** — needed only if a batch queues
more than one ingest before committing — still runs into the same sharp edge
as before: **a delete-that-also-removes-the-backing-data operation for a
read-only ingest doesn't exist yet**, the gap recorded in
`docs/read-only-ingest-client-plan.md` ("Noted for later: deleting a read-only
table"), which already says it needs a DDS-side endpoint since this package
holds no S3 credentials to do it directly. Until that lands, an ingest
session's rollback guarantee is either limited to a single queued ingest at a
time, or stated plainly as unavailable for a multi-step ingest batch — but
critically, this no longer blocks the *catalog* session's rollback the way it
did when the two were one saga.

Two more things this design has to answer, not just note as open:

- **Compensation can itself fail** (the delete-the-copy call errors while
  rolling back). `commit()` can't then pretend the batch is cleanly rolled
  back — it needs a distinct failure mode ("rolled back" vs. "**rollback
  incomplete**, these steps are unresolved") so a custodian isn't told
  everything's fine when it isn't.
- **Retry and rollback are two different recoveries** for the same failure, and
  the API has to make the caller choose: `commit(retry=True)` re-attempts the
  failed step in place (existing `retry_state` machinery, previous section);
  rolling back undoes everything instead. Defaulting to one or the other is a
  product decision, not a technical one — worth settling explicitly rather than
  picking implicitly by whichever gets built first.

## Two magics, two sessions

If we do offer `%magic` commands (previous section's "possibly also reachable
as..."), splitting them along the existing package boundary — `%catalog` for
the `catalog` verbs, `%ingest` for `dds_ingestion` — now maps directly onto the
two-session split above, rather than needing a shared queue underneath: each
magic owns its own session and its own `commit()`. That mirrors how the code is
already organised, gives a custodian tab-completion scoped to the right
vocabulary instead of one long mixed list, and matches the real dependency —
`%ingest` has to be run, and committed, before `%catalog` has anything to act
on.

## Loading the magics without typing a magic to do it

**Implemented.** `import eea_datalakehouse.notebook` registers `%catalog`/
`%ingest` as a side effect (`notebook/__init__.py`'s `_autoregister`, guarded
against double-registering — and against `%load_ext` afterwards clobbering
it — by checking `magics_manager.registry` first) — so the cost is one
ordinary import line, not a magic syntax to memorize. `%load_ext
eea_datalakehouse.notebook.magics` still works, in either order.

Getting to *zero* typing (no import either) needs something outside this
package's own code, at one of two levels:

- **Environment provisioning, not a labextension** — an IPython startup file
  (`~/.ipython/profile_default/startup/*.py`) or a kernel-spec argument that
  runs the import/`%load_ext` at every kernel start. This could be shipped by
  this package (a small installer helper) or baked into whatever builds the
  Lakehouse JupyterLab image — it doesn't require the `eeadata/EEALakeHouse`
  labextension (TypeScript/UI code) to change at all.
- **Actually modifying the JupyterLab extension** — only if the platform wants
  this injected centrally at the Jupyter *server* level (a `jupyter_server`
  extension hooking kernel startup) rather than per-environment config. The
  heaviest option, and the only one that's genuinely "modify the extension
  instead of the library."

Neither of those two is built — recorded here as the next step if literally
zero typing turns out to matter, not assumed to be needed yet.

## Credentials: what "hiding" can and can't mean

A kernel is trusted user code, not a sandbox — anything the facade can read
(`os.environ["DREMIO_TOKEN"]`, a session's own private attributes) a
custodian's own cell can read too, deliberately or by accident. So "hide the
token" can only mean two different, both worth doing, things:

- **Never let this package be the reason it leaks.** Already true today: the
  token goes straight from `os.environ` into `Catalog(...)`'s constructor
  inside `_build_catalog_session()` and never touches `self.shell.user_ns`
  (the notebook's own variables), and neither `Catalog` nor
  `RestSqlExecutor`/`CatalogRestClient` put it in a `repr()` or an error
  message. Worth keeping as an explicit rule for anything added later to the
  facade, not just an accident of how it happens to be written now.
- **Shrink what's actually at risk if a custodian *does* print it** (`%env`, a
  stray `print(os.environ)`, then saving the notebook — a real and common way
  secrets end up committed in `.ipynb` output cells). This package can't
  prevent that; the platform can, by minting a short-lived, scoped-down token
  per kernel session instead of injecting a long-lived PAT — so what leaks, if
  anything does, expires soon and can't do much. That's a decision for
  `eeadata/EEALakeHouse` (the Hub/image that populates the kernel's
  environment), not this repo.

## Pre-filling catalog context from a JupyterLab tree click

A custodian should never have to know or type the top levels of a catalog
path (space, dataflow/domain, ...) — those are fixed for whatever project
they're in, not a per-call decision. This is the same idea "Session context
lives in the Python process" above already raised (`a "current" catalog
path/context ... so repeated calls can take a relative path`), sharpened with
a concrete trigger: a catalog-tree UI in the JupyterLab extension, where
selecting a leaf pre-fills that context automatically, rather than only
updating lazily from whatever path a call last touched.

This splits into two sides that live in two different repositories.

**The receiving side, here — implemented and encapsulated on both routes in:**
`CatalogSession.set_context(path)`/`_resolve` (`catalog/session.py`) resolve the
open question below with an explicit marker (a leading `.`, e.g.
`session.tag(".water_temperature", ...)`), and — critically — `set_context`
itself is not something a data custodian is expected to call:

- **Ordinary use already keeps it current on its own.** Every queueing verb
  updates the context from whatever path it just touched (the target's
  parent for `copy`/`move`; the touched path itself for a folder-scoped verb
  like `create_folder`/`set_meta`), so a script that never once calls
  `set_context` still gets working relative paths after its first absolute
  one.
- **The Comm channel below is the *other* caller**, not the custodian either.

**The emitting side, in `eeadata/EEALakeHouse` — still not built:** the
catalog-tree UI's click handler needs to push the selected path through a
Jupyter Comm to the target name `eea_datalakehouse.catalog_context`
(`notebook/magics.py`'s `_register_context_comm` already listens for
`{"path": "<selected path>"}` and applies it via `EEALakehouseMagics.
_apply_context` — silently, no cell runs, nothing a custodian could see or
type). That target name and payload shape is the interface contract; opening
the Comm and sending on it from the tree view is separate UI work in that
repository, which this session doesn't have open.

## What the facade would concretely look like

- Sensible defaults everywhere the developer API demands an explicit choice —
  the session-derived `idempotency_key` above is the main one; there should be
  no others once the facade design settles.
- Exceptions translated at the boundary into a short printed message (or a
  `rich`/notebook-display block) rather than surfaced as a raw
  `CatalogOperationError` traceback.
- A handful of high-level verbs matching how a custodian actually thinks about
  the task, e.g. `session.copy(...)`, `session.tag(...)`, rather than the full
  `datacopy`/`gettagsfrom`/`settagsto` vocabulary — queued and committed as
  above, possibly also reachable as one-shot `%magic` commands for a true
  single-call use.
- Ties into the same gaps `dev-notes.md` already flags: missing docstrings on
  `Catalog`'s delegate methods (so `Shift+Tab` shows nothing useful today), and
  no pinned notebook environment to develop/test this against
  (`jupyterlab`/`ipykernel` extra, per that file's last finding).

## Open questions

- Where should the facade live — a new module in this package
  (`eea_datalakehouse.catalog.notebook`?), or a separate thin package installed
  alongside it in the Lakehouse JupyterLab image?
- `%magic` commands vs. plain friendly functions/methods — magics are more
  discoverable inside a notebook cell, but harder to discover *outside* one
  (no `Shift+Tab`, no import to `help()`), and less testable with the normal
  pytest tooling this repo already uses.
- Does this depend on, or should it wait for, the docstring and
  notebook-environment gaps already recorded in `dev-notes.md`?
