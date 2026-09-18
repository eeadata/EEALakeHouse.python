# Making the catalog API approachable for data scientists in JupyterLab

Idea note, not a plan — captures a discussion, nothing here is agreed or scheduled.

**Prototype exists.** `CatalogSession`/`IngestSession`
(`src/eea_datalakehouse/catalog/session.py`,
`src/eea_datalakehouse/dds_ingestion/session.py`) implement "Queue calls, then
commit" and "Two sessions, not one" below; `%catalog`/`%ingest`
(`src/eea_datalakehouse/notebook/magics.py`) implement "Two magics, two
sessions" and "Loading the magics without typing a magic to do it" over them;
relative-path context (a leading `.`, set explicitly via `use`, plus a Comm
channel for an integration to push it invisibly) implements "Pre-filling
catalog context" — see `docs/notebooks/catalog_session_example.ipynb` and
`docs/notebooks/ingest_session_example.ipynb` for worked examples. Still just
a prototype behind an opt-in `notebook` extra, not reviewed or adopted — the
rest of this file is the design reasoning behind it, kept as written.

**Update:** `%catalog` no longer exposes queue-then-commit at the magic
level — each call now commits itself immediately (`CatalogSession` still
does the committing underneath, one call at a time, so the retry/rollback
behaviour described below is unchanged; there's just no longer a separate
`%catalog commit` a custodian has to remember). `%ingest` is unaffected and
still queues/commits as described throughout this doc. Context has also
grown past what's described below: `use(path)` (also via `%%catalog
use(path)`, the cell-magic form) sets it deliberately — making a live
check that `path` exists in the catalog first, unlike everything else
here — and `get_context()` reads it back. `copy`/`move` were later renamed
`datacopy`/`datamove`, matching `Catalog`'s own names instead of inventing
friendlier ones, then renamed again to `data_copy`/`data_move` for
consistency with the rest of the facade's underscored verbs (`Catalog`'s
own `datacopy`/`datamove` are unchanged — only the `CatalogSession`/
`%catalog` wrapper got the underscore); six more verbs were added — read-only
`get_wiki`, `get_tags`, `list`, `schema` (answered immediately, like
`get_context`), and queued `delete_view`/`delete_table` (never reversible,
like `delete_folder`, but unlike the idempotent `Catalog.deleteview`/
`deletetable` they wrap, require `path` to already exist — originally
named `deleteview`/`deletetable` to match, then renamed with an underscore
for consistency with `delete_folder`/`delete_wiki`). `tag`/`untag` were
similarly renamed `set_tags`/`delete_tags` (matching `set_wiki`/
`delete_wiki`'s pattern), and `set_meta` was removed — the raw
`Catalog.setmeta2wiki`/`getmetafromwiki` are still there for a folder's
wiki Meta Data section, just not wrapped by `CatalogSession` any more.

Relative-path resolution grew, then partly retreated, then grew again.
It first grew past a leading `.`: a leading `../` (or a bare `..`) on any
relative path walked up that many levels of the context first, and —
since requiring a dot everywhere turned out to be a real papercut in
practice (a custodian's first instinct after `use(...)` was to type a
bare short name regardless of which verb came next) — every single-path
verb started accepting a bare path with no leading `.` at all too, once a
context existed, the same as `use` already did (`data_copy`/`data_move`/
`create_view` were a deliberate exception at first: since they resolve
`source_path`/`target_path` independently against the *same* starting
context and routinely pair a relative one with a genuinely unrelated
absolute one in the same call, a bare path there stayed absolute always,
context or not — see below for why that exception didn't last). `use`
itself then reverted the other way: it now only ever accepts a whole,
absolute `path` — never resolved against whatever context already exists,
and never a leading `.`/`../` fragment — so pointing context somewhere
always means saying exactly where, with the same live existence check as
before. `list`'s own `path` became optional on top of that — omitted (or
`""`), it lists the current context itself, raising `CatalogSessionError`
if none is set, rather than making a custodian who's already `use()`d
somewhere repeat that same path right back to `list()`.

The dot-optional rule then grew one more exception of its own: a bare path
that already starts with `catalog` (`_ROOT_SOURCE` in `session.py` — this
deployment's one real top-level source) is always taken literally as
absolute, context or not, rather than getting appended to whatever
context happens to be set. Before this, passing a full `catalog....` path
alongside an already-set context — mixing an absolute path with ordinary
relative use in the same session — silently produced a nonsense
double-nested path (`f"{context}.catalog...."`) unless a custodian
remembered to clear context first; `_ROOT_SOURCE` makes that case
unambiguous instead. `data_copy`/`data_move`/`create_view` didn't get this
treatment at first — the reasoning above still seemed to hold, that a
`_ROOT_SOURCE` check couldn't tell a deliberately relative bare path
(still meant to be appended there) apart from one that just happened not
to start with `_ROOT_SOURCE` — until it became clear that reasoning was
wrong: since a *genuinely* absolute path in this single-source deployment
always starts with `_ROOT_SOURCE` by definition, the "unrelated absolute
target" case these three verbs exist to support is already exactly the
case `_ROOT_SOURCE` disambiguates. There was no real ambiguity left to protect against — only a papercut,
the same one every other verb already had fixed, still hitting
`data_copy`/`data_move`/`create_view` calls that tried a bare relative
path and got a confusing failure instead (worse than a papercut for
`data_copy`/`data_move`, in fact: a bare single-segment path like
`"raw_2026"` isn't valid as a literal absolute path at all — no schema to
split it on — so the failure surfaced as a raw `ValueError` from deep in
`operations.py`, not even a clean `CatalogOperationError`). Dropped the
`dot_required` parameter entirely (`_resolve_path` no longer needs two
modes) and `data_copy`/`data_move`/`create_view` now resolve exactly like
every other verb.

Finally, context stopped being a side effect at all: every queueing verb
used to update it from whatever path it just touched (the target's parent
for `data_copy`/`data_move`/`create_view`, the touched path itself for
`create_folder`) — this is what "ordinary use already keeps context
current on its own" meant throughout the rest of this doc. That auto-update
is gone; `use`/`set_context` are now the *only* two ways context ever
changes. Every verb still *reads* context to resolve its own relative
`path` (unchanged, see above), it just never writes it back — a custodian
found a call that plainly wasn't about navigation (`create_folder`, most
concretely) silently moving context out from under them more surprising
than the convenience of not having to call `use` again was worth. `_resolve`
(the internal helper that used to do the resolve-then-update-context pair)
is gone along with it — every verb now calls `_resolve_path` directly.

`use`'s own output followed from that: since it never queues anything,
`%catalog`'s auto-commit (`_dispatch` in `magics.py`) used to hand back an
empty `CommitReport` — accurate, but useless to look at, and not what a
custodian typing `use(path)` actually wants to see. It now prints
`context set to <path>` instead whenever a commit had nothing to report,
covering `use`/`set_context` today and any future verb with the same
"chains back to `self`, queues nothing" shape (a plain `print()`, not a
returned value IPython would auto-display, matching every other status
line this module prints — see its own module docstring). `%%catalog`'s
own line loop needed a small fix alongside this: it used to treat a bare
`None` result as "an error was already printed", which broke the moment
`use(None)` (clearing context) started legitimately returning `None` too
— see `_DISPATCH_FAILED` in `magics.py`.

Two more `_resolve_path` gaps surfaced once `use`'s own live existence
check made it obvious a custodian would reach for `"."`/`"./"` to mean
"the context, unchanged" (the same way `cd .` does): neither was handled
before, so both silently appended themselves as a literal trailing
`.`/`./` onto the context instead — `_resolve_path(".")` returned
`f"{context}."`, not `context`. Both now resolve to the context exactly
as `get_context()` would show it; `"./name"` also now means the same
thing as `".name"` rather than embedding a stray `/` in the resolved
path. The `%catalog help` note describing all of this was rewritten into
an explicit, itemised list (absolute / `.`-or-`./` / `../` / bare-or-dot
relative) rather than one dense paragraph, once it became the obvious
place a custodian would actually go looking for exactly this.

A freshly built `CatalogSession` no longer starts with an empty context
either: `__init__` now sets it to `_ROOT_SOURCE` ("catalog", the catalog
root) rather than `None`, so a custodian's very first relative call — one
made before ever calling `use()` — already has something to resolve
against instead of raising "no context is set". Every other invariant
above is unchanged (only `use`/`set_context` ever touch context). `use`'s
own `None`/`""` handling changed to match: rather than clearing context
to `None` (a state a fresh session no longer starts in either), it now
resets to `_ROOT_SOURCE` — the same place a custodian already lands on
before ever calling `use()`, so "clear the context" and "go back to
where I started" become the same action. `set_context` — the lower-level
primitive `use` wraps, documented as not something a custodian should
normally reach for directly — kept its old `None` behaviour (clears to no
context at all), since it's still needed as an explicit "unset" for the
JupyterLab-Comm integration path and internal use. `%catalog help`'s note
and `get_context`'s/`use`'s own entries were updated to say so.

See `src/eea_datalakehouse/notebook/magics.py`'s module docstring and
`src/eea_datalakehouse/catalog/session.py`'s `use`/`get_context`/
`_resolve_path`/`list` for the current, authoritative behaviour.

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

| catalog (runs immediately) | ingestion (queue, then commit) |
| --- | --- |
| `data_copy`, `data_move` (data) | `ingest` |
| `list`, `schema`, `delete_table` (table) | |
| `create_view`, `delete_view` (view) | |
| `set_wiki`, `delete_wiki`, `get_wiki` (wiki) | |
| `set_tags`, `delete_tags`, `get_tags` (tags) | |
| `create_folder`, `delete_folder` | |
| `use` (also via `%%catalog use(path)`), `get_context` | |
| | `commit(retry=True)` |

(`%catalog help`'s own ordering follows the same five groups — data, table,
view, wiki, tags — then folders, with `use`/`get_context` always first.)

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

%catalog set_tags(".water_temperature", ["reviewed"])
%catalog set_wiki(".", "# Water temperature\n\nBathing water assessments.")
```

Ingest first and commit it fully; only then touch the catalog side — that
ordering is enforced by convention (a catalog operation can't run before its
target exists), not by code. `%ingest` doesn't reach Dremio until a
`commit`; `%catalog` reaches it the moment each call runs. Either way a
failure prints a short message and undoes whatever it safely can, rather
than a traceback.

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

Catalog operations can't run before their target exists — a `data_copy`, tag or
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
catalog.data_copy("draft.raw_2026", "bwd.reference.water_temperature", overwrite=True)
catalog.set_tags("bwd.reference.water_temperature", reviewed_by="jdoe")
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
selecting a leaf pre-fills that context automatically, rather than the
custodian having to call `use` themselves first.

This splits into two sides that live in two different repositories.

**The receiving side, here — implemented and encapsulated on both routes in:**
`CatalogSession.set_context(path)`/`_resolve_path` (`catalog/session.py`) resolve
the open question below with an explicit marker (a leading `.`, e.g.
`session.set_tags(".water_temperature", ...)`), and — critically — `set_context`
itself is not something a data custodian is expected to call directly
(`use`, added later, is the custodian-facing entry point for setting
context deliberately — see "What it exposes" above — though it takes only
a whole, absolute path, plus a live existence check `set_context` itself
doesn't make):

- **`use`/`set_context` are the only two ways context ever changes.** No
  queueing verb updates it as a side effect of running, even one whose own
  `path` fully resolved to something that could sensibly become the new
  context (an earlier draft of this design had exactly that — every verb
  updating context from whatever it just touched, the target's parent for
  `data_copy`/`data_move`, the touched path itself for `create_folder` —
  but a custodian finding context moved out from under them by a call that
  never looked like it should touch it turned out to be a bigger surprise
  than the convenience was worth; see `use`'s own docstring).
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
  the task, e.g. `session.set_tags(...)`, rather than the full `gettagsfrom`/
  `settagsto` vocabulary — queued and committed as above, possibly also
  reachable as one-shot `%magic` commands for a true single-call use.
  (`datacopy`/`datamove` are the one deliberate exception — later renamed
  back to match `Catalog`'s own names, rather than staying `copy`/`move`;
  see `%catalog help`'s current vocabulary for what actually shipped.)
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
