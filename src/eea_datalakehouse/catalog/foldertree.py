"""Move or copy a whole folder — sub-folders, tables and views — from one
place in the Dremio catalog to another.

    from eea_datalakehouse.catalog import Catalog, foldermove

    with Catalog(DREMIO_URL, DREMIO_TOKEN, username=DREMIO_USERNAME) as catalog:
        report = foldermove(catalog, "bwd.draft", "bwd.archive.2025", idempotency_key=KEY)
        print(report.summary())

`foldercopy` reproduces the source subtree under the target and proves the
copy landed. `foldermove` is `foldercopy` plus removal of the source — and it
removes **nothing** unless the copy verified clean (`report.verified`). That
order — copy, verify, then delete — is the whole point: a folder move across a
Community-Edition Dremio has no transaction behind it, so the only safe
sequencing is to leave the source standing until the target is proven.

What each kind of entry needs is different, and that is why this is more than
a loop over `datacopy`:

* **Folders** are reproduced first, shallowest first, so nothing lands in a
  folder that does not exist yet. Empty ones are included — they are invisible
  to `INFORMATION_SCHEMA` and only Dremio's REST catalog knows about them.
* **Tables** are copied with `datacopy` (``CREATE TABLE … AS SELECT``, over
  Arrow Flight) and then **counted on both sides**: ``COUNT(*)`` on the source
  and on the target, which have to agree. A table whose counts disagree fails
  the whole move.
* **Views** are *not* copied as data — a ``CREATE TABLE … AS SELECT`` over a
  view silently materialises it into a table, which is a different kind of
  thing from the original. Each view is recreated from its own SQL with every
  reference into the moved subtree **remapped to the new location** (see
  `remap_view_sql`), so the copied folder is self-contained and still points at
  its own tables rather than back at the ones about to be deleted. Views are
  created in dependency order, so a view selecting from another view in the
  same subtree is created after it.

Assumptions worth checking against your actual Dremio deployment before
relying on this — in the same spirit as `operations`' own list:

* **A view's SQL is read from Dremio's catalog entity where possible**
  (`getviewsql`), because that carries the ``sqlContext`` the SQL was written
  against as well as the text. Where that lookup is rejected — some source
  types answer a nested by-path lookup with a 400, see
  `CatalogRestClient.is_folder` — it falls back to
  ``INFORMATION_SCHEMA."VIEWS"``, which has the text but no context. With no
  context, an unqualified reference inside a view cannot be resolved and is
  left exactly as it was; `remap_view_sql` says so in its notes rather than
  guessing.
* **The rewrite is textual**, not a parse of Dremio's SQL dialect. It skips
  string literals and comments, and only rewrites identifier chains it can
  resolve to something it knows about (see `remap_view_sql` for the exact
  rules and the one corner it can get wrong). A dry run prints every rewritten
  definition — read them before turning `dry_run` off on a subtree of
  complicated views.
* **Nothing here is parallel.** `datacopy` is Flight-only and
  `FlightSqlExecutor` holds one client with a lazily-cached auth header whose
  initialisation is unguarded; this repo has not verified concurrent statement
  execution on one Flight client. N tables copy one after another.
* **Re-running is expected to be cheap.** With the default
  `skip_existing=True`, a target table that is already there and already has
  the source's row count is left alone and reported as ``skipped`` — so a run
  interrupted halfway (a cold engine, a dropped kernel) is resumed by simply
  running it again, rather than by unpicking what already made it.
* **Deletion is explicit, never a cascade.** The source is taken apart in the
  order it was built: views (reverse dependency order), then tables, then
  folders deepest-first. `deletefolder`'s own `cascade=True` is deliberately
  not used — a folder that will not delete because something unexpected is
  still inside it is a fact worth surfacing, not something to bulldoze.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from .errors import CatalogOperationError
from .operations import _quote_path

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .client import Catalog

EntryKind = Literal["FOLDER", "TABLE", "VIEW"]
EntryStatus = Literal["planned", "created", "copied", "skipped", "failed"]

ProgressCallback = Callable[[str], None]

# Identifier chain segment: a double-quoted identifier (with "" as an escaped
# quote inside it) or a bare one. Digits can't start a bare identifier, which
# is what keeps `1.5` from being read as a two-segment chain.
_SQL_IDENT = re.compile(r'"(?:[^"]|"")*"|[A-Za-z_][A-Za-z0-9_$]*')

# Keywords a dataset reference can follow. Only used to decide whether an
# unresolved chain is worth one existence check to see if it was written
# relative to a context this move is dropping — never to decide a rewrite that
# the chain itself already justifies.
_REFERENCE_KEYWORDS = frozenset({"FROM", "JOIN", "INTO", "TABLE", "UPDATE"})

# Enough of Dremio's SQL vocabulary to keep the "unqualified reference"
# warning below from firing on every keyword in an ordinary SELECT. It only
# gates a note; nothing is ever rewritten on the strength of this list.
_SQL_WORDS = frozenset({
    "ALL", "AND", "AS", "ASC", "AVG", "BETWEEN", "BY", "CASE", "CAST", "COALESCE", "COUNT",
    "CROSS", "CURRENT_DATE", "CURRENT_TIMESTAMP", "DESC", "DISTINCT", "ELSE", "END", "ESCAPE",
    "EXISTS", "EXTRACT", "FALSE", "FROM", "FULL", "GROUP", "HAVING", "IN", "INNER", "INTERVAL",
    "INTO", "IS", "JOIN", "LEFT", "LIKE", "LIMIT", "MAX", "MIN", "NATURAL", "NOT", "NULL",
    "NULLIF", "OFFSET", "ON", "OR", "ORDER", "OUTER", "OVER", "PARTITION", "RIGHT", "ROW",
    "ROWS", "SELECT", "SET", "SUBSTRING", "SUM", "TABLE", "THEN", "TRIM", "TRUE", "UNION",
    "UPDATE", "USING", "VALUES", "WHEN", "WHERE", "WITH",
})


@dataclass(frozen=True, slots=True)
class TreeEntry:
    """One thing to reproduce at the target: a folder, a table or a view."""

    source_path: str
    target_path: str
    kind: EntryKind
    #: Views only — the SQL Dremio holds for the view, exactly as stored.
    view_sql: str | None = None
    #: Views only — the ``sqlContext`` that SQL was written against, if known.
    view_context: tuple[str, ...] = ()
    #: Views only — the rewritten SQL that will be used at the target.
    remapped_sql: str | None = None
    #: Views only — source paths inside this subtree the view selects from.
    depends_on: tuple[str, ...] = ()
    #: Anything the caller should read before trusting this entry.
    notes: tuple[str, ...] = ()

    @property
    def name(self) -> str:
        return self.source_path.rsplit(".", 1)[-1]


@dataclass(frozen=True, slots=True)
class FolderTree:
    """What `folderscan` found under a source folder, already mapped to targets.

    `views` is in **dependency order**: a view that selects from another view
    in this same subtree comes after it, so creating them in list order works.
    `folders` is shallowest first, for the same reason.
    """

    source_path: str
    target_path: str
    folders: tuple[TreeEntry, ...] = ()
    tables: tuple[TreeEntry, ...] = ()
    views: tuple[TreeEntry, ...] = ()
    notes: tuple[str, ...] = ()

    @property
    def entries(self) -> tuple[TreeEntry, ...]:
        """Every entry, in the order a copy has to create them."""
        return self.folders + self.tables + self.views

    def __len__(self) -> int:
        return len(self.entries)


@dataclass(slots=True)
class EntryResult:
    """What actually happened to one `TreeEntry`."""

    entry: TreeEntry
    status: EntryStatus = "planned"
    source_rows: int | None = None
    target_rows: int | None = None
    sql: str | None = None
    error: str | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def counts_match(self) -> bool:
        """Both sides counted, and they agree. False while either is unknown."""
        return (
            self.source_rows is not None
            and self.target_rows is not None
            and self.source_rows == self.target_rows
        )


@dataclass(slots=True)
class FolderTransferReport:
    """The outcome of a `foldercopy` / `foldermove`, entry by entry.

    `verified` is the green light: every folder reproduced, every table copied
    *and* counted equal on both sides, every view created, and nothing failed.
    `foldermove` deletes the source only when it is true.
    """

    source_path: str
    target_path: str
    dry_run: bool = False
    folders: list[EntryResult] = field(default_factory=list)
    tables: list[EntryResult] = field(default_factory=list)
    views: list[EntryResult] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    delete_errors: list[tuple[str, str]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    source_deleted: bool = False

    @property
    def results(self) -> list[EntryResult]:
        return self.folders + self.tables + self.views

    @property
    def failures(self) -> list[EntryResult]:
        return [result for result in self.results if result.status == "failed"]

    @property
    def verified(self) -> bool:
        """Whether the target is a proven-good copy of the source.

        A dry run is never verified — nothing was written, so there is nothing
        to have proven. Tables must have matching counts on both sides; views
        must have been created or deliberately skipped, and have matching
        counts too wherever they were checked (`verify_views`).
        """
        if self.dry_run or not self.results:
            return False
        if self.failures:
            return False
        if any(result.status == "planned" for result in self.results):
            return False
        for result in self.tables:
            if not result.counts_match:
                return False
        for result in self.views:
            if result.source_rows is not None and not result.counts_match:
                return False
        return True

    def summary(self) -> str:
        """A readable report — what moved, what it counted, what went wrong."""
        head = "PLAN" if self.dry_run else "RESULT"
        lines: list[str] = [f"{head}: {self.source_path} -> {self.target_path}"]
        for label, results in (
            ("folders", self.folders),
            ("tables", self.tables),
            ("views", self.views),
        ):
            if not results:
                continue
            lines.append(f"\n{label} ({len(results)}):")
            for result in results:
                mark = {"failed": "x", "planned": "-", "skipped": "="}.get(result.status, "+")
                counts = ""
                if result.source_rows is not None or result.target_rows is not None:
                    src = "?" if result.source_rows is None else f"{result.source_rows:,}"
                    tgt = "?" if result.target_rows is None else f"{result.target_rows:,}"
                    # "->" while a side is still unknown (a plan, say) — only a
                    # count that was actually taken on both sides can disagree.
                    both_known = result.source_rows is not None and result.target_rows is not None
                    agree = "==" if result.counts_match else ("!=" if both_known else "->")
                    counts = f"   [{src} {agree} {tgt} rows]"
                lines.append(
                    f"  {mark} {result.entry.source_path} -> {result.entry.target_path}"
                    f"   ({result.status}){counts}"
                )
                lines.extend(f"      note: {note}" for note in result.notes)
                if result.error:
                    lines.append(f"      error: {result.error}")
        if self.deleted:
            lines.append(f"\nremoved from the source ({len(self.deleted)}):")
            lines.extend(f"  - {path}" for path in self.deleted)
        if self.delete_errors:
            lines.append("\ncould not remove:")
            lines.extend(f"  x {path}: {error}" for path, error in self.delete_errors)
        if self.notes:
            lines.append("\nnotes:")
            lines.extend(f"  {note}" for note in self.notes)
        tail = f"\nverified: {self.verified}"
        if self.dry_run:
            tail += "   (dry run — nothing was written)"
        else:
            tail += f"   source removed: {self.source_deleted}"
        lines.append(tail)
        return "\n".join(lines)


# --------------------------------------------------------------------------
# paths
# --------------------------------------------------------------------------


def _segments(path: str) -> list[str]:
    return [segment for segment in path.split(".") if segment]


def _lower(segments: list[str] | tuple[str, ...]) -> list[str]:
    return [segment.lower() for segment in segments]


def _remap_path(path: str, source_path: str, target_path: str) -> str:
    """`path`, with its `source_path` prefix swapped for `target_path`."""
    remainder = _segments(path)[len(_segments(source_path)) :]
    return ".".join(_segments(target_path) + remainder)


def _is_inside(path: str, ancestor: str) -> bool:
    """Whether `path` is `ancestor` itself, or something under it (case-insensitively)."""
    path_segments = _lower(_segments(path))
    ancestor_segments = _lower(_segments(ancestor))
    return path_segments[: len(ancestor_segments)] == ancestor_segments


def _unquote_ident(raw: str) -> str:
    if raw.startswith('"') and raw.endswith('"') and len(raw) >= 2:
        return raw[1:-1].replace('""', '"')
    return raw


# --------------------------------------------------------------------------
# view SQL rewriting
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Token:
    """One piece of a SQL string: passthrough text, or an identifier chain."""

    kind: Literal["text", "chain"]
    raw: str
    segments: tuple[str, ...] = ()


def _iter_sql_chains(sql: str) -> Iterator[_Token]:
    """Walk `sql`, yielding a `_Token` per piece: text, or an identifier chain.

    String literals (``'…'``, with ``''`` as an escaped quote), line comments
    (``-- …``) and block comments (``/* … */``) are passed through as text and
    never looked inside — the one hard rule that keeps a path-shaped string
    from being rewritten as if it were a reference.

    A *chain* is one or more identifiers joined by dots with no whitespace
    between them: ``a.b``, ``"my space"."tbl"``, ``t.col``. Whitespace around
    a dot ends the chain, which is a deliberate simplification — Dremio's own
    stored view SQL does not write references that way.
    """
    index, length = 0, len(sql)
    buffer: list[str] = []
    while index < length:
        char = sql[index]
        if char == "'":
            cursor = index + 1
            while cursor < length:
                if sql[cursor] == "'":
                    if cursor + 1 < length and sql[cursor + 1] == "'":
                        cursor += 2
                        continue
                    cursor += 1
                    break
                cursor += 1
            buffer.append(sql[index:cursor])
            index = cursor
            continue
        if sql.startswith("--", index):
            cursor = sql.find("\n", index)
            cursor = length if cursor == -1 else cursor
            buffer.append(sql[index:cursor])
            index = cursor
            continue
        if sql.startswith("/*", index):
            cursor = sql.find("*/", index + 2)
            cursor = length if cursor == -1 else cursor + 2
            buffer.append(sql[index:cursor])
            index = cursor
            continue
        match = _SQL_IDENT.match(sql, index)
        if match is None:
            buffer.append(char)
            index += 1
            continue

        segments: list[str] = []
        cursor = index
        while True:
            match = _SQL_IDENT.match(sql, cursor)
            if match is None:
                break
            segments.append(_unquote_ident(match.group(0)))
            cursor = match.end()
            if cursor < length and sql[cursor] == "." and _SQL_IDENT.match(sql, cursor + 1):
                cursor += 1
                continue
            break
        if buffer:
            yield _Token("text", "".join(buffer))
            buffer = []
        yield _Token("chain", sql[index:cursor], tuple(segments))
        index = cursor
    if buffer:
        yield _Token("text", "".join(buffer))


@dataclass(frozen=True, slots=True)
class RemapResult:
    """`remap_view_sql`'s answer: the new SQL, and what it had to touch."""

    sql: str
    #: Source paths inside the moved subtree this SQL selects from.
    references: tuple[str, ...] = ()
    #: Anything the caller should read before trusting the rewrite.
    notes: tuple[str, ...] = ()


def remap_view_sql(
    sql: str,
    *,
    source_path: str,
    target_path: str,
    entries: dict[str, str],
    context: tuple[str, ...] = (),
    external_exists: Callable[[str], bool] | None = None,
) -> RemapResult:
    """Rewrite a view's SQL so it points at the copied subtree, not the original.

    `entries` maps a **lower-cased** source path to its target path — every
    table and view being moved. `context` is the view's own ``sqlContext``.

    Each identifier chain outside a string literal or comment is resolved, in
    this order, and the first rule that answers wins:

    1. **A known entry.** The longest prefix of the chain that is one of
       `entries` becomes that entry's target path — so ``bwd.draft.t.col``
       rewrites the ``bwd.draft.t`` part and leaves ``.col`` alone.
    2. **Anything else under the source root.** A chain starting with
       `source_path` has that prefix swapped for `target_path`. This is the
       safety net for something the scan did not enumerate (a table created
       between the scan and the rewrite).
    3. **Context-relative, into the subtree.** With a `context`, the chain is
       retried as ``context + chain``; if that resolves under rule 1 or 2, the
       chain is replaced with the **fully qualified** target path. This is
       what keeps a view that says ``FROM sales`` working: after the move it
       no longer has that context, so the reference is spelled out.
    4. **Context-relative, out of the subtree.** A chain that follows
       ``FROM``/``JOIN``/``INTO``/``TABLE``/``UPDATE`` and still has not
       resolved is checked once against `external_exists` as
       ``context + chain``; if it really is there, it is spelled out in full
       so it survives losing the context. Skipped entirely without an
       `external_exists`.

    Anything else is left exactly as written.

    **The corner it can get wrong.** Rule 2 is prefix matching, so if
    `source_path` is a single segment (a whole space) and the SQL aliases a
    table with that same name, ``alias.column`` looks like a path into the
    source. Moving a whole space is not the normal case, and moving a folder
    (two segments or more) cannot hit it — but it is why a dry run prints
    every rewritten definition.
    """
    source_segments = _segments(source_path)
    target_segments = _segments(target_path)
    source_lower = _lower(source_segments)
    context_segments = list(context)

    references: set[str] = set()
    notes: list[str] = []
    probed: dict[str, bool] = {}

    def resolve_known(
        segments: list[str], *, allow_root_prefix: bool = True
    ) -> tuple[int, list[str]] | None:
        """(segments consumed, replacement) under rules 1 and 2, or None.

        `allow_root_prefix=False` drops rule 2, leaving only an exact match
        against a known entry. Rule 3 needs that: prepending a context that is
        itself inside the source tree makes *every* chain — every keyword
        included — start with the source root, so the fallback would rewrite
        `SELECT` as eagerly as a real reference. The enumeration is complete
        for the subtree, so an exact match is all a context-relative
        reference into it ever needs.
        """
        for count in range(len(segments), 0, -1):
            key = ".".join(_lower(segments[:count]))
            if key in entries:
                references.add(key)
                return count, _segments(entries[key])
        if not allow_root_prefix:
            return None
        lowered = _lower(segments)
        if len(lowered) >= len(source_lower) and lowered[: len(source_lower)] == source_lower:
            return len(segments), target_segments + segments[len(source_segments) :]
        return None

    out: list[str] = []
    previous_keyword = ""
    for token in _iter_sql_chains(sql):
        if token.kind == "text":
            out.append(token.raw)
            stripped = token.raw.strip()
            if stripped and stripped[-1] == ",":
                previous_keyword = ","
            continue

        segments = list(token.segments)
        replacement: list[str] | None = None
        consumed = len(segments)

        resolved = resolve_known(segments)
        if resolved is not None:
            consumed, replacement = resolved
        elif context_segments:
            relative = resolve_known(context_segments + segments, allow_root_prefix=False)
            if relative is not None:
                # The match covered the context as well, so only what is left
                # over of the chain itself counts as consumed — `sales.region`
                # against a context that resolves `sales` keeps `.region`.
                consumed = max(relative[0] - len(context_segments), 1)
                replacement = relative[1]
            elif external_exists is not None and previous_keyword in _REFERENCE_KEYWORDS:
                candidate = ".".join(context_segments + segments)
                if candidate not in probed:
                    probed[candidate] = external_exists(candidate)
                if probed[candidate]:
                    consumed, replacement = len(segments), context_segments + segments
                    notes.append(
                        f"{'.'.join(segments)!r} was relative to the view's context — "
                        f"written out in full as {candidate!r}"
                    )

        if replacement is None:
            out.append(token.raw)
        else:
            out.append(_quote_path(".".join(replacement)))
            # Whatever of the chain the rule did not consume (a column, say)
            # is appended back unchanged.
            if consumed < len(segments):
                out.append("." + ".".join(f'"{segment}"' for segment in segments[consumed:]))

        previous_keyword = segments[0].upper() if len(segments) == 1 else ""

    if not context_segments:
        unqualified = [
            token
            for token in _iter_sql_chains(sql)
            if token.kind == "chain"
            and len(token.segments) == 1
            and token.segments[0].upper() not in _SQL_WORDS
        ]
        if unqualified:
            notes.append(
                "the view's sqlContext is unknown, so any unqualified reference in its "
                "SQL was left as written — check it resolves from the new location"
            )

    return RemapResult(sql="".join(out), references=tuple(sorted(references)), notes=tuple(notes))


# --------------------------------------------------------------------------
# scanning
# --------------------------------------------------------------------------


def _key(idempotency_key: str, operation: str, path: str) -> str:
    """One idempotency key per entry, derived from the caller's own.

    A folder move is N operations, and `retry_state` remembers one attempt per
    key — so sharing a single key across them would leave only the last stall
    remembered. Deriving ``"<key>:<operation>:<path>"`` keeps every step
    separately resumable with `catalog.retry_pending(...)`, and keeps a re-run
    of the same job landing on the same keys.
    """
    return f"{idempotency_key}:{operation}:{path}"


def _emit(on_progress: ProgressCallback | None, message: str) -> None:
    if on_progress is not None:
        on_progress(message)


def _order_views(views: list[TreeEntry]) -> list[TreeEntry]:
    """Views sorted so a view comes after every view in this subtree it selects
    from. A dependency cycle (which Dremio would not have let you create) is
    left in scan order rather than raising."""
    by_path = {entry.source_path.lower(): entry for entry in views}
    state: dict[str, str] = {}
    ordered: list[TreeEntry] = []

    def visit(entry: TreeEntry) -> None:
        key = entry.source_path.lower()
        if state.get(key) in ("done", "visiting"):
            return
        state[key] = "visiting"
        for dependency in entry.depends_on:
            other = by_path.get(dependency.lower())
            if other is not None and other.source_path != entry.source_path:
                visit(other)
        state[key] = "done"
        ordered.append(entry)

    for entry in views:
        visit(entry)
    return ordered


def folderscan(
    catalog: Catalog,
    source_path: str,
    target_path: str,
    *,
    idempotency_key: str,
    qualify_external: bool = True,
    on_progress: ProgressCallback | None = None,
) -> FolderTree:
    """Everything under `source_path`, mapped to where it would land under
    `target_path`. Reads only — nothing is created, changed or deleted.

    Views come back with their SQL already rewritten (`TreeEntry.remapped_sql`)
    and in dependency order, so the plan a dry run prints is exactly what an
    apply would run.

    Raises `CatalogOperationError` if the two paths are the same, if either is
    inside the other (a move into your own subtree has no fixed point), or if
    `source_path` cannot be read at all.
    """
    if not _segments(source_path) or not _segments(target_path):
        raise CatalogOperationError("both a source and a target path are required")
    if _lower(_segments(source_path)) == _lower(_segments(target_path)):
        raise CatalogOperationError(f"source and target are the same folder: {source_path!r}")
    if _is_inside(target_path, source_path):
        raise CatalogOperationError(
            f"target {target_path!r} is inside source {source_path!r} — "
            "a folder cannot be moved into itself"
        )
    if _is_inside(source_path, target_path):
        raise CatalogOperationError(
            f"source {source_path!r} is inside target {target_path!r} — "
            "that would copy a folder onto its own parent"
        )

    notes: list[str] = []

    _emit(on_progress, f"scanning {source_path} …")
    entries = catalog.getentriesfrom(
        source_path, idempotency_key=_key(idempotency_key, "getentriesfrom", source_path)
    )

    folder_paths: list[str] = []
    try:
        folder_paths = catalog.getfoldersfrom(
            source_path, idempotency_key=_key(idempotency_key, "getfoldersfrom", source_path)
        )
    except CatalogOperationError as exc:
        if not entries:
            raise CatalogOperationError(f"could not read {source_path!r}: {exc}") from exc
        notes.append(
            f"folders could not be listed over the REST catalog ({exc}) — the tree was "
            "rebuilt from the paths of the tables and views found, so any empty folder "
            "under the source will not be reproduced"
        )

    # Every folder an entry sits in has to exist at the target whether or not
    # the REST listing found it, so derive those too and merge.
    derived: set[str] = set()
    source_depth = len(_segments(source_path))
    for entry in entries:
        segments = _segments(entry.path)
        for depth in range(source_depth + 1, len(segments)):
            derived.add(".".join(segments[:depth]))
    all_folders = sorted(set(folder_paths) | derived, key=lambda path: (path.count("."), path))

    folders = tuple(
        TreeEntry(
            source_path=path,
            target_path=_remap_path(path, source_path, target_path),
            kind="FOLDER",
        )
        for path in all_folders
    )
    tables = tuple(
        TreeEntry(
            source_path=entry.path,
            target_path=_remap_path(entry.path, source_path, target_path),
            kind="TABLE",
        )
        for entry in entries
        if entry.kind == "TABLE"
    )

    # The whole map has to be known before any view can be rewritten — a view
    # may select from a table listed after it.
    path_map = {entry.source_path.lower(): entry.target_path for entry in (*tables, *folders)}
    canonical = {entry.source_path.lower(): entry.source_path for entry in (*tables, *folders)}
    view_entries = [entry for entry in entries if entry.kind == "VIEW"]
    for entry in view_entries:
        path_map[entry.path.lower()] = _remap_path(entry.path, source_path, target_path)
        canonical[entry.path.lower()] = entry.path

    def external_exists(path: str) -> bool:
        return catalog.entryexists(path, idempotency_key=_key(idempotency_key, "entryexists", path))

    views: list[TreeEntry] = []
    for entry in view_entries:
        entry_target = _remap_path(entry.path, source_path, target_path)
        entry_notes: list[str] = []
        view_sql: str | None = None
        view_context: tuple[str, ...] = ()
        remapped: str | None = None
        references: tuple[str, ...] = ()
        try:
            definition = catalog.getviewsql(
                entry.path, idempotency_key=_key(idempotency_key, "getviewsql", entry.path)
            )
            view_sql, view_context = definition.sql, definition.context
            result = remap_view_sql(
                view_sql,
                source_path=source_path,
                target_path=target_path,
                entries=path_map,
                context=view_context,
                external_exists=external_exists if qualify_external else None,
            )
            remapped = result.sql
            references = tuple(canonical.get(ref, ref) for ref in result.references)
            entry_notes.extend(result.notes)
        except CatalogOperationError as exc:
            entry_notes.append(f"could not read this view's definition: {exc}")
        views.append(
            TreeEntry(
                source_path=entry.path,
                target_path=entry_target,
                kind="VIEW",
                view_sql=view_sql,
                view_context=view_context,
                remapped_sql=remapped,
                depends_on=references,
                notes=tuple(entry_notes),
            )
        )

    tree = FolderTree(
        source_path=source_path,
        target_path=target_path,
        folders=folders,
        tables=tables,
        views=tuple(_order_views(views)),
        notes=tuple(notes),
    )
    _emit(
        on_progress,
        f"found {len(tree.folders)} folder(s), {len(tree.tables)} table(s), "
        f"{len(tree.views)} view(s) under {source_path}",
    )
    return tree


# --------------------------------------------------------------------------
# copying
# --------------------------------------------------------------------------


def _row_count(catalog: Catalog, path: str, idempotency_key: str) -> int | None:
    """``COUNT(*)`` for `path`, or None if it is not there at all."""
    try:
        return catalog.gettableitemsfrom(
            path, idempotency_key=_key(idempotency_key, "gettableitemsfrom", path)
        ).row_count
    except CatalogOperationError:
        return None


def _carry_metadata(catalog: Catalog, entry: TreeEntry, idempotency_key: str) -> list[str]:
    """Copy the wiki (and, for tables/views, Dremio's tags) across.

    Best-effort by design: metadata is descriptive, and losing a wiki is not a
    reason to fail a move that otherwise verified. Whatever went wrong comes
    back as notes on the entry instead.

    Not covered: the DDS ``meta.json`` sidecar the JupyterLab extension shows
    under *⚙ Domain*. That lives in the Document Service, not in Dremio, and
    is a different system's copy to make.
    """
    problems: list[str] = []
    try:
        wiki = catalog.getwikifrom(
            entry.source_path,
            idempotency_key=_key(idempotency_key, "getwikifrom", entry.source_path),
        )
    except CatalogOperationError:
        wiki = ""  # no wiki, or nothing readable — normal, nothing to carry
    if wiki:
        try:
            catalog.setwikito(
                entry.target_path,
                wiki,
                idempotency_key=_key(idempotency_key, "setwikito", entry.target_path),
            )
        except CatalogOperationError as exc:
            problems.append(f"wiki not carried over: {exc}")

    if entry.kind == "FOLDER":
        return problems  # Dremio's tags exist only on tables and views
    try:
        tags = catalog.gettagsfrom(
            entry.source_path,
            idempotency_key=_key(idempotency_key, "gettagsfrom", entry.source_path),
        )
    except CatalogOperationError:
        return problems
    if tags:
        try:
            catalog.settagsto(
                entry.target_path,
                tags,
                idempotency_key=_key(idempotency_key, "settagsto", entry.target_path),
            )
        except CatalogOperationError as exc:
            problems.append(f"tags not carried over: {exc}")
    return problems


def foldercopy(
    catalog: Catalog,
    source_path: str,
    target_path: str,
    *,
    idempotency_key: str,
    dry_run: bool = False,
    overwrite: bool = False,
    skip_existing: bool = True,
    verify_views: bool = True,
    copy_metadata: bool = True,
    qualify_external: bool = True,
    tree: FolderTree | None = None,
    on_progress: ProgressCallback | None = None,
) -> FolderTransferReport:
    """Reproduce the whole `source_path` subtree under `target_path`, and prove it.

    The source is left completely untouched — this is the copy half of
    `foldermove`, and useful on its own for taking a snapshot of a folder.

    * `dry_run=True` writes nothing and returns the plan, with each table's
      source row count filled in so you can see what a real run would move.
    * `overwrite=True` replaces whatever is already at the target, per entry.
    * `skip_existing=True` (the default) leaves a target table that is already
      there **and already has the source's row count** alone, reporting it as
      ``skipped`` — which is what makes an interrupted run resumable by just
      running it again. A target that exists with a *different* count fails
      the entry rather than being silently accepted, and so does any occupied
      target with `skip_existing=False`: without `overwrite=True`, nothing
      already at the target is ever destroyed.
    * `verify_views=True` (the default) also counts each copied view on both
      sides. That is the only real proof a rewritten definition resolves to
      the same data, so leave it on unless the views are expensive to run.
    * `copy_metadata=True` carries each entry's wiki and tags across
      (best-effort — see `_carry_metadata`).
    * `tree=` skips the scan and applies a `FolderTree` you already have —
      the "look at the plan, then run it" flow, without paying for a second
      scan or risking a plan that differs from the one you read.

    Returns a `FolderTransferReport`. A per-entry failure does not raise: the
    whole point is to finish the sweep and report, so one bad table does not
    hide the state of the other forty.
    """
    if tree is None:
        tree = folderscan(
            catalog,
            source_path,
            target_path,
            idempotency_key=idempotency_key,
            qualify_external=qualify_external,
            on_progress=on_progress,
        )

    report = FolderTransferReport(
        source_path=tree.source_path,
        target_path=tree.target_path,
        dry_run=dry_run,
        notes=list(tree.notes),
    )
    report.folders = [EntryResult(entry=entry) for entry in tree.folders]
    report.tables = [EntryResult(entry=entry) for entry in tree.tables]
    report.views = [
        EntryResult(entry=entry, sql=entry.remapped_sql, notes=list(entry.notes))
        for entry in tree.views
    ]

    if dry_run:
        for result in report.tables:
            result.source_rows = _row_count(catalog, result.entry.source_path, idempotency_key)
        for result in report.views:
            if result.entry.remapped_sql is None:
                result.status = "failed"
                result.error = "no usable view definition — see the notes on this entry"
        return report

    for result in report.folders:
        entry = result.entry
        try:
            catalog.createfolder(
                entry.target_path,
                create_parents=True,
                idempotency_key=_key(idempotency_key, "createfolder", entry.target_path),
            )
            result.status = "created"
            _emit(on_progress, f"folder  {entry.target_path}")
        except CatalogOperationError as exc:
            result.status = "failed"
            result.error = str(exc)
            _emit(on_progress, f"FAILED folder {entry.target_path}: {exc}")

    for result in report.tables:
        entry = result.entry
        try:
            result.source_rows = _row_count(catalog, entry.source_path, idempotency_key)
            existing_rows = _row_count(catalog, entry.target_path, idempotency_key)
            # Something is already at the target. `overwrite=False` never
            # destroys it — not even to "finish" the copy — so the only two
            # ways past here are accepting it (skip_existing, and only when
            # the counts already agree) or being told to replace it.
            if existing_rows is not None and not overwrite:
                result.target_rows = existing_rows
                if skip_existing and existing_rows == result.source_rows:
                    result.status = "skipped"
                    _emit(on_progress, f"table   {entry.target_path} (there already, counts agree)")
                    continue
                result.status = "failed"
                result.error = (
                    f"{entry.target_path!r} already exists with {existing_rows} row(s) and the "
                    f"source has {result.source_rows} — pass overwrite=True to replace it"
                )
                _emit(on_progress, f"FAILED table {entry.target_path}: {result.error}")
                continue
            catalog.datacopy(
                entry.source_path,
                entry.target_path,
                overwrite=overwrite,
                create_target_folder=True,
                idempotency_key=_key(idempotency_key, "datacopy", entry.target_path),
            )
            result.target_rows = _row_count(catalog, entry.target_path, idempotency_key)
            result.status = "copied"
            _emit(
                on_progress,
                f"table   {entry.target_path} "
                f"({result.source_rows} -> {result.target_rows} rows)",
            )
        except CatalogOperationError as exc:
            result.status = "failed"
            result.error = str(exc)
            _emit(on_progress, f"FAILED table {entry.target_path}: {exc}")

    for result in report.views:
        entry = result.entry
        if entry.remapped_sql is None:
            result.status = "failed"
            result.error = "no usable view definition — see the notes on this entry"
            _emit(on_progress, f"FAILED view {entry.target_path}: {result.error}")
            continue
        try:
            already_there = catalog.entryexists(
                entry.target_path,
                idempotency_key=_key(idempotency_key, "entryexists", entry.target_path),
            )
            if already_there and skip_existing and not overwrite:
                result.status = "skipped"
                _emit(on_progress, f"view    {entry.target_path} (there already)")
            else:
                # `overwrite` is passed through as given, so an occupied target
                # with overwrite=False fails here rather than being replaced.
                catalog.createviewas(
                    entry.target_path,
                    entry.remapped_sql,
                    overwrite=overwrite,
                    create_target_folder=True,
                    idempotency_key=_key(idempotency_key, "createviewas", entry.target_path),
                )
                result.status = "created"
                _emit(on_progress, f"view    {entry.target_path}")
        except CatalogOperationError as exc:
            result.status = "failed"
            result.error = str(exc)
            _emit(on_progress, f"FAILED view {entry.target_path}: {exc}")

    if verify_views:
        for result in report.views:
            if result.status == "failed":
                continue
            try:
                result.source_rows = _row_count(catalog, result.entry.source_path, idempotency_key)
                result.target_rows = _row_count(catalog, result.entry.target_path, idempotency_key)
            except CatalogOperationError as exc:
                result.status = "failed"
                result.error = f"the copied view would not run: {exc}"
                continue
            if not result.counts_match:
                result.status = "failed"
                result.error = (
                    f"row counts disagree — source {result.source_rows}, "
                    f"target {result.target_rows}; the rewritten definition is not equivalent"
                )
                _emit(on_progress, f"FAILED view {result.entry.target_path}: {result.error}")

    if copy_metadata:
        for result in report.results:
            if result.status in ("failed", "planned"):
                continue
            result.notes.extend(_carry_metadata(catalog, result.entry, idempotency_key))

    return report


# --------------------------------------------------------------------------
# moving
# --------------------------------------------------------------------------


def foldermove(
    catalog: Catalog,
    source_path: str,
    target_path: str,
    *,
    idempotency_key: str,
    dry_run: bool = False,
    overwrite: bool = False,
    skip_existing: bool = True,
    verify_views: bool = True,
    copy_metadata: bool = True,
    qualify_external: bool = True,
    delete_source_root: bool = True,
    tree: FolderTree | None = None,
    on_progress: ProgressCallback | None = None,
) -> FolderTransferReport:
    """Move the whole `source_path` subtree to `target_path`.

    `foldercopy` first — folders, tables (row-counted on both sides), views
    (rewritten to point at the copies) — and then, **only if
    `report.verified` came back true**, the source is taken apart: views in
    reverse dependency order, then tables, then folders deepest-first, then
    `source_path` itself unless `delete_source_root=False`.

    Anything less than a clean copy leaves the source exactly where it is, and
    says so in `report.notes`. Nothing is ever removed on a `dry_run`.

    All of `foldercopy`'s keyword arguments mean the same thing here.
    """
    report = foldercopy(
        catalog,
        source_path,
        target_path,
        idempotency_key=idempotency_key,
        dry_run=dry_run,
        overwrite=overwrite,
        skip_existing=skip_existing,
        verify_views=verify_views,
        copy_metadata=copy_metadata,
        qualify_external=qualify_external,
        tree=tree,
        on_progress=on_progress,
    )

    if dry_run:
        report.notes.append(
            "dry run — the source would be removed only after the copy above verified clean"
        )
        return report

    if not report.verified:
        plural = "y" if len(report.failures) == 1 else "ies"
        report.notes.append(
            f"the copy did not verify ({len(report.failures)} failed entr{plural}) — "
            f"{source_path!r} was left exactly as it was. Fix what failed and run this "
            "again; entries that already landed are skipped."
        )
        _emit(on_progress, f"source {source_path} left untouched — the copy did not verify")
        return report

    _emit(on_progress, f"copy verified — removing {source_path}")

    def remove(path: str, delete: Callable[[str], object]) -> None:
        try:
            delete(path)
            report.deleted.append(path)
            _emit(on_progress, f"removed {path}")
        except CatalogOperationError as exc:
            report.delete_errors.append((path, str(exc)))
            _emit(on_progress, f"FAILED to remove {path}: {exc}")

    def drop_view(path: str) -> None:
        catalog.deleteview(path, idempotency_key=_key(idempotency_key, "deleteview", path))

    def drop_table(path: str) -> None:
        catalog.deletetable(path, idempotency_key=_key(idempotency_key, "deletetable", path))

    def drop_folder(path: str) -> None:
        catalog.deletefolder(path, idempotency_key=_key(idempotency_key, "deletefolder", path))

    for result in reversed(report.views):
        remove(result.entry.source_path, drop_view)
    for result in report.tables:
        remove(result.entry.source_path, drop_table)
    # Deepest first: a folder has to be empty before it will delete, and
    # cascade is deliberately not used here — see the module docstring.
    for result in sorted(
        report.folders, key=lambda item: item.entry.source_path.count("."), reverse=True
    ):
        remove(result.entry.source_path, drop_folder)
    if delete_source_root:
        remove(source_path, drop_folder)

    report.source_deleted = not report.delete_errors
    if report.delete_errors:
        report.notes.append(
            "the copy verified and the target is complete, but part of the source could not "
            "be removed (listed above) — it is safe to re-run this, or to delete what is "
            "left by hand"
        )
    return report
