#!/usr/bin/env python3
"""Posts each pushed commit's message as a note on the Redmine (taskman)
issue(s) it references.

A commit references an issue by using one of Redmine's own reference
keywords immediately before the issue number — e.g. "refs #1234" or
"fixes #1234" — deliberately NOT a bare "#1234": this repo's own merge-commit
messages already contain GitHub PR numbers as "#123", which would otherwise
misfire as Redmine updates.

This only posts a note. It never changes the issue's status/tracker/etc,
regardless of which keyword (referencing vs. closing) matched — that's a
deliberate scope limit, not a missing feature; see the workflow file for why.

Reads the push event payload from $GITHUB_EVENT_PATH (no `git log` needed —
the payload's own `commits[]` array already has id/message/url/author for
every commit in the push). Stdlib only, no pip install step required.

Per-author attribution: each commit is posted using the Redmine API key that
belongs to ITS author, not one shared key for everyone, so the note in
Redmine shows up as written by the actual committer. REDMINE_API_KEYS_JSON
(optional) is a JSON object mapping the commit author's git email (lowercase)
to that person's own Redmine API key, e.g.:
    {"alice@example.org": "abc123...", "bob@example.org": "def456..."}
An author not present in that map falls back to REDMINE_API_KEY (required) —
so this works immediately with just one shared key, and gains per-author
attribution incrementally as more people are added to the JSON map, with no
workflow-file changes needed per person.
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import defaultdict

# Redmine's own default reference keywords (Settings > Repositories, both the
# non-closing "refs"/"references" and the closing "fixes"/"closes"/"resolves"
# families) — kept deliberately as a fixed allowlist rather than a bare
# "#\d+" match; see module docstring.
KEYWORD_RE = re.compile(
    r"\b(?:refs?|references?|fix(?:es)?|clos(?:es?|ed)?|resolves?)\b",
    re.IGNORECASE,
)
LEADING_SEP_RE = re.compile(r"^[\s:]*")
# Between issue numbers in a list: ", ", " and ", " & " — covers "fixes #1,
# #2", "fixes #1 and #2", "fixes #1, #2 and #3", not just comma-lists.
LIST_SEP_RE = re.compile(r"^(?:\s*(?:,|and|&)\s*)+", re.IGNORECASE)
ISSUE_NUM_RE = re.compile(r"#(\d+)")


def extract_issue_ids(message: str) -> set[int]:
    ids: set[int] = set()
    for keyword_match in KEYWORD_RE.finditer(message):
        rest = message[keyword_match.end():]
        rest = rest[LEADING_SEP_RE.match(rest).end():]
        num_match = ISSUE_NUM_RE.match(rest)
        while num_match:
            ids.add(int(num_match.group(1)))
            rest = rest[num_match.end():]
            sep_match = LIST_SEP_RE.match(rest)
            if not sep_match:
                break
            rest = rest[sep_match.end():]
            num_match = ISSUE_NUM_RE.match(rest)
    return ids


def load_author_keys(raw_json: str) -> dict[str, str]:
    """Parses REDMINE_API_KEYS_JSON. Empty/unset -> no per-author overrides
    (everyone uses the default key). Malformed JSON is a warning, not a
    crash — the workflow should still post notes with the default key rather
    than fail the whole run over one typo in the mapping."""
    raw_json = (raw_json or "").strip()
    if not raw_json:
        return {}
    try:
        parsed = json.loads(raw_json)
        return {str(k).lower(): str(v) for k, v in parsed.items()}
    except (json.JSONDecodeError, AttributeError) as exc:
        print(f"REDMINE_API_KEYS_JSON is not valid JSON, ignoring it: {exc}", file=sys.stderr)
        return {}


def resolve_api_key(commit: dict, author_keys: dict[str, str], default_key: str) -> str:
    email = commit.get("author", {}).get("email", "").lower()
    return author_keys.get(email, default_key)


def build_note(repo: str, ref_name: str, commits: list[dict]) -> str:
    lines = [f'Commits pushed to "{repo}":https://github.com/{repo} (branch: {ref_name}):', ""]
    for c in commits:
        short_sha = c["id"][:12]
        author = c.get("author", {}).get("name", "unknown")
        first_line = c["message"].splitlines()[0]
        lines.append(f'* "{short_sha}":{c["url"]} by {author}: {first_line}')
        rest = c["message"].splitlines()[1:]
        if any(line.strip() for line in rest):
            lines.append("<pre>" + "\n".join(rest).strip() + "</pre>")
    return "\n".join(lines)


def post_note(redmine_url: str, api_key: str, issue_id: int, note: str) -> None:
    url = f"{redmine_url.rstrip('/')}/issues/{issue_id}.json"
    body = json.dumps({"issue": {"notes": note}}).encode()
    req = urllib.request.Request(
        url,
        data=body,
        method="PUT",
        headers={
            "Content-Type": "application/json",
            "X-Redmine-API-Key": api_key,
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()


def main() -> int:
    event_path = os.environ["GITHUB_EVENT_PATH"]
    redmine_url = os.environ["REDMINE_URL"]
    default_key = os.environ["REDMINE_API_KEY"]
    author_keys = load_author_keys(os.environ.get("REDMINE_API_KEYS_JSON", ""))
    repo = os.environ["REPO_FULL_NAME"]
    ref_name = os.environ["REF_NAME"]

    with open(event_path, encoding="utf-8") as f:
        event = json.load(f)

    commits = event.get("commits", [])
    if not commits:
        print("No commits in this push event — nothing to do.")
        return 0

    # Grouped by (issue, api_key) rather than just issue: two authors'
    # commits referencing the same issue in one push must post as TWO
    # separate notes, each under its own author's identity — merging them
    # into one note would mean posting under only one of the two keys.
    by_issue_and_key: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for commit in commits:
        resolved_key = resolve_api_key(commit, author_keys, default_key)
        for issue_id in extract_issue_ids(commit["message"]):
            by_issue_and_key[(issue_id, resolved_key)].append(commit)

    if not by_issue_and_key:
        print(f"No Redmine issue references found in {len(commits)} commit(s).")
        return 0

    failures: list[int] = []
    for (issue_id, resolved_key), issue_commits in by_issue_and_key.items():
        note = build_note(repo, ref_name, issue_commits)
        try:
            post_note(redmine_url, resolved_key, issue_id, note)
            print(f"Issue #{issue_id}: posted note for {len(issue_commits)} commit(s).")
        except urllib.error.HTTPError as exc:
            print(f"Issue #{issue_id}: FAILED ({exc.code} {exc.reason}) — {exc.read().decode(errors='replace')}", file=sys.stderr)
            failures.append(issue_id)
        except urllib.error.URLError as exc:
            print(f"Issue #{issue_id}: FAILED ({exc.reason})", file=sys.stderr)
            failures.append(issue_id)

    if failures:
        print(f"Failed to update {len(failures)} issue(s): {failures}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
