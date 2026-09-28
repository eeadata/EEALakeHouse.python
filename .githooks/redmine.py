#!/usr/bin/env python3
"""Posts each pushed commit's message as a note on the Redmine (taskman)
issue(s) it references — from the developer's own machine, with the
developer's own Redmine API key, which never leaves that machine.

Three entry points, one per caller:

  commit-msg <msg-file>     (git hook) Tags a commit that references an issue
                            with a "Redmine-Hook: v1" trailer — but only when
                            an API key is configured, so the trailer means
                            "this author's push will post the note".
  pre-push <remote> <url>   (git hook) Posts a note for each commit being
                            pushed that references an issue. Never blocks the
                            push: a failure is a warning.
  check                     (CI) Fails for any commit that references an issue
                            but lacks the trailer — i.e. its author doesn't
                            have the hooks installed, or has no key configured.

A commit references an issue by using one of Redmine's own reference
keywords immediately before the issue number — e.g. "refs #1234" or
"fixes #1234" — deliberately NOT a bare "#1234": this repo's own merge-commit
messages already contain GitHub PR numbers as "#123", which would otherwise
misfire as Redmine updates.

This only posts a note. It never changes the issue's status/tracker/etc,
regardless of which keyword (referencing vs. closing) matched.

API key lookup, first match wins: $REDMINE_API_KEY, then REDMINE_API_KEY= in
<repo>/.env, then in <repo>/debugger/.env (both gitignored). REDMINE_URL is
looked up the same way, defaulting to taskman.

Stdlib only, so the hooks run under any python3 without the project's venv.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

DEFAULT_REDMINE_URL = "https://taskman.eionet.europa.eu"
TRAILER_KEY = "Redmine-Hook"
TRAILER = f"{TRAILER_KEY}: v1"
TRAILER_RE = re.compile(rf"^{TRAILER_KEY}:.*$", re.MULTILINE)
ENV_FILES = (".env", "debugger/.env")
ZERO_SHA = "0" * 40
# Commits GitHub itself creates (web-UI edits, squash/rebase merges) can't
# have run anyone's local hooks, so the CI check doesn't hold them to it.
GITHUB_COMMITTER_EMAIL = "noreply@github.com"

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
GITHUB_REMOTE_RE = re.compile(r"github\.com[:/](?P<repo>[^/]+/[^/]+?)(?:\.git)?/?$")

SETUP_HINT = (
    "Run `git config core.hooksPath .githooks` once in your clone, and put\n"
    "REDMINE_API_KEY=<your key> in .env (taskman > My account > API access key)."
)


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


def git(*args: str) -> str:
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.removeprefix("export ").partition("=")
        values[key.strip()] = value.strip().strip("'\"")
    return values


def config_value(name: str, default: str = "") -> str:
    if os.environ.get(name):
        return os.environ[name]
    root = Path(git("rev-parse", "--show-toplevel").strip())
    for rel in ENV_FILES:
        value = read_env_file(root / rel).get(name)
        if value:
            return value
    return default


def build_note(repo: str, ref_name: str, commits: list[dict]) -> str:
    lines = [f'Commits pushed to "{repo}":https://github.com/{repo} (branch: {ref_name}):', ""]
    for c in commits:
        short_sha = c["id"][:12]
        first_line = c["message"].splitlines()[0]
        lines.append(f'* "{short_sha}":{c["url"]} by {c["author_name"]}: {first_line}')
        rest = TRAILER_RE.sub("", "\n".join(c["message"].splitlines()[1:])).strip()
        if rest:
            lines.append("<pre>" + rest + "</pre>")
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


# --- commit-msg ---------------------------------------------------------------

def cmd_commit_msg(msg_file: str) -> int:
    message = Path(msg_file).read_text(encoding="utf-8")
    # Ignore git's own "# ..." comment lines, which are stripped after this hook.
    body = "\n".join(line for line in message.splitlines() if not line.startswith("#"))
    if not extract_issue_ids(body) or TRAILER_RE.search(body):
        return 0
    if not config_value("REDMINE_API_KEY"):
        print(f"redmine: this commit references an issue but no REDMINE_API_KEY is set,\n"
              f"so it won't be posted to Redmine and CI will flag it.\n{SETUP_HINT}", file=sys.stderr)
        return 0
    subprocess.run(
        ["git", "interpret-trailers", "--in-place", "--if-exists", "doNothing",
         "--trailer", TRAILER, msg_file],
        check=True,
    )
    return 0


# --- pre-push -----------------------------------------------------------------

def pushed_commits(remote: str, local_sha: str, remote_sha: str) -> list[str]:
    if remote_sha == ZERO_SHA:
        # New branch: only what no ref on this remote already has.
        return git("rev-list", "--reverse", local_sha, "--not", f"--remotes={remote}").split()
    return git("rev-list", "--reverse", f"{remote_sha}..{local_sha}").split()


def commit_info(sha: str) -> dict:
    author_name, author_email, message = git("log", "-1", "--format=%an%x00%ae%x00%B", sha).split("\0", 2)
    return {"id": sha, "author_name": author_name, "author_email": author_email.lower(), "message": message}


def ledger_path() -> Path:
    return Path(git("rev-parse", "--git-common-dir").strip()) / "redmine-notified"


def cmd_pre_push(remote: str, url: str) -> int:
    api_key = config_value("REDMINE_API_KEY")
    if not api_key:
        print(f"redmine: no REDMINE_API_KEY set, not posting to Redmine.\n{SETUP_HINT}", file=sys.stderr)
        return 0
    match = GITHUB_REMOTE_RE.search(url)
    repo = match.group("repo") if match else remote
    my_email = git("config", "user.email").strip().lower()
    redmine_url = config_value("REDMINE_URL", DEFAULT_REDMINE_URL)

    # pre-push runs BEFORE the push lands, so a rejected push (non-fast-forward,
    # say) is retried with the same commits — this ledger of already-posted
    # (sha, issue) pairs keeps the retry from posting the same note twice.
    ledger = ledger_path()
    done = set(ledger.read_text().split()) if ledger.is_file() else set()

    by_issue: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for line in sys.stdin:
        local_ref, local_sha, remote_ref, remote_sha = line.split()
        if local_sha == ZERO_SHA:  # branch deletion
            continue
        branch = remote_ref.removeprefix("refs/heads/")
        for sha in pushed_commits(remote, local_sha, remote_sha):
            commit = commit_info(sha)
            # Someone else's commits (a merged or rebased branch) are theirs to
            # post, under their own key — posting them here would duplicate.
            if commit["author_email"] != my_email:
                continue
            commit["url"] = f"https://github.com/{repo}/commit/{sha}"
            for issue_id in extract_issue_ids(commit["message"]):
                if f"{sha}:{issue_id}" not in done:
                    by_issue[(issue_id, branch)].append(commit)

    for (issue_id, branch), commits in by_issue.items():
        try:
            post_note(redmine_url, api_key, issue_id, build_note(repo, branch, commits))
        except urllib.error.HTTPError as exc:
            print(f"redmine: issue #{issue_id}: FAILED ({exc.code} {exc.reason}), not posted", file=sys.stderr)
            continue
        except urllib.error.URLError as exc:
            print(f"redmine: issue #{issue_id}: FAILED ({exc.reason}), not posted", file=sys.stderr)
            continue
        print(f"redmine: issue #{issue_id}: posted note for {len(commits)} commit(s)", file=sys.stderr)
        with ledger.open("a") as f:
            f.writelines(f"{c['id']}:{issue_id}\n" for c in commits)
    return 0


# --- check (CI) ---------------------------------------------------------------

def ci_commits() -> list[dict]:
    """The commits to check, as {id, message, committer_email}: every commit
    of the PR on pull_request, the push payload's own commits on push."""
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as f:
        event = json.load(f)
    if "pull_request" in event:
        pr = event["pull_request"]
        out = git("log", "--format=%H%x00%ce%x00%B%x1e", f"{pr['base']['sha']}..{pr['head']['sha']}")
        commits = []
        for record in filter(str.strip, out.split("\x1e")):
            sha, committer_email, message = record.strip("\n").split("\0", 2)
            commits.append({"id": sha, "message": message, "committer_email": committer_email})
        return commits
    return [
        {"id": c["id"], "message": c["message"], "committer_email": c.get("committer", {}).get("email", "")}
        for c in event.get("commits", [])
    ]


def cmd_check() -> int:
    missing = [
        c for c in ci_commits()
        if extract_issue_ids(c["message"])
        and not TRAILER_RE.search(c["message"])
        and c["committer_email"].lower() != GITHUB_COMMITTER_EMAIL
    ]
    for c in missing:
        print(f"::error::Commit {c['id'][:12]} references a Redmine issue but was not made with "
              f"the Redmine hooks installed, so no note was posted: {c['message'].splitlines()[0]}")
    if missing:
        print(f"\n{len(missing)} commit(s) missing the '{TRAILER}' trailer.\n{SETUP_HINT}\n"
              "Then re-run the hook over those commits and force-push, e.g.:\n  git rebase --exec 'git commit --amend --no-edit' <base-branch>")
        return 1
    print("All commits that reference a Redmine issue were made with the hooks installed.")
    return 0


def main(argv: list[str]) -> int:
    commands = {
        "commit-msg": lambda: cmd_commit_msg(argv[1]),
        "pre-push": lambda: cmd_pre_push(argv[1], argv[2]),
        "check": cmd_check,
    }
    if not argv or argv[0] not in commands:
        print(f"usage: redmine.py {{{','.join(commands)}}} ...", file=sys.stderr)
        return 2
    return commands[argv[0]]()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
