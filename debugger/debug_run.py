"""Debug entrypoint for stepping through eea_datalakehouse's src code.

Run under VS Code's debugger — F5, or "Debug src (mocked)" in the Run and
Debug panel — using the project's .venv interpreter (where the package is
installed editable, so breakpoints land on the actual files under src/).
From a terminal, the equivalent is:

    .venv/bin/python3 debugger/debug_run.py

Set breakpoints directly in the src/ files you want to step through, e.g.:

- src/eea_datalakehouse/data_preparation/transformation.py
  (ParquetTransformer.prepare / .validate)
- src/eea_datalakehouse/dds_ingestion/folder.py
  (FolderIngest.run / _upload_all / _upload_one)
- src/eea_datalakehouse/dds_ingestion/client.py
  (IngestClient.begin / .upload_file / ._post)

The dds_ingestion side is fully mocked with respx (DDS API + presigned S3
endpoints) against a temp folder of fake parquet files — no real network
call, no real DDS/S3 side effects, safe to re-run as often as you like.
"""

from __future__ import annotations

import tempfile
import os
import json
import httpx



import argparse
import atexit
import subprocess
import sys

import requests

from pathlib import Path
from xmlrpc.client import Boolean
from eea_datalakehouse.catalog import Catalog
from eea_datalakehouse.dds_ingestion import DremioCreds, FolderIngest, IngestClient
from eea_datalakehouse.dds_ingestion.common.dremio_identity import (endpoint, dds_credentials,
                                    resolve as _resolve_identity)


BASE_URL = "https://dds.debug.local"
# This script always lives directly in debugger/, so its own directory *is*
# the folder to look in — independent of whatever cwd the debugger/terminal
# happened to launch with.   
CACHE_PATH = os.path.expanduser("~/.dremio_msal_cache.json")
TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange"
JWT_TYPE = "urn:ietf:params:oauth:token-type:jwt"
PAT_TYPE = "urn:ietf:params:oauth:token-type:dremio:personal-access-token"

ENV_PATH = Path(__file__).resolve().parent / ".env"



def PreflightCheck() -> Boolean:


    problems = []

    # 1. the parquet exists and looks like what step 2 promised
    files = sorted(PARQUET_DIR.glob("*.parquet"))
    total_bytes = sum(f.stat().st_size for f in files)
    print(f"parquet     {len(files)} files, {total_bytes/1e6:.1f} MB in {PARQUET_DIR}")
    if not files:
        problems.append(f"no parquet in {PARQUET_DIR} — run step 2 (lib/prepare.py) first")

    # 2. row count still matches what the release declares — catches a stale or
    #    partial prepare, which is exactly the thing that would silently ingest wrong.
    try:
        import pyarrow.parquet as pq
        rows = sum(pq.read_metadata(f).num_rows for f in files)
        expected = 30256 #int(CONFIG["payload"]["table_range"].rsplit(":", 1)[1].lstrip("ABCDEFGHIJKLM")) - 1
        print(f"rows        {rows:,} (expected {expected:,})")
        if rows != expected:
            problems.append(f"parquet holds {rows:,} rows, the release declares {expected:,}")
    except Exception as e:                      # noqa: BLE001 — report, do not mask
        problems.append(f"could not read parquet metadata: {e}")

    # 3. credentials present (never printed)
    for var in ("_DREMIO_USER", "_DREMIO_PWD", "DDS_BASE_URL"):
        if not os.environ.get(var):
            problems.append(f"{var} is not set in the environment")
    print("credentials " + ("present" if all(os.environ.get(v) for v in ("_DREMIO_USER", "_DREMIO_PWD")) else "MISSING"))

    # 4. the Document Service answers
    if DDS_BASE_URL:
        try:
            r = httpx.get(f"{DDS_BASE_URL}/health", timeout=5)
            print(f"dds         {r.status_code} {r.text[:60]}")
            if r.status_code != 200:
                problems.append(f"document service /health returned {r.status_code}")
        except Exception as e:                  # noqa: BLE001
            problems.append(f"document service unreachable at {DDS_BASE_URL}: {e}")

    print()
    if problems:
        print("PREFLIGHT FAILED")
        for p in problems:
            print(f"  - {p}")
        return False
    else:
        print("preflight OK")
        return True 



def run_dds_ingestion() -> None:
    if DRY_RUN:
        files = sorted(PARQUET_DIR.glob("*.parquet"))
        total_bytes = sum(f.stat().st_size for f in files)        
        print("DRY RUN — not transferring. Set DRY_RUN = False to run this cell for real.")
        print(f"  would send  {len(files)} parquet files ({total_bytes/1e6:.1f} MB)")
        print(f"  to          {TARGET_CATALOG_PATH}.{TABLE_NAME}")
        print(f"  intent      {INTENT}, conflict_mode {CONFLICT_MODE}")
        print(f"  key         {IDEMPOTENCY_KEY}")
        outcome = None
    else:
        outcome = FolderIngest(
            folder=PARQUET_DIR,
            target_catalog_path=TARGET_CATALOG_PATH,
            data_format="parquet",
            intent=INTENT,
            table_name=TABLE_NAME,
            conflict_mode=CONFLICT_MODE,
            parallelism=PARALLELISM,
            idempotency_key=IDEMPOTENCY_KEY,
        ).run()

        print(f"session     {outcome.begin.session_id}")
        print(f"uploaded    {outcome.files_uploaded}  skipped {outcome.files_skipped}")
        print(f"status      {outcome.commit.status}")
        print(f"table       {outcome.commit.table_path}")
        print(f"records     {outcome.commit.record_count:,}" if outcome.commit.record_count is not None else "records     ")


def run_table2view() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    if DRY_RUN:
        print("DRY RUN — not creating the view. Set DRY_RUN = False to run this for real.")
        print(f"  view      {VIEW_PATH}")
        print(f"  source    {SOURCE_PATH}")
        return

    # CREATE VIEW is metadata-only — row_count on the result is always 0 (no
    # rows are copied), so it says nothing about success. See
    # run_gettablesfrom() / run_gettableitemsfrom() for the real
    # verification: an independent look-up asking the catalog itself whether
    # the view is actually there and queryable now.
    catalog.table2view(VIEW_PATH, SOURCE_PATH, idempotency_key=TABLE2VIEW_IDEMPOTENCY_KEY)


def run_datacopy() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    target_path = f"{VIEW_PATH}_copy"
    source_path = "catalog.water_management_resources.bathing_water.bwd.draft.bw_assessment.test_view"
    target_path = "catalog.water_management_resources.bathing_water.bwd.draft.altia_test"


    #target_path="catalog.water_management_resources.bathing_water.bwd.draft.altia_test.aaa.vvvv"
    if DRY_RUN:
        print("DRY RUN — not copying data. Set DRY_RUN = False to run this for real.")
        print(f"  source    {VIEW_PATH}")
        print(f"  target    {target_path}")
        return
    # datacopy always goes over catalog._flight_executor, never REST — set a
    # breakpoint on FlightSqlExecutor._get_client() (sql.py) to confirm the
    # gRPC channel is only opened once here and reused for both steps.
    print(f"datacopy    via {catalog._flight_executor!r}")
    result = catalog.datacopy(
        source_path,
        target_path,
        overwrite=True,
        create_target_folder=False,
        idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-datacopy",
    )
    print(f"datacopy    {VIEW_PATH} -> {target_path}  row_count {result.row_count}")


def run_datamove() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    source_path = "catalog.water_management_resources.bathing_water.bwd.draft.altia_test.test_view"
    target_path = "catalog.water_management_resources.bathing_water.bwd.draft.altia_test.aaa.vvvv"
    #target_path = f"{VIEW_PATH}_moved"
    if DRY_RUN:
        print("DRY RUN — not moving data. Set DRY_RUN = False to run this for real.")
        print(f"  source    {source_path}")
        print(f"  target    {target_path}")
        return
    # datamove always goes over catalog._flight_executor, never REST — same
    # reasoning as run_datacopy() above.
    print(f"datamove    via {catalog._flight_executor!r}")
    result = catalog.datamove(
        source_path,
        target_path,
        # entry_type omitted — auto-detected from INFORMATION_SCHEMA now,
        # regardless of whether source_path is actually a table or a view.
        create_target_folder=False,
        idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-datamove",
    )
    print(f"datamove    {VIEW_PATH} -> {target_path}  row_count {result.row_count}")


def run_gettableitemsfrom() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    # Confirms the view is actually queryable (schema + row count), not just
    # present in a catalog listing.
    info = catalog.gettableitemsfrom(VIEW_PATH, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-items")
    print(f"gettableitemsfrom  {VIEW_PATH}")
    print(f"  schema     {info.schema}")
    print(f"  row_count  {info.row_count:,}")


def run_gettablesfrom() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    #schema_path, _, _ = VIEW_PATH.rpartition(".")

    schema_path, _, _ = "catalog.airpollut12ion.dsafdsafdsafd.aa".rpartition(".")
    # gettablesfrom returns full paths (it recurses into subfolders), not
    # bare names — compare against VIEW_PATH itself, not the leaf name.
    tables = catalog.gettablesfrom(schema_path, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-verify")
    if VIEW_PATH in tables:
        print(f"gettablesfrom  OK — {VIEW_PATH} confirmed under {schema_path}")
    else:
        print(f"gettablesfrom  {VIEW_PATH!r} was NOT found under {schema_path!r}")
        print(f"  found instead: {tables}")


def run_deleteview() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    if DRY_RUN:
        print("DRY RUN — not deleting the view. Set DRY_RUN = False to run this for real.")
        print(f"  view      {VIEW_PATH}")
        return
    catalog.deleteview(VIEW_PATH, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-delete")
    print(f"deleteview  {VIEW_PATH} dropped (if it existed)")


def run_getwikifrom() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    wiki = catalog.getwikifrom(VIEW_PATH, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-wiki")
    print(f"getwikifrom  {VIEW_PATH}")
    print(wiki)


def run_gettagsfrom() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    tags = catalog.gettagsfrom(VIEW_PATH, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-tags")
    print(f"gettagsfrom  {VIEW_PATH}")
    print(f"  tags       {tags}")


def run_setwikito() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    if DRY_RUN:
        print("DRY RUN — not setting the wiki. Set DRY_RUN = False to run this for real.")
        print(f"  view      {VIEW_PATH}")
        return
    wiki_text = "# Bathing water assessments\n\nDebug-set wiki text for testing setwikito."
    #wiki_text ="blabla"
    meta_tags = [
        {"tag_name": "owner", "tag_value": "bwd-team", "tag_title": "Owner"},
        {"tag_name": "status", "tag_value": "debug", "tag_title": "Status"},
    ]
    catalog.setwikito(
        VIEW_PATH,
        wiki_text,
        tags=meta_tags,
        idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-set-wiki",
    )
    print(f"setwikito  wiki set on {VIEW_PATH} (with {len(meta_tags)} meta tags)")


def run_deletewiki() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    if DRY_RUN:
        print("DRY RUN — not deleting the wiki. Set DRY_RUN = False to run this for real.")
        print(f"  view      {VIEW_PATH}")
        return
    catalog.deletewiki(VIEW_PATH, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-delete-wiki")
    print(f"deletewiki  wiki cleared on {VIEW_PATH} (if it had one)")


def run_setmeta2wiki() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    # Folders only — tables/views (like VIEW_PATH used elsewhere) have
    # Dremio's own tags/labels for this instead and would raise here.
    folder_path = "catalog.water_management_resources.bathing_water.bwd.draft.altia_test"
    if DRY_RUN:
        print("DRY RUN — not updating wiki metadata. Set DRY_RUN = False to run this for real.")
        print(f"  folder    {folder_path}")
        return
    new_tags = [{"tag_name": "reviewed_by", "tag_value": "debug-run", "tag_title": "Reviewed by"}]
    catalog.setmeta2wiki(
        folder_path,
        tags=new_tags,
        overwrite=False,  # merge with whatever tags are already there
        idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-write-meta",
    )
    print(f"setmeta2wiki  meta updated on {folder_path}")


def run_getmetafromwiki() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    folder_path = "catalog.water_management_resources.bathing_water.bwd.draft.altia_test"
    # run_setmeta2wiki() must have run at least once first, so there's a
    # "# Meta Data" section here to read back.
    all_tags = catalog.getmetafromwiki(
        folder_path, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-read-meta-all"
    )
    print(f"getmetafromwiki  all tags on {folder_path}: {all_tags}")

    one_tag = catalog.getmetafromwiki(
        folder_path,
        "reviewed_by",
        "tag_value",
        idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-read-meta-one",
    )
    print(f"getmetafromwiki  reviewed_by's tag_value: {one_tag}")


def run_settagsto() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    if DRY_RUN:
        print("DRY RUN — not setting tags. Set DRY_RUN = False to run this for real.")
        print(f"  view      {VIEW_PATH}")
        return
    tags = ["bathing-water", "debug"]
    catalog.settagsto(VIEW_PATH, tags, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-set-tags")
    print(f"settagsto  tags set on {VIEW_PATH}: {tags}")


def run_deletetags() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    tags = ["debug"]
    if DRY_RUN:
        print("DRY RUN — not deleting tags. Set DRY_RUN = False to run this for real.")
        print(f"  view      {VIEW_PATH}")
        print(f"  tags      {tags}")
        return
    catalog.deletetags(VIEW_PATH, tags, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-delete-tags")
    print(f"deletetags  removed {tags} from {VIEW_PATH}")


def run_createfolder() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    folder_path = "catalog.water_management_resources.bathing_water.bwd.draft.altia_test"
    if DRY_RUN:
        print("DRY RUN — not creating a folder. Set DRY_RUN = False to run this for real.")
        print(f"  folder    {folder_path}")
        return
    catalog.createfolder(
        folder_path,
        create_parents=True,
        idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-createfolder",
    )
    print(f"createfolder  {folder_path}")


def run_deletefolder() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    folder_path = "catalog.water_management_resources.bathing_water.bwd.draft.altia_test"
    if DRY_RUN:
        print("DRY RUN — not deleting a folder. Set DRY_RUN = False to run this for real.")
        print(f"  folder    {folder_path}")
        return
    catalog.deletefolder(
        folder_path,
        cascade=True,
        idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-deletefolder",
    )
    print(f"deletefolder  {folder_path}")


def run_catalog_close() -> None:
    """Exercises Catalog.close() directly — set a breakpoint on it
    (client.py) to step into the underlying executor's/catalog_rest's own
    close() (FlightSqlExecutor's FlightClient.close(), or RestSqlExecutor's
    httpx.Client.close())."""
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    print(f"catalog_close  {catalog!r}  closed={catalog._closed}")

    catalog.gettagsfrom(VIEW_PATH, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-lifecycle")

    catalog.close()
    print(f"catalog_close  after first close()   closed={catalog._closed}")
    catalog.close()  # idempotent — must not raise or reconnect
    print(f"catalog_close  after second close()  closed={catalog._closed}")


def run_catalog_context_manager() -> None:
    """Confirms `with Catalog(...) as catalog:` closes it on exit, without
    an explicit catalog.close() call."""
    with Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME) as catalog:
        catalog.gettagsfrom(VIEW_PATH, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-lifecycle-ctx")
        print(f"catalog_context_manager  inside with-block  closed={catalog._closed}")

    print(f"catalog_context_manager  after with-block    closed={catalog._closed}")


def run_catalog_bulk_close() -> None:
    """Exercises the same helper the atexit/SIGTERM/SIGINT fallback uses
    (`_close_all_open_catalogs`) — without actually exiting the process or
    sending a signal — to confirm every still-open Catalog gets disposed."""
    from eea_datalakehouse.catalog.client import _close_all_open_catalogs

    a = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    b = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    print(f"catalog_bulk_close  before  a.closed={a._closed}  b.closed={b._closed}")

    _close_all_open_catalogs()

    print(f"catalog_bulk_close  after   a.closed={a._closed}  b.closed={b._closed}")



# --------------------------------------------------------------------------
# Step 1 — get an Entra ID JWT
# --------------------------------------------------------------------------
def entra_token_azcli(scope: str) -> str:
    """Reuse the Azure CLI's cached login. Silent, no prompts."""
    out = subprocess.run(
        ["az", "account", "get-access-token", "--scope", scope, "-o", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)["accessToken"]


def _msal_app(cls, **kwargs):
    import msal

    cache = msal.SerializableTokenCache()
    if os.path.exists(CACHE_PATH):
        cache.deserialize(open(CACHE_PATH).read())

    def _flush():
        if cache.has_state_changed:
            fd = os.open(CACHE_PATH, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w") as fh:
                fh.write(cache.serialize())

    atexit.register(_flush)
    return cls(token_cache=cache, **kwargs)


def entra_token_device(tenant: str, client_id: str, scope: str) -> str:
    """Device-code flow with a persistent cache: interactive once, silent after."""
    import msal

    app = _msal_app(
        msal.PublicClientApplication,
        client_id=client_id,
        authority=f"https://login.microsoftonline.com/{tenant}",
    )

    accounts = app.get_accounts()
    result = app.acquire_token_silent([scope], account=accounts[0]) if accounts else None

    if not result:
        flow = app.initiate_device_flow(scopes=[scope])
        if "user_code" not in flow:
            raise RuntimeError(f"device flow failed: {flow}")
        print(flow["message"], file=sys.stderr)
        result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        raise RuntimeError(f"Entra ID error: {result.get('error_description', result)}")
    return result["access_token"]


def entra_token_sp(tenant: str, client_id: str, client_secret: str, scope: str) -> str:
    """Client credentials — fully unattended, app identity."""
    import msal

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=f"https://login.microsoftonline.com/{tenant}",
    )
    result = app.acquire_token_for_client(scopes=[scope])
    if "access_token" not in result:
        raise RuntimeError(f"Entra ID error: {result.get('error_description', result)}")
    return result["access_token"]


# --------------------------------------------------------------------------
# Step 2 — exchange the Entra JWT for a Dremio access token
# --------------------------------------------------------------------------
def dremio_exchange(host: str, subject_token: str, token_type: str = JWT_TYPE,
                    verify: bool | str = True) -> dict:
    r = requests.post(
        f"https://{host}/oauth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": TOKEN_EXCHANGE,
            "subject_token": subject_token,
            "subject_token_type": token_type,
            "scope": "dremio.all",
        },
        timeout=30,
        verify=verify,
    )
    if not r.ok:
        raise RuntimeError(f"Dremio token exchange failed [{r.status_code}]: {r.text}")
    return r.json()


# --------------------------------------------------------------------------
# Step 3 — optional: mint a long-lived PAT with that access token
# --------------------------------------------------------------------------
def create_pat(host: str, access_token: str, username: str, label: str,
               days: int = 90, verify: bool | str = True) -> dict:
    h = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    u = requests.get(f"https://{host}/api/v3/user/by-name/{username}",
                     headers=h, timeout=30, verify=verify)
    if not u.ok:
        raise RuntimeError(f"user lookup failed [{u.status_code}]: {u.text}")
    user_id = u.json()["id"]

    p = requests.post(
        f"https://{host}/api/v3/user/{user_id}/token",
        headers=h,
        json={"label": label, "millisecondsToExpire": days * 86_400_000},
        timeout=30,
        verify=verify,
    )
    if not p.ok:
        raise RuntimeError(f"PAT creation failed [{p.status_code}]: {p.text}")
    return p.json()



if __name__ == "__main__":
    if not ENV_PATH.exists():
        raise RuntimeError(f"could not find {ENV_PATH}")

    loaded = []
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ[key.strip()] = value.strip()
        loaded.append(key.strip())

    ##READ CONFIG VALUES
    # --- what to send ----------------------------------------------------------
    # Anchored to this script's own location (debugger/), not cwd — same reasoning
    # as ENV_PATH above: correct regardless of where debug_run.py is launched from.
    DATAFLOW_DIR = Path(__file__).resolve().parent / "bathing_water"
    CONFIG       = json.loads((DATAFLOW_DIR / "config" / "source.json").read_text())
    PARQUET_DIR  = DATAFLOW_DIR / "output" / "parquet"

    # --- where it lands --------------------------------------------------------
    # Ingest lands in `draft`, and that is where this notebook stops. Promotion to
    # versions.v2025_1 and repointing the consumer views is a separate, deliberate
    # step and deliberately not automated here.
    TARGET_CATALOG_PATH = CONFIG["target"]["ingest_catalog_path"]
    TABLE_NAME          = CONFIG["target"]["table"]
    RELEASE             = CONFIG["target"]["release"]

    # --- how ------------------------------------------------------------------
    INTENT        = "editable"   # CTAS into an Iceberg table: this table is served, not an archival drop
    CONFLICT_MODE = "replace"    # re-running the same release replaces the draft rather than appending
    PARALLELISM   = 4
    # Every DDS call gets this long. The client default is 60s, which is not enough
    # for `begin`: that call authorises the target against Dremio before a single
    # byte is uploaded, and a Dremio engine that is still starting can take minutes
    # to answer. 60s there fails the run before anything has happened.
    DDS_TIMEOUT    = 900        # seconds, per HTTP call

    # Send every file as a multipart PUT instead of a pre-signed POST form upload.
    # The server picks POST for anything under its multipart threshold
    # (`use_multipart = body.multipart or size > multipart_threshold`), and the S3
    # backend behind this deployment does not implement POST Object — it answers
    # `501 XNotImplemented` on `POST /<bucket>`. Multipart presigns a PUT per part,
    # which it does implement. Set this back to False once the storage supports
    # POST: it is a workaround for the backend, not a property of the data.
    FORCE_MULTIPART = True
    IDEMPOTENCY_KEY = f"bwd-{RELEASE}"   # change it only to force a genuinely new session

    # --- table2view demo --------------------------------------------------------
    VIEW_PATH = "catalog.water_management_resources.bathing_water.bwd.draft.bw_assessment.test_view"
    SOURCE_PATH = (
        "nossl_s3.datahub-pre-01.datasets."
        "[EU SDG 14_40] Bathing waters with excellent quality."
        "Bathing waters with excellent quality 2000-2024."
        "eea_s_eu-sdg-14-40_p_2000-2024_v02_r00"
    )
    TABLE2VIEW_IDEMPOTENCY_KEY = "debug-table2view-testview"

    # --- services --------------------------------------------------------------
    # Resolved most-authoritative-first by endpoint():
    #   1. the JupyterLab "Dremio Catalog" settings panel — ddsServerUrl / dremioUrl.
    #      That panel is where these are actually configured, and `%init` cannot see
    #      it: the magic binds from the kernel *environment*, which is not where the
    #      panel writes. A Hub kernel whose environment still says `dds-server`
    #      would otherwise override the URL you set in the settings UI.
    #   2. whatever `%init` did bind;
    #   3. this project's .env, for running outside JupyterLab entirely.
    DDS_BASE_URL    = endpoint("DDS_BASE_URL")
    DREMIO_BASE_URL = endpoint("DREMIO_URL", aliases=("DREMIO_BASE_URL",))

    # --- identity --------------------------------------------------------------
    # Resolved with the same precedence as the endpoints: Dremio Catalog panel
    # first, then whatever %init bound into this kernel. The token is never printed.
    _ident = _resolve_identity()
    DREMIO_USERNAME = _ident.get("DREMIO_USERNAME") or globals().get("DREMIO_USERNAME", "")
    DREMIO_TOKEN    = _ident.get("DREMIO_TOKEN")    or globals().get("DREMIO_TOKEN", "")

    # --- safety ----------------------------------------------------------------
    DRY_RUN = False    # set False to actually transfer

    os.environ.setdefault("_DREMIO_USER", os.environ.get("DREMIO_USER", ""))
    os.environ.setdefault("_DREMIO_PWD", os.environ.get("DREMIO_TOKEN", ""))


    print(f"release   {RELEASE}  ({CONFIG['sdi']['latest_record_title']})")
    print(f"target    {TARGET_CATALOG_PATH}.{TABLE_NAME}")
    print(f"intent    {INTENT}   conflict_mode {CONFLICT_MODE}")
    print(f"identity  {DREMIO_USERNAME or '<none>'}")
    print(f"dds       {DDS_BASE_URL or '<unset>'}")
    print(f"dremio    {DREMIO_BASE_URL or '<unset>'}")
    print(f"dry run   {DRY_RUN}")

    #if not PreflightCheck():
    #    raise RuntimeError("preflight failed, aborting")

    #run_dds_ingestion()
    #run_table2view()
    #run_datacopy()
    #run_datamove()
    #run_gettablesfrom()
    #run_gettableitemsfrom()
    
    #run_setwikito()
    #run_deletewiki()
    #run_setmeta2wiki()
    #run_getmetafromwiki()
    #run_settagsto()
    #run_deletetags()

    #run_getwikifrom()
    #run_gettagsfrom()
    #run_deleteview()

    run_createfolder()
    #run_deletefolder()





    """
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["azcli", "device", "sp"], default="device")
    ap.add_argument("--host", default=os.getenv("DREMIO_HOST"))
    ap.add_argument("--tenant", default=os.getenv("ENTRA_TENANT_ID"))
    ap.add_argument("--client-id", default=os.getenv("ENTRA_CLIENT_ID"))
    ap.add_argument("--client-secret", default=os.getenv("ENTRA_CLIENT_SECRET"))
    ap.add_argument("--scope", default=os.getenv("ENTRA_SCOPE"))
    ap.add_argument("--create-pat", metavar="USERNAME",
                    help="also mint a PAT for this Dremio username")
    ap.add_argument("--pat-label", default="automated")
    ap.add_argument("--pat-days", type=int, default=90, help="1-180, default 90")
    ap.add_argument("--ca-bundle", help="path to CA bundle for self-signed Dremio certs")
    ap.add_argument("--insecure", action="store_true", help="skip TLS verification")
    args = ap.parse_args()

    missing = [n for n, v in [("--host", args.host), ("--scope", args.scope)] if not v]
    if args.mode != "azcli":
        missing += [n for n, v in [("--tenant", args.tenant),
                                   ("--client-id", args.client_id)] if not v]
    if args.mode == "sp" and not args.client_secret:
        missing.append("--client-secret")
    if missing:
        ap.error("missing required: " + ", ".join(missing))

    verify: bool | str = args.ca_bundle or (not args.insecure)

    if args.mode == "azcli":
        jwt = entra_token_azcli(args.scope)
    elif args.mode == "device":
        jwt = entra_token_device(args.tenant, args.client_id, args.scope)
    else:
        jwt = entra_token_sp(args.tenant, args.client_id, args.client_secret, args.scope)

    tok = dremio_exchange(args.host, jwt, verify=verify)
    access_token = tok["access_token"]
    print(f"# Dremio access token (expires in {tok.get('expires_in')}s)", file=sys.stderr)
    print(access_token)

    if args.create_pat:
        pat = create_pat(args.host, access_token, args.create_pat,
                         args.pat_label, args.pat_days, verify=verify)
        print("# PAT (store it now, it is not retrievable again)", file=sys.stderr)
        print(pat.get("token") or json.dumps(pat))

    print ("OK")
    """    
