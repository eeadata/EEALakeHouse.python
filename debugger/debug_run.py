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

    target_path="catalog.water_management_resources.bathing_water.bwd.draft.altia_test.aaa.vvvv"
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
        VIEW_PATH,
        target_path,
        mode="replace",
        create_target_folder=False,
        idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-datacopy",
    )
    print(f"datacopy    {VIEW_PATH} -> {target_path}  row_count {result.row_count}")


def run_datamove() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    source_path = "catalog.water_management_resources.bathing_water.bwd.draft.altia_test.test_view"
    target_path="catalog.water_management_resources.bathing_water.bwd.draft.altia_test.aaa.vvvv"
    target_path = f"{VIEW_PATH}_moved"
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
        create_target_folder=True,
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


def run_assignwikito() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    if DRY_RUN:
        print("DRY RUN — not setting the wiki. Set DRY_RUN = False to run this for real.")
        print(f"  view      {VIEW_PATH}")
        return
    wiki_text = "# Bathing water assessments\n\nDebug-set wiki text for testing assignwikito."
    #wiki_text ="blabla"
    catalog.assignwikito(VIEW_PATH, wiki_text, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-set-wiki")
    print(f"assignwikito  wiki set on {VIEW_PATH}")


def run_assigntagsto() -> None:
    catalog = Catalog(DREMIO_BASE_URL, DREMIO_TOKEN, username=DREMIO_USERNAME)
    if DRY_RUN:
        print("DRY RUN — not setting tags. Set DRY_RUN = False to run this for real.")
        print(f"  view      {VIEW_PATH}")
        return
    tags = ["bathing-water", "debug"]
    catalog.assigntagsto(VIEW_PATH, tags, idempotency_key=f"{TABLE2VIEW_IDEMPOTENCY_KEY}-set-tags")
    print(f"assigntagsto  tags set on {VIEW_PATH}: {tags}")


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
    run_datamove()
    #run_gettablesfrom()
    #run_gettableitemsfrom()
    
    #run_assignwikito()
    #run_assigntagsto()
    #run_deletetags()

    #run_getwikifrom()
    #run_gettagsfrom()
    #run_deleteview()

    #run_catalog_close()
    #run_catalog_context_manager()
    #run_catalog_bulk_close()


    
