"""
ingest_posts_dag.py
===================
FIPSAR Internship — API Data Ingestion Pipeline
Author  : Joshua
Purpose : Airflow DAG that schedules and executes the JSONPlaceholder
          posts ingestion pipeline every 5 minutes.

DAG ID  : fipsar_ingest_posts
Schedule: Every 5 minutes (*/5 * * * *)
Tasks   :
    1. check_db_connection   → quick ping to verify PostgreSQL is reachable
    2. ingest_posts          → fetch from API and upsert into posts table
    3. log_run_summary       → print final counts to Airflow task logs

Retry policy:
    - 3 retries per task
    - 1 minute wait between retries
    - Alerts logged on every retry

How Airflow finds this file:
    Airflow scans the folder set in AIRFLOW__CORE__DAGS_FOLDER.
    Any .py file at the top level of that folder is auto-discovered.
    This file must live in your dags/ folder — not inside a subfolder.
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Airflow imports ────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Make scripts/ importable from the DAG ─────────────────────
# Airflow runs DAGs from its own working directory, so we add
# the project root to sys.path so `from scripts.ingest_posts import ...`
# resolves correctly regardless of where Airflow is installed.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent  # mcp-workspace/
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Import the ingestion function we wrote in scripts/
from scripts.ingest_posts import run_ingestion  # noqa: E402

# =============================================================
# LOGGER
# Airflow captures anything written to Python's logging module
# and shows it in the task log UI (Admin → Task Logs).
# =============================================================
log = logging.getLogger(__name__)


# =============================================================
# DEFAULT ARGS
# Applied to every task in the DAG unless overridden per-task.
# =============================================================
DEFAULT_ARGS = {
    # DAG owner shown in the Airflow UI
    "owner": "joshua_fipsar",

    # If True, Airflow runs all missed intervals since start_date
    # on first deployment. False = start fresh from now.
    "depends_on_past": False,

    # Email alerts (set your address here if Airflow SMTP is configured)
    "email": [],
    "email_on_failure": False,
    "email_on_retry": False,

    # Retry policy — 3 attempts, 1 minute apart
    # Network blips and transient DB errors usually resolve in < 60s
    "retries": 3,
    "retry_delay": timedelta(minutes=1),

    # If a single task runs longer than this, Airflow kills it
    # 5 minutes is generous for a simple API fetch + DB write
    "execution_timeout": timedelta(minutes=5),
}


# =============================================================
# TASK FUNCTIONS
# Each function below is wrapped in a PythonOperator.
# Airflow passes a 'context' dict automatically — it contains
# the run_id, dag_id, task_id, execution_date, etc.
# =============================================================

def task_check_db_connection(**context) -> None:
    """
    Task 1 — Sanity check before ingestion starts.

    Attempts a lightweight PostgreSQL connection and immediately closes it.
    If this fails, the ingestion task never runs — saving a wasted retry
    on a DB that's clearly down.

    Raises:
        Exception: If DB is unreachable (triggers Airflow retry).
    """
    import os
    import psycopg2
    from dotenv import load_dotenv

    # Load .env — Airflow may not have these vars unless injected via
    # Airflow Connections or environment. .env is the fallback for local dev.
    load_dotenv(_PROJECT_ROOT / ".env", override=False)

    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]

    log.info(f"[{dag_id}] DB connection check — run_id: {run_id}")

    try:
        conn = psycopg2.connect(
            host     = os.getenv("POSTGRES_HOST", "localhost"),
            port     = int(os.getenv("POSTGRES_PORT", "5432")),
            dbname   = os.getenv("POSTGRES_DB"),
            user     = os.getenv("POSTGRES_USER"),
            password = os.getenv("POSTGRES_PASSWORD"),
        )
        conn.close()
        log.info(f"[{dag_id}] PostgreSQL connection — OK ✓")

    except Exception as exc:
        log.error(f"[{dag_id}] PostgreSQL connection — FAILED: {exc}")
        # Re-raise so Airflow marks the task as failed and triggers retry
        raise


def task_ingest_posts(**context) -> None:
    """
    Task 2 — Main ingestion task.

    Calls run_ingestion() from scripts/ingest_posts.py.
    Passes dag_id and task_id so the ingestion_logs table records
    exactly which DAG run produced each log row.

    Raises:
        RuntimeError: If run_ingestion() returns status != 'success',
                      so Airflow marks the task failed and retries.
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["run_id"]

    log.info(f"[{dag_id}/{task_id}] Starting ingestion — run_id: {run_id}")
    log.info(f"[{dag_id}/{task_id}] Execution date (UTC): "
             f"{datetime.now(timezone.utc).isoformat()}")

    # run_ingestion never raises — it returns a result dict
    result = run_ingestion(dag_id=dag_id, task_id=task_id)

    # Log the summary to Airflow's task log UI
    log.info(
        f"[{dag_id}/{task_id}] Ingestion result — "
        f"status={result['status']} | "
        f"fetched={result['records_fetched']} | "
        f"inserted={result['records_inserted']} | "
        f"updated={result['records_updated']}"
    )

    # If ingestion failed, raise so Airflow retries this task
    if result["status"] != "success":
        error_detail = result.get("error", "unknown error")
        log.error(f"[{dag_id}/{task_id}] Ingestion failed: {error_detail}")
        raise RuntimeError(
            f"Ingestion pipeline returned status='{result['status']}'. "
            f"Error: {error_detail}"
        )

    # Push result to XCom so the next task can read it
    # XCom = Airflow's key-value store for passing data between tasks
    context["ti"].xcom_push(key="ingestion_result", value=result)
    log.info(f"[{dag_id}/{task_id}] Result pushed to XCom ✓")


def task_log_run_summary(**context) -> None:
    """
    Task 3 — Print a human-readable summary to the Airflow task log.

    Pulls the ingestion result from XCom (written by task_ingest_posts)
    and logs a clean summary. Useful for a quick glance in the Airflow UI
    without opening the ingestion task's full log.

    This task always runs — even if a previous run's XCom is stale —
    so the DAG never ends without a summary entry.
    """
    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id

    # Pull result from XCom — task_ids tells Airflow which task wrote it
    result = context["ti"].xcom_pull(
        task_ids="ingest_posts",
        key="ingestion_result",
    )

    if result is None:
        log.warning(
            f"[{dag_id}/{task_id}] No XCom result found — "
            "ingestion task may have failed before pushing."
        )
        return

    # ── Print summary ──────────────────────────────────────────
    log.info("=" * 55)
    log.info("  FIPSAR PIPELINE — RUN SUMMARY")
    log.info("=" * 55)
    log.info(f"  Status          : {result['status'].upper()}")
    log.info(f"  Records Fetched : {result['records_fetched']}")
    log.info(f"  Records Inserted: {result['records_inserted']}")
    log.info(f"  Records Updated : {result['records_updated']}")
    log.info(f"  Run ID          : {context['run_id']}")
    log.info(f"  Completed (UTC) : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 55)


# =============================================================
# DAG DEFINITION
# =============================================================
with DAG(
    # Unique identifier — shown in Airflow UI sidebar
    dag_id="fipsar_ingest_posts",

    # Applied to all tasks unless overridden
    default_args=DEFAULT_ARGS,

    # Plain-English description shown in the Airflow UI
    description=(
        "Fetches posts from JSONPlaceholder API every 5 minutes "
        "and upserts them into PostgreSQL. FIPSAR Internship project."
    ),

    # Cron: */5 * * * * = every 5 minutes
    # Read as: "every 5th minute, every hour, every day"
    schedule_interval="*/5 * * * *",

    # start_date = when Airflow begins scheduling this DAG
    # days_ago(1) = yesterday, so it's immediately eligible to run
    start_date=days_ago(1),

    # False = don't auto-run all missed intervals since start_date
    # (avoids a flood of backfill runs on first deploy)
    catchup=False,

    # Tags appear as filter buttons in the Airflow UI — useful when
    # you have many DAGs in a real project
    tags=["fipsar", "ingestion", "jsonplaceholder", "postgresql"],

    # Max number of active DAG runs at once
    # 1 = never run two instances of this DAG simultaneously
    # Prevents double-writes if a run is slow and the next one starts
    max_active_runs=1,

    # Render Jinja templates in params (good practice to keep enabled)
    render_template_as_native_obj=False,

) as dag:

    # -----------------------------------------------------------------
    # TASK 1 — Check DB is reachable before doing any real work
    # -----------------------------------------------------------------
    check_db = PythonOperator(
        task_id="check_db_connection",
        python_callable=task_check_db_connection,

        # Pass the Airflow context dict to the function
        # (gives access to dag_id, run_id, execution_date, etc.)
        provide_context=True,

        # Override retry count for this specific task:
        # DB check failing 3 times = DB is genuinely down, stop fast
        retries=3,
        retry_delay=timedelta(seconds=30),  # shorter than default — check quickly
    )

    # -----------------------------------------------------------------
    # TASK 2 — Run the full ingestion pipeline
    # -----------------------------------------------------------------
    ingest = PythonOperator(
        task_id="ingest_posts",
        python_callable=task_ingest_posts,
        provide_context=True,
        # Inherits retries=3, retry_delay=1min from DEFAULT_ARGS
    )

    # -----------------------------------------------------------------
    # TASK 3 — Log a clean summary (runs after ingestion completes)
    # -----------------------------------------------------------------
    summarise = PythonOperator(
        task_id="log_run_summary",
        python_callable=task_log_run_summary,
        provide_context=True,

        # trigger_rule = "all_done" means this task runs even if
        # the ingest task failed — we always want a summary log
        trigger_rule="all_done",

        # No retries needed — this task only reads XCom and logs
        retries=0,
    )

    # -----------------------------------------------------------------
    # TASK DEPENDENCIES (execution order)
    # check_db → ingest_posts → log_run_summary
    # Airflow reads >> as "then run"
    # -----------------------------------------------------------------
    check_db >> ingest >> summarise
