"""
ingest_posts.py
===============
FIPSAR Internship — API Data Ingestion Pipeline
Author  : Joshua
Purpose : Fetch posts from JSONPlaceholder API and upsert them into
          the PostgreSQL `posts` table. Writes one row to `ingestion_logs`
          per run regardless of success or failure.

Usage (standalone):
    python scripts/ingest_posts.py

Usage (from Airflow DAG):
    from scripts.ingest_posts import run_ingestion
    run_ingestion(dag_id="posts_dag", task_id="ingest_posts_task")

Environment variables (see .env.example):
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD,
    API_BASE_URL, API_TIMEOUT, LOG_LEVEL, LOG_DIR
"""

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# =============================================================
# 0. ENVIRONMENT SETUP
# =============================================================

# Load .env from the project root (one level above scripts/)
# override=False means real environment variables take priority —
# important inside Docker / Airflow where vars are injected externally
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env", override=False)


# =============================================================
# 1. LOGGING SETUP
# =============================================================

def _setup_logger() -> logging.Logger:
    """
    Configure and return a logger that writes to both:
      - stdout (so Airflow captures it in task logs)
      - a rotating file under LOG_DIR (for local debugging)

    Log level is controlled by the LOG_LEVEL env var (default: INFO).
    """
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level     = getattr(logging, log_level_str, logging.INFO)

    log_dir = Path(os.getenv("LOG_DIR", str(_PROJECT_ROOT / "logs")))
    log_dir.mkdir(parents=True, exist_ok=True)  # create logs/ if missing

    # One log file per day — keeps old runs separate
    log_file = log_dir / f"ingest_posts_{datetime.now().strftime('%Y-%m-%d')}.log"

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),          # Airflow / terminal
            logging.FileHandler(log_file, encoding="utf-8"),  # local file
        ],
    )
    return logging.getLogger("fipsar.ingest_posts")


logger = _setup_logger()


# =============================================================
# 2. CONFIG HELPERS
# =============================================================

def _get_db_config() -> dict[str, Any]:
    """
    Read PostgreSQL connection parameters from environment variables.
    Raises EnvironmentError immediately if any required var is missing —
    better to crash at startup than to fail silently mid-run.
    """
    required = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB",
                 "POSTGRES_USER", "POSTGRES_PASSWORD"]

    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variable(s): {', '.join(missing)}. "
            f"Copy .env.example → .env and fill in your values."
        )

    return {
        "host"    : os.getenv("POSTGRES_HOST"),
        "port"    : int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname"  : os.getenv("POSTGRES_DB"),
        "user"    : os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def _get_api_config() -> dict[str, Any]:
    """
    Read API connection parameters from environment variables.
    Falls back to JSONPlaceholder defaults so the script works
    out-of-the-box without a real API key.
    """
    return {
        "base_url": os.getenv("API_BASE_URL", "https://jsonplaceholder.typicode.com"),
        "timeout" : int(os.getenv("API_TIMEOUT", "30")),
    }


# =============================================================
# 3. API FETCH
# =============================================================

def fetch_posts(api_config: dict[str, Any]) -> list[dict]:
    """
    Fetch all posts from the JSONPlaceholder /posts endpoint.

    Args:
        api_config: dict with 'base_url' and 'timeout' keys.

    Returns:
        List of post dicts, each with keys: id, userId, title, body.

    Raises:
        requests.exceptions.Timeout       : API took too long to respond.
        requests.exceptions.ConnectionError: Network unreachable.
        requests.exceptions.HTTPError     : Non-2xx response from API.
        ValueError                        : Response is not a JSON list.
    """
    url = f"{api_config['base_url']}/posts"
    logger.info(f"Fetching posts from: {url}")

    # requests.get raises ConnectionError / Timeout automatically
    response = requests.get(url, timeout=api_config["timeout"])

    # Raise an HTTPError for 4xx / 5xx status codes
    response.raise_for_status()

    data = response.json()

    # ── Validate response structure ────────────────────────────
    # We expect a JSON array, not an object or error payload
    if not isinstance(data, list):
        raise ValueError(
            f"Expected a JSON list from API, got {type(data).__name__}. "
            f"Response snippet: {str(data)[:200]}"
        )

    if len(data) == 0:
        # Not necessarily an error, but worth warning about
        logger.warning("API returned an empty list — no posts to ingest.")
        return []

    # Spot-check the first record for expected fields
    _validate_post_schema(data[0])

    logger.info(f"Fetched {len(data)} posts successfully.")
    return data


def _validate_post_schema(post: dict) -> None:
    """
    Validate that a single post dict contains all required fields.
    Called on the first record as a quick sanity check — if the API
    changes its shape, we catch it before writing anything to the DB.

    Args:
        post: A single post dict from the API response.

    Raises:
        ValueError: If any required field is missing.
    """
    required_fields = {"id", "userId", "title", "body"}
    missing = required_fields - post.keys()

    if missing:
        raise ValueError(
            f"API response is missing expected field(s): {missing}. "
            f"Got keys: {list(post.keys())}"
        )

    # Type checks — catch silent coercion bugs early
    if not isinstance(post["id"], int):
        raise ValueError(f"Expected 'id' to be int, got {type(post['id']).__name__}")
    if not isinstance(post["userId"], int):
        raise ValueError(f"Expected 'userId' to be int, got {type(post['userId']).__name__}")

    logger.debug(f"Schema validation passed for post id={post['id']}")


# =============================================================
# 4. DATABASE OPERATIONS
# =============================================================

def get_db_connection(db_config: dict[str, Any]) -> psycopg2.extensions.connection:
    """
    Open and return a psycopg2 connection to PostgreSQL.

    Args:
        db_config: Dict with host, port, dbname, user, password.

    Returns:
        An open psycopg2 connection (autocommit=False).

    Raises:
        psycopg2.OperationalError: If the DB is unreachable or creds are wrong.
    """
    logger.info(
        f"Connecting to PostgreSQL at "
        f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    conn = psycopg2.connect(**db_config)
    # autocommit=False is psycopg2's default — we commit manually
    # so we can roll back everything if the batch partially fails
    conn.autocommit = False
    return conn


def upsert_posts(
    conn: psycopg2.extensions.connection,
    posts: list[dict],
) -> tuple[int, int]:
    """
    Insert new posts and update existing ones in a single SQL statement.

    Uses PostgreSQL's INSERT ... ON CONFLICT DO UPDATE (UPSERT) so that:
      - First run: all rows are inserted.
      - Subsequent runs: changed titles/bodies are updated; unchanged rows
        are left alone (DO UPDATE only fires if values actually differ).

    Args:
        conn  : Open psycopg2 connection (not yet committed).
        posts : List of post dicts from the API.

    Returns:
        Tuple of (records_inserted, records_updated).

    Raises:
        psycopg2.Error: On any DB-level error (rolls back automatically
                        in the calling function).
    """
    # SQL uses a CTE to track what changed — cleaner than two queries
    upsert_sql = """
        WITH upserted AS (
            INSERT INTO posts (id, user_id, title, body)
            VALUES %s
            ON CONFLICT (id) DO UPDATE
                SET
                    user_id    = EXCLUDED.user_id,
                    title      = EXCLUDED.title,
                    body       = EXCLUDED.body,
                    updated_at = NOW()
                -- Only update if something actually changed
                -- Prevents updated_at from bumping on identical re-runs
                WHERE
                    posts.title   IS DISTINCT FROM EXCLUDED.title
                    OR posts.body IS DISTINCT FROM EXCLUDED.body
            RETURNING
                id,
                -- xmax = 0 means the row was freshly inserted
                -- xmax > 0 means it was updated (existing row locked)
                (xmax = 0) AS was_inserted
        )
        SELECT
            COUNT(*) FILTER (WHERE was_inserted)      AS inserted_count,
            COUNT(*) FILTER (WHERE NOT was_inserted)  AS updated_count
        FROM upserted;
    """

    # Build list of tuples matching column order in VALUES clause:
    # (id, user_id, title, body)
    rows = [
        (post["id"], post["userId"], post["title"], post["body"])
        for post in posts
    ]

    with conn.cursor() as cur:
        # execute_values sends all rows in one round-trip — much faster
        # than looping and calling execute() for each post individually
        psycopg2.extras.execute_values(cur, upsert_sql, rows, page_size=100)

        result = cur.fetchone()
        inserted_count = result[0] if result else 0
        updated_count  = result[1] if result else 0

    logger.info(
        f"Upsert complete — inserted: {inserted_count}, updated: {updated_count}"
    )
    return inserted_count, updated_count


def write_ingestion_log(
    conn: psycopg2.extensions.connection,
    *,
    records_fetched : int,
    records_inserted: int,
    records_updated : int,
    status          : str,
    error_message   : str | None = None,
    dag_id          : str | None = None,
    task_id         : str | None = None,
) -> None:
    """
    Write one row to the `ingestion_logs` table summarising this run.

    Called at the END of every run — both on success and on failure —
    so we always have a full audit trail.

    Args:
        conn             : Open psycopg2 connection.
        records_fetched  : Total records returned by the API.
        records_inserted : New rows inserted this run.
        records_updated  : Existing rows updated this run.
        status           : 'success' | 'failed' | 'partial'.
        error_message    : Exception message on failure, else None.
        dag_id           : Airflow DAG id (optional, for Airflow runs).
        task_id          : Airflow task id (optional, for Airflow runs).
    """
    sql = """
        INSERT INTO ingestion_logs
            (records_fetched, records_inserted, records_updated,
             status, error_message, dag_id, task_id)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s);
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            records_fetched,
            records_inserted,
            records_updated,
            status,
            error_message,
            dag_id,
            task_id,
        ))

    logger.info(f"Ingestion log written — status: {status}")


# =============================================================
# 5. MAIN ORCHESTRATOR
# =============================================================

def run_ingestion(
    dag_id : str | None = None,
    task_id: str | None = None,
) -> dict[str, Any]:
    """
    Top-level function that orchestrates the full ingestion pipeline:
      1. Load config from environment
      2. Fetch posts from JSONPlaceholder
      3. Validate API response
      4. Connect to PostgreSQL
      5. Upsert posts in a single transaction
      6. Write audit log to ingestion_logs
      7. Commit — or roll back everything on any failure

    Args:
        dag_id  : Passed in by the Airflow DAG for log attribution.
        task_id : Passed in by the Airflow DAG for log attribution.

    Returns:
        Dict with keys: status, records_fetched, records_inserted,
                        records_updated, error (if any).

    This function NEVER raises — it catches all exceptions, logs them,
    writes a 'failed' log row, and returns a result dict. This allows
    Airflow to decide whether to retry based on the return value.
    """
    logger.info("=" * 60)
    logger.info("FIPSAR Ingestion Pipeline — START")
    logger.info(f"Run timestamp (UTC): {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    # Track counts across the whole run for the audit log
    records_fetched  = 0
    records_inserted = 0
    records_updated  = 0
    conn             = None

    try:
        # ── Step 1: Load config ────────────────────────────────
        db_config  = _get_db_config()
        api_config = _get_api_config()

        # ── Step 2 & 3: Fetch + validate ──────────────────────
        posts           = fetch_posts(api_config)
        records_fetched = len(posts)

        if records_fetched == 0:
            # Nothing to do — log it and exit cleanly
            logger.warning("No records fetched. Exiting early.")
            return {
                "status"          : "success",
                "records_fetched" : 0,
                "records_inserted": 0,
                "records_updated" : 0,
            }

        # ── Step 4: Connect to DB ──────────────────────────────
        conn = get_db_connection(db_config)

        # ── Step 5: Upsert inside a transaction ───────────────
        records_inserted, records_updated = upsert_posts(conn, posts)

        # ── Step 6: Write audit log (inside same transaction) ──
        write_ingestion_log(
            conn,
            records_fetched  = records_fetched,
            records_inserted = records_inserted,
            records_updated  = records_updated,
            status           = "success",
            dag_id           = dag_id,
            task_id          = task_id,
        )

        # ── Step 7: Commit everything atomically ───────────────
        conn.commit()
        logger.info("Transaction committed successfully.")

        result = {
            "status"          : "success",
            "records_fetched" : records_fetched,
            "records_inserted": records_inserted,
            "records_updated" : records_updated,
        }

    except EnvironmentError as exc:
        # Config is wrong — no point retrying without fixing .env
        logger.error(f"Configuration error: {exc}")
        result = _handle_failure(
            conn, exc, records_fetched, dag_id, task_id, "failed"
        )

    except (requests.exceptions.Timeout,
            requests.exceptions.ConnectionError) as exc:
        # Network issue — safe to retry
        logger.error(f"Network error fetching from API: {exc}")
        result = _handle_failure(
            conn, exc, records_fetched, dag_id, task_id, "failed"
        )

    except requests.exceptions.HTTPError as exc:
        logger.error(f"API returned an error response: {exc}")
        result = _handle_failure(
            conn, exc, records_fetched, dag_id, task_id, "failed"
        )

    except ValueError as exc:
        # Validation failed — API changed shape
        logger.error(f"API response validation failed: {exc}")
        result = _handle_failure(
            conn, exc, records_fetched, dag_id, task_id, "failed"
        )

    except psycopg2.Error as exc:
        logger.error(f"Database error: {exc}")
        result = _handle_failure(
            conn, exc, records_fetched, dag_id, task_id, "failed"
        )

    except Exception as exc:
        # Catch-all — unexpected errors should never silently swallow
        logger.exception(f"Unexpected error during ingestion: {exc}")
        result = _handle_failure(
            conn, exc, records_fetched, dag_id, task_id, "failed"
        )

    finally:
        # Always close the connection — even if commit/rollback failed
        if conn is not None:
            conn.close()
            logger.info("Database connection closed.")

    logger.info("=" * 60)
    logger.info(f"FIPSAR Ingestion Pipeline — END | {result}")
    logger.info("=" * 60)

    return result


def _handle_failure(
    conn            : psycopg2.extensions.connection | None,
    exc             : Exception,
    records_fetched : int,
    dag_id          : str | None,
    task_id         : str | None,
    status          : str,
) -> dict[str, Any]:
    """
    Shared failure handler called from every except block in run_ingestion.
    Rolls back the transaction, attempts to write a failure log,
    and returns a standardised result dict.

    Args:
        conn           : psycopg2 connection (may be None if DB never connected).
        exc            : The exception that triggered the failure.
        records_fetched: How many records we got before failing.
        dag_id         : Airflow DAG id (for log attribution).
        task_id        : Airflow task id (for log attribution).
        status         : 'failed' or 'partial'.

    Returns:
        Dict with keys: status, records_fetched, records_inserted=0,
                        records_updated=0, error.
    """
    error_message = str(exc)

    if conn is not None:
        try:
            conn.rollback()
            logger.info("Transaction rolled back.")

            # Write failure log in a fresh transaction (the previous one
            # was rolled back, so we need a new one for the log row)
            write_ingestion_log(
                conn,
                records_fetched  = records_fetched,
                records_inserted = 0,
                records_updated  = 0,
                status           = status,
                error_message    = error_message,
                dag_id           = dag_id,
                task_id          = task_id,
            )
            conn.commit()

        except Exception as log_exc:
            # If even the failure log fails, just log to file — don't crash
            logger.error(f"Could not write failure log to DB: {log_exc}")

    return {
        "status"          : status,
        "records_fetched" : records_fetched,
        "records_inserted": 0,
        "records_updated" : 0,
        "error"           : error_message,
    }


# =============================================================
# 6. ENTRY POINT
# =============================================================

if __name__ == "__main__":
    """
    Run the ingestion pipeline directly from the terminal.
    When called from an Airflow DAG, use run_ingestion() instead.
    """
    result = run_ingestion()

    # Exit with code 1 if the pipeline failed — useful for CI/CD checks
    # and for Airflow to detect failures when run as a BashOperator
    if result.get("status") != "success":
        sys.exit(1)
