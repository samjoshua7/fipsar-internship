# Project Documentation — FIPSAR API Data Ingestion Pipeline

## Overview

This project implements an automated API data ingestion pipeline as part of a 1-month internship at FIPSAR. The system fetches post records from the JSONPlaceholder public REST API on a recurring schedule, validates and transforms the data, and persists it to a PostgreSQL database using an UPSERT pattern that prevents duplicates across runs.

The pipeline is orchestrated by Apache Airflow and the entire stack runs in Docker containers — making it portable, reproducible, and environment-agnostic.

---

## Architecture

```
JSONPlaceholder API
        │
        │ HTTP GET /posts
        ▼
  ingest_posts.py          ← Python ingestion script
  (fetch → validate → upsert)
        │
        │ psycopg2 UPSERT
        ▼
  PostgreSQL (Docker)
  ├── posts               ← 100 post records
  └── ingestion_logs      ← one row per pipeline run
        ▲
        │ schedules every 5 min
  Apache Airflow
  ├── check_db_connection
  ├── ingest_posts
  └── log_run_summary
        ▲
        │ containerises all services
  Docker Compose
  ├── fipsar_postgres      (postgres:15-alpine, port 5433)
  ├── fipsar_airflow_init  (one-shot, exits after setup)
  ├── fipsar_airflow_webserver (port 8080)
  └── fipsar_airflow_scheduler
```

---

## Features

### UPSERT — no duplicates
The ingestion script uses PostgreSQL's `INSERT ... ON CONFLICT DO UPDATE` pattern. If the same post ID already exists, it updates only if the title or body has changed. Identical re-runs produce zero inserts and zero updates — not duplicate rows.

### Audit trail
Every pipeline execution writes one row to `ingestion_logs`, recording:
- How many records were fetched from the API
- How many were newly inserted
- How many were updated
- The run status (`success`, `failed`, or `partial`)
- The error message if the run failed
- Which Airflow DAG and task triggered the run

### Atomic transactions
The upsert and the log entry are committed in a single PostgreSQL transaction. If the database fails mid-run, both operations roll back together — the audit log never shows a success for a run that didn't complete.

### Schema validation
Before any database write, the script validates the API response — checking that the expected fields (`id`, `userId`, `title`, `body`) are present and of the correct types. If the API changes its shape, the pipeline fails loudly rather than silently writing bad data.

### Retry logic
Every Airflow task is configured with 3 retries and a 1-minute delay between attempts. The DB connection check task uses a shorter 30-second retry delay so transient startup issues resolve quickly.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `POSTGRES_HOST` | Yes | DB hostname — `postgres` inside Docker, `localhost` outside |
| `POSTGRES_PORT` | Yes | DB port — `5432` inside Docker network |
| `POSTGRES_DB` | Yes | Database name |
| `POSTGRES_USER` | Yes | Database user |
| `POSTGRES_PASSWORD` | Yes | Database password — no special URL characters |
| `AIRFLOW__CORE__FERNET_KEY` | Yes | Fernet key for Airflow secret encryption |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Yes | Secret key for Airflow UI session signing |
| `AIRFLOW_ADMIN_USER` | Yes | Airflow UI login username |
| `AIRFLOW_ADMIN_PASSWORD` | Yes | Airflow UI login password |
| `AIRFLOW_ADMIN_EMAIL` | Yes | Airflow UI admin email |
| `API_BASE_URL` | No | API base URL (default: `https://jsonplaceholder.typicode.com`) |
| `API_TIMEOUT` | No | Request timeout in seconds (default: `30`) |
| `LOG_LEVEL` | No | Python log level (default: `INFO`) |
| `LOG_DIR` | No | Log file directory (default: `./logs`) |

---

## Database Schema

### `posts` table

| Column | Type | Description |
|---|---|---|
| `id` | `INT PRIMARY KEY` | API post ID — source of truth |
| `user_id` | `INT NOT NULL` | Author user ID from API |
| `title` | `TEXT NOT NULL` | Post title |
| `body` | `TEXT NOT NULL` | Post body content |
| `ingested_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | When this record was first ingested |
| `updated_at` | `TIMESTAMPTZ NULL` | When this record was last updated by re-ingest |
| `is_deleted` | `BOOLEAN NOT NULL DEFAULT FALSE` | Soft delete flag |

**Indexes:** `idx_posts_user_id`, `idx_posts_ingested_at`

### `ingestion_logs` table

| Column | Type | Description |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | Auto-increment run ID |
| `run_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | Execution timestamp |
| `records_fetched` | `INT NOT NULL DEFAULT 0` | Total records from API |
| `records_inserted` | `INT NOT NULL DEFAULT 0` | New rows inserted |
| `records_updated` | `INT NOT NULL DEFAULT 0` | Existing rows updated |
| `status` | `VARCHAR(20) NOT NULL` | `success`, `failed`, or `partial` |
| `error_message` | `TEXT NULL` | Exception detail on failure |
| `dag_id` | `VARCHAR(100) NULL` | Airflow DAG identifier |
| `task_id` | `VARCHAR(100) NULL` | Airflow task identifier |

**Indexes:** `idx_ingestion_logs_status`, `idx_ingestion_logs_run_at`

---

## Airflow DAG

**DAG ID:** `fipsar_ingest_posts`
**Schedule:** `*/5 * * * *` (every 5 minutes)
**Max active runs:** 1 (prevents parallel double-writes)
**Catchup:** disabled

### Task flow

```
check_db_connection → ingest_posts → log_run_summary
```

| Task | Operator | Retries | Purpose |
|---|---|---|---|
| `check_db_connection` | PythonOperator | 3 × 30s | Ping PostgreSQL before doing any real work |
| `ingest_posts` | PythonOperator | 3 × 60s | Fetch API + UPSERT + write audit log |
| `log_run_summary` | PythonOperator | 0 | Print counts from XCom to task log |

`log_run_summary` uses `trigger_rule="all_done"` — it runs even if `ingest_posts` fails, so there is always a run summary in the logs.

---

## Docker Services

| Container | Image | Port | Purpose |
|---|---|---|---|
| `fipsar_postgres` | `postgres:15-alpine` | 5433 → 5432 | Data store |
| `fipsar_airflow_init` | Custom (Dockerfile) | — | One-shot DB init + admin user creation |
| `fipsar_airflow_webserver` | Custom (Dockerfile) | 8080 → 8080 | Airflow UI |
| `fipsar_airflow_scheduler` | Custom (Dockerfile) | — | DAG scheduling engine |

The custom Airflow image extends `apache/airflow:2.9.1-python3.11` and adds `psycopg2-binary`, `requests`, and `python-dotenv` — dependencies required by the ingestion script that are not included in the base image.

All Airflow containers share a YAML anchor (`x-airflow-env`) so environment variables are defined once and inherited — no duplication.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `WARN: variable not set, defaulting to blank` | `--env-file .env` not passed | Add `--env-file .env` to the compose command |
| `connection refused` on port 5432 | Windows PostgreSQL port conflict | Stack uses port 5433 externally — connect via `localhost:5433` |
| `password authentication failed` | Special chars in password (`@`, `!`) | Use alphanumeric-only passwords |
| DAG not appearing in Airflow UI | Import error in DAG file | Check `docker logs fipsar_airflow_scheduler` for traceback |
| `403 Forbidden` on REST API | `AUTH_BACKENDS` not set | Ensure `AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth` |
| `airflow-init` exits with code 127 | Multiline bash command parse error | Keep the `command:` on a single line in compose |

---

## Future Improvements

- Add unit tests for `fetch_posts`, `_validate_post_schema`, and `upsert_posts`
- Add integration tests using a test PostgreSQL instance
- Support additional JSONPlaceholder endpoints (`/users`, `/comments`)
- Implement soft-delete detection — mark posts removed from API as `is_deleted = true`
- Add Airflow email/Slack alerting on DAG failure
- Replace LocalExecutor with CeleryExecutor for horizontal scaling
- Add data quality checks using Great Expectations or dbt tests
- Parameterise the API endpoint so the DAG can ingest from any REST source

---

*FIPSAR Internship — Joshua, 2026*
