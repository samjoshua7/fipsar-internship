# Technical Documentation — FIPSAR API Data Ingestion Pipeline

## scripts/ingest_posts.py

### Module structure

| Section | Purpose |
|---|---|
| Section 0 | `.env` loading via `python-dotenv` |
| Section 1 | Logger setup — stdout + daily rotating file |
| Section 2 | Config helpers — read and validate env vars |
| Section 3 | API fetch + response schema validation |
| Section 4 | PostgreSQL operations — connect, upsert, log |
| Section 5 | Main orchestrator — `run_ingestion()` |
| Section 6 | `__main__` entry point |

### Key design decisions

#### `run_ingestion()` never raises
The top-level function catches every exception category explicitly, logs it, writes a `'failed'` row to `ingestion_logs`, and returns a result dict. Airflow reads the return value and raises `RuntimeError` if `status != 'success'` — this separation keeps the script usable both standalone and from the DAG.

#### UPSERT with `xmax` trick
```sql
INSERT INTO posts (id, user_id, title, body)
VALUES %s
ON CONFLICT (id) DO UPDATE
    SET title = EXCLUDED.title, body = EXCLUDED.body, updated_at = NOW()
    WHERE posts.title IS DISTINCT FROM EXCLUDED.title
       OR posts.body  IS DISTINCT FROM EXCLUDED.body
RETURNING id, (xmax = 0) AS was_inserted
```
`xmax = 0` on a returned row means it was freshly inserted. `xmax > 0` means it was an existing row that got updated. This lets us count inserts vs updates in a single statement without a second query.

The `WHERE` clause on `DO UPDATE` means unchanged rows do not trigger an update — `updated_at` only bumps when the data actually changed.

#### Atomic transaction for upsert + audit log
Both `upsert_posts()` and `write_ingestion_log()` execute inside the same open connection before `conn.commit()`. If PostgreSQL fails between the two, both roll back. The audit log never records a success for a run that didn't actually complete.

#### Failure log in a new transaction
When `_handle_failure()` is called, the current transaction is already rolled back. The function opens a new implicit transaction to write the `'failed'` log row, then commits that separately. This ensures failures are always recorded even when the main transaction died.

#### `execute_values` for batch insert
`psycopg2.extras.execute_values()` sends all rows in a single round-trip with a parameterised `VALUES` clause. For 100 rows this is trivially faster; for larger datasets it avoids N×latency.

---

## dags/ingest_posts_dag.py

### sys.path injection
```python
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
```
Airflow's DagFileProcessor runs DAG files from its own working directory. Without this, `from scripts.ingest_posts import run_ingestion` raises `ModuleNotFoundError`. The guard (`if not in sys.path`) prevents duplicate entries on repeated imports.

### XCom usage
`task_ingest_posts` pushes the result dict to XCom:
```python
context["ti"].xcom_push(key="ingestion_result", value=result)
```
`task_log_run_summary` pulls it back:
```python
result = context["ti"].xcom_pull(task_ids="ingest_posts", key="ingestion_result")
```
XCom values are stored in Airflow's metadata database. This is appropriate for small dicts — not for large datasets.

### `trigger_rule="all_done"`
The summary task sets `trigger_rule="all_done"` so it executes regardless of whether `ingest_posts` succeeded or failed. Default trigger rule is `"all_success"` which would skip the summary on failure — losing the run summary from the Airflow log view.

### `provide_context=True`
Required in Airflow 2.x for PythonOperator tasks that use `**context`. Without it, the function receives no keyword arguments and `context["dag"]`, `context["ti"]`, etc. raise `KeyError`.

---

## docker/docker-compose.yml

### YAML anchor pattern
```yaml
x-airflow-env: &airflow-env
  AIRFLOW__CORE__EXECUTOR: LocalExecutor
  ...

services:
  airflow-webserver:
    environment:
      <<: *airflow-env
```
`x-airflow-env` is a YAML extension field (prefix `x-`) that Docker Compose ignores as a service definition but keeps available for anchoring. `&airflow-env` names the anchor; `<<: *airflow-env` merges it. This defines all Airflow env vars once — changes propagate to all three services automatically.

### Port mapping — 5433:5432
The host port is `5433` (not `5432`) to avoid collision with any locally installed PostgreSQL instance. Internal Docker networking still uses port `5432` — containers communicate via service name `postgres:5432` on `fipsar_net`.

### `initdb.d` for schema auto-creation
```yaml
volumes:
  - ../sql/create_tables.sql:/docker-entrypoint-initdb.d/01_create_tables.sql:ro
```
PostgreSQL's Docker image runs all `.sql` files in `/docker-entrypoint-initdb.d/` on first container initialisation (when the data volume is empty). This creates `posts` and `ingestion_logs` automatically — no manual `psql` command needed after `docker compose up`.

### `service_completed_successfully` condition
```yaml
depends_on:
  airflow-init:
    condition: service_completed_successfully
```
Airflow's webserver and scheduler only start after `airflow-init` exits with code 0. If init fails (e.g. wrong DB password), the dependent services never start — preventing a cascade of misleading errors.

---

## sql/create_tables.sql

### `TEXT` vs `VARCHAR(n)`
`title` and `body` use `TEXT` (unlimited length) rather than `VARCHAR(255)`. JSONPlaceholder posts have variable-length content, and an arbitrary length cap risks truncation errors if the API returns longer strings in future. PostgreSQL stores `TEXT` and `VARCHAR` identically — there is no performance difference.

### `TIMESTAMPTZ` vs `TIMESTAMP`
All timestamp columns use `TIMESTAMPTZ` (timezone-aware). PostgreSQL stores these internally as UTC and converts on read based on the session timezone. Using `TIMESTAMP` (timezone-naive) is a common source of bugs when the application server and database server are in different timezones.

### `IF NOT EXISTS` on all DDL
All `CREATE TABLE` and `CREATE INDEX` statements use `IF NOT EXISTS`. This makes the script idempotent — safe to re-run without error if the schema already exists.

---

## docker/Dockerfile

```dockerfile
FROM apache/airflow:2.9.1-python3.11
USER airflow
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    requests==2.31.0 \
    python-dotenv==1.0.1
```

- `USER airflow` — the base image requires pip installs to run as the `airflow` user, not root. Running as root causes permission errors on image startup.
- `--no-cache-dir` — pip's download cache is not needed in an image layer. Omitting it keeps the image smaller.
- Pinned versions — exact versions prevent silent breakage when upstream packages release incompatible changes.

---

## Security notes

| Item | Approach |
|---|---|
| DB credentials | Environment variables only — never in source code |
| Airflow Fernet key | Environment variable — regenerate per deployment |
| Airflow secret key | Environment variable — regenerate per deployment |
| `.env` file | Listed in `.gitignore` — never committed |
| Passwords | Alphanumeric only — special chars break URL-encoded connection strings |
| Airflow admin password | Environment variable (`AIRFLOW_ADMIN_PASSWORD`) |

---

*FIPSAR Internship — Joshua, 2026*
