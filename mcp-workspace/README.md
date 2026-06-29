# FIPSAR Internship — API Data Ingestion Pipeline

A production-style API data ingestion pipeline built during a 1-month internship at FIPSAR. Fetches post data from the JSONPlaceholder REST API, validates and upserts it into PostgreSQL, and orchestrates the entire workflow with Apache Airflow — all containerised with Docker.

---

## Features

- Scheduled ingestion every 5 minutes via Airflow DAG
- UPSERT logic — no duplicate records across pipeline runs
- Full audit trail via `ingestion_logs` table
- Structured exception handling with per-run failure logging
- Docker-first setup — one command to start the entire stack
- Environment-variable-driven configuration — no hardcoded secrets

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.1 |
| Language | Python 3.11 |
| Database | PostgreSQL 15 |
| Containerisation | Docker + Docker Compose |
| API Source | JSONPlaceholder (REST) |
| Runtime Environment | WSL2 / Ubuntu on Windows |

---

## Project Structure

```
mcp-workspace/
├── dags/
│   └── ingest_posts_dag.py     # Airflow DAG — 3-task pipeline
├── scripts/
│   └── ingest_posts.py         # Core ingestion logic
├── sql/
│   └── create_tables.sql       # PostgreSQL schema
├── docker/
│   ├── Dockerfile              # Custom Airflow image
│   └── docker-compose.yml      # Full stack definition
├── config/                     # Reserved for non-secret config
├── docs/                       # Extended documentation
├── logs/                       # Runtime logs (gitignored)
├── tests/                      # Test suite (pytest)
├── .env.example                # Environment variable template
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker Desktop (running)
- WSL2 with Ubuntu
- Git

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/samjoshua7/fipsar-internship.git
cd fipsar-internship/mcp-workspace

# 2. Create your environment file
cp .env.example .env
# Edit .env and fill in your values (see Configuration section)

# 3. Start the full stack
docker compose -f docker/docker-compose.yml --env-file .env up -d --build

# 4. Wait ~60 seconds, then open the Airflow UI
# http://localhost:8080
# Login with your AIRFLOW_ADMIN_USER / AIRFLOW_ADMIN_PASSWORD from .env
```

### Stop the stack

```bash
# Stop containers (data preserved)
docker compose -f docker/docker-compose.yml down

# Stop and wipe all data (full reset)
docker compose -f docker/docker-compose.yml down -v
```

---

## Configuration

All configuration is done via environment variables. Copy `.env.example` to `.env` and fill in your values. The `.env` file is gitignored and must never be committed.

### Generating security keys

```bash
# Fernet key (Airflow encrypts stored secrets with this)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Web secret key (signs Airflow UI session cookies)
python -c "import secrets; print(secrets.token_hex(32))"
```

---

## Airflow UI

| URL | `http://localhost:8080` |
|---|---|
| DAG ID | `fipsar_ingest_posts` |
| Schedule | Every 5 minutes (`*/5 * * * *`) |
| Tasks | `check_db_connection` → `ingest_posts` → `log_run_summary` |

---

## Verify data ingestion

```bash
# Connect to Docker PostgreSQL (port 5433)
docker exec fipsar_postgres psql -U <POSTGRES_USER> -d api_ingestion_mcp -c \
  "SELECT COUNT(*) FROM posts; SELECT * FROM ingestion_logs ORDER BY run_at DESC LIMIT 5;"
```

---

## Author

Joshua — FIPSAR Internship, 2026
