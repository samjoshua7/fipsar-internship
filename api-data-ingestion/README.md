# Real-Time API Data Ingestion Pipeline

## Overview

This project demonstrates an end-to-end data engineering workflow that consumes data from a public API, processes the response using Python, stores the data in PostgreSQL, and automates execution using Apache Airflow.

The workflow is containerized using Docker and executed within a Linux environment provided by WSL2.

## Assignment Objective

Simulate a real-time data ingestion pipeline by:

1. Consuming data from a public API
2. Processing JSON data using Python
3. Persisting data in PostgreSQL
4. Scheduling and orchestrating the workflow using Apache Airflow

## Architecture

```text
Public API
    ↓
Python Script
    ↓
PostgreSQL
    ↑
Apache Airflow
    ↓
Docker
    ↓
WSL2
```

## Technology Stack

| Component              | Technology     |
| ---------------------- | -------------- |
| Programming Language   | Python         |
| Database               | PostgreSQL     |
| Workflow Orchestration | Apache Airflow |
| Containerization       | Docker         |
| Linux Environment      | WSL2 (Ubuntu)  |
| Version Control        | Git            |
| IDE                    | VS Code        |

## Project Structure

```text
api-data-ingestion/
├── airflow/
│   ├── dags/
│   │   └── api_ingestion_dag.py
│   ├── logs/
│   ├── plugins/
│   ├── docker-compose.yaml
│   └── .env
├── dags/
├── docs/
├── logs/
├── scripts/
│   └── ingest_posts.py
├── sql/
│   └── create_tables.sql
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

## Data Flow

1. Apache Airflow triggers the DAG based on a schedule.
2. The DAG executes the Python ingestion script.
3. The Python script requests data from the public API.
4. The JSON response is parsed and validated.
5. Data is inserted into PostgreSQL.
6. Airflow logs execution status and retries failed tasks.

## API Details

API Endpoint:

```text
https://jsonplaceholder.typicode.com/posts
```

Sample Response:

```json
{
  "userId": 1,
  "id": 1,
  "title": "sample title",
  "body": "sample body"
}
```

## Database Schema

Table: `posts`

| Column     | Type      | Description                |
| ---------- | --------- | -------------------------- |
| id         | INTEGER   | Unique post identifier     |
| user_id    | INTEGER   | User identifier            |
| title      | TEXT      | Post title                 |
| body       | TEXT      | Post content               |
| fetched_at | TIMESTAMP | Record ingestion timestamp |

## Features

* REST API integration using Python
* JSON parsing and transformation
* PostgreSQL data persistence
* UPSERT operations using `ON CONFLICT`
* Environment variable management
* Airflow DAG scheduling
* Automatic retries on failures
* Dockerized deployment
* Structured logging

## Prerequisites

Install the following:

* Python 3.12 or later
* PostgreSQL
* WSL2 with Ubuntu
* Docker Desktop with WSL integration enabled
* Git

## Environment Variables

Create a `.env` file in the project root.

Example:

```env
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=api_ingestion
DB_USER=postgres
DB_PASSWORD=your_password
API_URL=https://jsonplaceholder.typicode.com/posts
```

## Setup Instructions

### 1. Clone the Repository

```bash
git clone <repository-url>
cd fipsar-internship/api-data-ingestion
```

### 2. Create the PostgreSQL Database

```sql
CREATE DATABASE api_ingestion;
```

Connect to the database and create the table:

```sql
CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 3. Start Docker Desktop

Ensure Docker Desktop is running and WSL integration is enabled.

Verify installation:

```bash
docker --version
docker compose version
```

### 4. Initialize Airflow

Navigate to the Airflow directory:

```bash
cd airflow
```

Initialize Airflow:

```bash
docker compose up airflow-init
```

### 5. Start Airflow Services

```bash
docker compose up -d
```

Verify containers:

```bash
docker ps
```

### 6. Access Airflow

Open:

```text
http://localhost:8080
```

Default credentials:

```text
Username: admin
Password: admin
```

## Running the Pipeline

Enable the DAG in the Airflow web interface.

Trigger the DAG manually or wait for the scheduled execution.

Monitor task logs from the Airflow UI.

## Verifying Data Ingestion

Connect to PostgreSQL:

```bash
psql -U postgres -d api_ingestion
```

Verify records:

```sql
SELECT COUNT(*) FROM posts;

SELECT * FROM posts LIMIT 10;
```

## Common Issues

### Airflow Does Not Run on Windows

Apache Airflow is designed for Linux environments.

Solution:

* Install WSL2
* Use Docker Desktop with WSL integration

### PostgreSQL Connection Refused

Problem:

```text
connection to server at "localhost" failed
```

Cause:

Containers cannot access services running on the host machine using `localhost`.

Solution:

Use:

```env
DB_HOST=host.docker.internal
```

### Logs Continuously Appear in Git

Ensure `.gitignore` excludes:

```text
airflow/logs/
*.log
```

Remove tracked files:

```bash
git rm -r --cached airflow/logs
```

## Future Improvements

* Add unit tests
* Add data quality checks
* Integrate alerting and notifications
* Add monitoring dashboards
* Deploy to a cloud environment
* Containerize the ingestion script independently

## Author

Sam Joshua C
Project for FIPSAR Internship
