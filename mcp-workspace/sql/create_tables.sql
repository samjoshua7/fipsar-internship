-- =============================================================
-- FIPSAR Internship — API Data Ingestion Pipeline
-- Schema: JSONPlaceholder Posts
-- Author: Joshua
-- Created via Claude MCP (Filesystem MCP Server)
-- =============================================================

-- -------------------------------------------------------------
-- TABLE: posts
-- Stores individual post records fetched from the API.
-- Source: https://jsonplaceholder.typicode.com/posts
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS posts (
    -- API's own post ID — used as PK to prevent duplicate inserts
    -- on re-runs (INSERT ... ON CONFLICT DO UPDATE pattern)
    id              INT             PRIMARY KEY,

    -- The user who authored the post (from API field "userId")
    -- Named snake_case to follow PostgreSQL conventions
    user_id         INT             NOT NULL,

    -- Post title — TEXT used instead of VARCHAR to avoid
    -- arbitrary length limits causing truncation errors
    title           TEXT            NOT NULL,

    -- Post body/content — same reasoning as title
    body            TEXT            NOT NULL,

    -- Pipeline audit: when did WE pull this record from the API
    -- TIMESTAMPTZ stores timezone-aware timestamps (UTC recommended)
    -- DEFAULT NOW() auto-fills on INSERT — no app code needed
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Pipeline audit: when was this row last updated by a re-ingest
    -- Nullable — NULL means the record has never been updated since first insert
    updated_at      TIMESTAMPTZ     NULL,

    -- Soft delete flag — if the API stops returning this post,
    -- we mark it deleted instead of hard-deleting (preserve history)
    is_deleted      BOOLEAN         NOT NULL DEFAULT FALSE
);

-- Index on user_id for fast lookups by user (common query pattern)
CREATE INDEX IF NOT EXISTS idx_posts_user_id
    ON posts (user_id);

-- Index on ingested_at for time-range queries and pipeline debugging
CREATE INDEX IF NOT EXISTS idx_posts_ingested_at
    ON posts (ingested_at);


-- -------------------------------------------------------------
-- TABLE: ingestion_logs
-- One row per pipeline run — tracks what happened each execution.
-- This is what tells you IF the pipeline ran, WHEN it ran,
-- HOW MANY records moved, and WHAT went wrong if it failed.
-- Most beginners skip this table. Don't.
-- -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion_logs (
    -- Surrogate PK — each run gets its own row
    id                  SERIAL          PRIMARY KEY,

    -- Timestamp of this pipeline execution
    run_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Total records the API returned in this run
    records_fetched     INT             NOT NULL DEFAULT 0,

    -- How many of those were brand new (inserted)
    records_inserted    INT             NOT NULL DEFAULT 0,

    -- How many already existed and were refreshed (updated)
    records_updated     INT             NOT NULL DEFAULT 0,

    -- Pipeline run outcome: 'success' | 'failed' | 'partial'
    -- VARCHAR(20) is fine here — values are controlled by app code
    status              VARCHAR(20)     NOT NULL,

    -- Only populated on failure — stores the exception/error message
    -- Nullable because success runs have nothing to put here
    error_message       TEXT            NULL,

    -- Which DAG/task wrote this log (useful when multiple pipelines
    -- share the same log table)
    dag_id              VARCHAR(100)    NULL,
    task_id             VARCHAR(100)    NULL
);

-- Index for filtering logs by status (e.g. "show me all failed runs")
CREATE INDEX IF NOT EXISTS idx_ingestion_logs_status
    ON ingestion_logs (status);

-- Index for time-range queries on logs
CREATE INDEX IF NOT EXISTS idx_ingestion_logs_run_at
    ON ingestion_logs (run_at);


-- =============================================================
-- END OF SCHEMA
-- Run this file once before starting the Airflow pipeline.
-- Command: psql -U <POSTGRES_USER> -d api_ingestion_mcp -f sql/create_tables.sql
-- =============================================================
