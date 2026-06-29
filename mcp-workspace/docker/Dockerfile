# =============================================================
# FIPSAR Internship — Custom Airflow Image
# Base : apache/airflow:2.9.1-python3.11
# Adds : psycopg2-binary, requests, python-dotenv
#        (required by scripts/ingest_posts.py)
#
# Why a custom image instead of pip install at runtime:
#   - The base Airflow image doesn't include psycopg2 or requests
#   - Installing at runtime (entrypoint) slows every container start
#   - Baking into the image = fast, reproducible, production-correct
# =============================================================

FROM apache/airflow:2.9.1-python3.11

# Switch to airflow user (required by the base image — never use root)
USER airflow

# Install only what ingest_posts.py needs
# --no-cache-dir keeps the image smaller
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    requests==2.31.0 \
    python-dotenv==1.0.1
