#!/bin/bash
# Runs the PySpark cleaning job inside the apache/spark-py Docker container.
# Called via: docker compose exec -T spark bash /app/spark-entrypoint.sh

export PYTHONPATH="/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
# DB host/user/password come from /app/.env (mounted). For local Docker Postgres use POSTGRES_HOST=postgres.

pip install --quiet python-dotenv psycopg2-binary sqlalchemy 2>/dev/null

cd /app
python3 -c "
import sys, logging
sys.path.insert(0, '/app')
logging.basicConfig(level=logging.INFO)
from oncoextract.spark.clean import run_cleaning_job
count = run_cleaning_job()
print(f'Cleaned {count} records')
"
