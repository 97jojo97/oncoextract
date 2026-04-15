"""PySpark job to clean and normalize raw PubMed data.

Designed to run inside the bitnami/spark Docker container where the
PostgreSQL JDBC driver is fetched automatically via spark.jars.packages.
Can also be triggered from the host via run_cleaning_in_docker().
"""

from __future__ import annotations

import html
import logging
import os
import re
import subprocess

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

from oncoextract.db.models import get_jdbc_url, postgres_sslmode
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

load_dotenv()
logger = logging.getLogger(__name__)

JDBC_DRIVER = "org.postgresql.Driver"
PG_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.3"


def get_spark_session() -> SparkSession:
    """Create a local-mode Spark session with PostgreSQL JDBC driver."""
    return (
        SparkSession.builder
        .appName("OncoExtract-Clean")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.packages", PG_JDBC_PACKAGE)
        .getOrCreate()
    )


def run_cleaning_in_docker() -> int:
    """Execute the Spark cleaning job inside the Docker Spark container.

    This is the primary entry point when called from Dagster or the CLI
    on the host machine. The actual PySpark logic runs inside the
    bitnami/spark container so no local Java/Hadoop is required.
    """
    logger.info("Submitting Spark cleaning job to Docker container...")
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "spark",
            "bash", "/app/spark-entrypoint.sh",
        ],
        capture_output=True,
        text=True,
        timeout=600,
    )
    if result.returncode != 0:
        logger.error("Spark job failed:\n%s", result.stderr)
        raise RuntimeError(f"Spark Docker job failed: {result.stderr[-500:]}")

    for line in result.stdout.splitlines():
        if line.startswith("Cleaned "):
            count = int(line.split()[1])
            logger.info("Spark Docker job completed: %d records", count)
            return count

    logger.warning("Could not parse record count from Spark output")
    return 0


def read_raw_pubmed(spark: SparkSession) -> DataFrame:
    """Read raw_pubmed table from Postgres via JDBC."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    user = os.getenv("POSTGRES_USER", "oncoextract")
    password = os.getenv("POSTGRES_PASSWORD", "oncoextract_dev")

    jdbc_url = get_jdbc_url()

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "raw_pubmed")
        .option("user", user)
        .option("password", password)
        .option("driver", JDBC_DRIVER)
        .load()
    )


def normalize_text(text: str | None) -> str:
    """Clean a text field: decode HTML entities, normalize whitespace."""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_and_clean(df: DataFrame) -> DataFrame:
    """Extract structured fields from raw JSONB and normalize text."""
    normalize_udf = F.udf(normalize_text, StringType())

    parsed = df.select(
        F.col("pmid"),
        F.get_json_object("raw_json", "$.title").alias("title_raw"),
        F.get_json_object("raw_json", "$.abstract_text").alias("abstract_raw"),
        F.get_json_object("raw_json", "$.authors").alias("authors_raw"),
        F.get_json_object("raw_json", "$.pub_date").alias("pub_date_raw"),
        F.get_json_object("raw_json", "$.mesh_terms").alias("mesh_terms_raw"),
        F.get_json_object("raw_json", "$.journal").alias("journal_raw"),
    )

    cleaned = parsed.select(
        F.col("pmid"),
        normalize_udf(F.col("title_raw")).alias("title"),
        normalize_udf(F.col("abstract_raw")).alias("abstract_text"),
        F.col("authors_raw").alias("authors"),
        F.to_date(F.col("pub_date_raw")).alias("pub_date"),
        F.col("mesh_terms_raw").alias("mesh_terms"),
        normalize_udf(F.col("journal_raw")).alias("journal"),
        F.current_timestamp().alias("cleaned_at"),
    )

    # Filter out records with no abstract
    return cleaned.filter(
        (F.col("abstract_text").isNotNull()) & (F.length(F.col("abstract_text")) > 10)
    )


def _delete_existing_for_pmids(pmids: list[str]) -> None:
    """Remove dependent rows so JDBC append can replace cleaned rows on re-runs."""
    if not pmids:
        return
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "oncoextract")
    user = os.getenv("POSTGRES_USER", "oncoextract")
    password = os.getenv("POSTGRES_PASSWORD", "oncoextract_dev")
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password,
        sslmode=postgres_sslmode(),
    )
    try:
        with conn.cursor() as cur:
            # Children reference cleaned_abstracts(pmid); delete in FK-safe order.
            cur.execute(
                "DELETE FROM generated_notes WHERE pmid = ANY(%s::text[])",
                (pmids,),
            )
            cur.execute(
                "DELETE FROM ai_extractions WHERE pmid = ANY(%s::text[])",
                (pmids,),
            )
            cur.execute(
                "DELETE FROM cleaned_abstracts WHERE pmid = ANY(%s::text[])",
                (pmids,),
            )
        conn.commit()
    finally:
        conn.close()


def write_cleaned(df: DataFrame) -> None:
    """Write cleaned data back to Postgres."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "oncoextract")
    user = os.getenv("POSTGRES_USER", "oncoextract")
    password = os.getenv("POSTGRES_PASSWORD", "oncoextract_dev")

    pmids = [r["pmid"] for r in df.select("pmid").distinct().collect()]
    _delete_existing_for_pmids(pmids)

    base = get_jdbc_url()
    sep = "&" if "?" in base else "?"
    jdbc_url = f"{base}{sep}stringtype=unspecified"

    (
        df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "cleaned_abstracts")
        .option("user", user)
        .option("password", password)
        .option("driver", JDBC_DRIVER)
        .mode("append")
        .save()
    )


def run_cleaning_job() -> int:
    """Execute the full cleaning pipeline. Returns number of cleaned records."""
    spark = get_spark_session()
    try:
        raw_df = read_raw_pubmed(spark)
        raw_count = raw_df.count()
        logger.info("Read %d raw records from Postgres", raw_count)

        if raw_count == 0:
            logger.info("No raw records to clean")
            return 0

        cleaned_df = parse_and_clean(raw_df)
        cleaned_count = cleaned_df.count()
        logger.info("Cleaned %d records (filtered from %d)", cleaned_count, raw_count)

        write_cleaned(cleaned_df)
        logger.info("Wrote %d cleaned records to Postgres", cleaned_count)
        return cleaned_count
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = run_cleaning_job()
    print(f"Cleaned {count} records")
