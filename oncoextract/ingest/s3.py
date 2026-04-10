"""S3 archival for raw PubMed data."""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
from dotenv import load_dotenv
from sqlalchemy import text

from oncoextract.db.models import get_engine

load_dotenv()

logger = logging.getLogger(__name__)


def get_s3_client():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def archive_to_s3(bucket: str | None = None) -> int:
    """Upload raw PubMed records to S3 as date-partitioned JSON files.

    Structure: s3://{bucket}/raw/pubmed/{YYYY-MM-DD}/{pmid}.json
    """
    bucket = bucket or os.getenv("S3_BUCKET", "oncoextract-data")
    engine = get_engine()
    s3 = get_s3_client()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT pmid, raw_json FROM raw_pubmed ORDER BY pmid"
        )).fetchall()

    logger.info("Archiving %d records to s3://%s/raw/pubmed/%s/", len(rows), bucket, today)
    uploaded = 0

    for pmid, raw_json in rows:
        key = f"raw/pubmed/{today}/{pmid}.json"
        body = raw_json if isinstance(raw_json, str) else json.dumps(raw_json)
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
        uploaded += 1

        if uploaded % 500 == 0:
            logger.info("Uploaded %d/%d to S3", uploaded, len(rows))

    logger.info("Archived %d records to s3://%s/raw/pubmed/%s/", uploaded, bucket, today)
    return uploaded


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = archive_to_s3()
    print(f"Archived {count} records to S3")
