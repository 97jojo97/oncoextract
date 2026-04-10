"""Dagster asset definitions for the OncoExtract pipeline."""

import logging

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

logger = logging.getLogger(__name__)


@asset(
    group_name="ingestion",
    description="Fetch NPC abstracts from PubMed and store raw JSON in Postgres.",
    kinds={"python", "postgres"},
)
def raw_pubmed_abstracts(context: AssetExecutionContext) -> MaterializeResult:
    from oncoextract.ingest.pubmed import ingest_to_postgres

    count = ingest_to_postgres(query="Nasopharyngeal Carcinoma", max_results=5000)
    context.log.info("Ingested %d new articles", count)

    return MaterializeResult(
        metadata={
            "articles_ingested": MetadataValue.int(count),
        }
    )


@asset(
    group_name="ingestion",
    deps=[raw_pubmed_abstracts],
    description="Archive raw PubMed JSON to S3 with date-partitioned keys.",
    kinds={"python", "s3"},
)
def s3_raw_archive(context: AssetExecutionContext) -> MaterializeResult:
    from oncoextract.ingest.s3 import archive_to_s3

    count = archive_to_s3()
    context.log.info("Archived %d records to S3", count)

    return MaterializeResult(
        metadata={
            "records_archived": MetadataValue.int(count),
        }
    )


@asset(
    group_name="cleaning",
    deps=[raw_pubmed_abstracts],
    description="Clean raw PubMed data using PySpark (in Docker) and write to cleaned_abstracts.",
    kinds={"pyspark", "docker", "postgres"},
)
def cleaned_abstracts(context: AssetExecutionContext) -> MaterializeResult:
    from oncoextract.spark.clean import run_cleaning_in_docker

    count = run_cleaning_in_docker()
    context.log.info("Cleaned %d records", count)

    return MaterializeResult(
        metadata={
            "records_cleaned": MetadataValue.int(count),
        }
    )


@asset(
    group_name="ai",
    deps=[cleaned_abstracts],
    description="Run LLM-based extraction on cleaned abstracts.",
    kinds={"python", "huggingface"},
)
def ai_extractions(context: AssetExecutionContext) -> MaterializeResult:
    from oncoextract.ai.extract import run_extraction

    count = run_extraction()
    context.log.info("Extracted variables from %d abstracts", count)

    return MaterializeResult(
        metadata={
            "abstracts_processed": MetadataValue.int(count),
        }
    )


@asset(
    group_name="ai",
    deps=[ai_extractions],
    description="Generate clinical summary notes from AI extractions.",
    kinds={"python", "huggingface"},
)
def generated_notes(context: AssetExecutionContext) -> MaterializeResult:
    from oncoextract.ai.summarize import run_summarization

    count = run_summarization()
    context.log.info("Generated notes for %d abstracts", count)

    return MaterializeResult(
        metadata={
            "notes_generated": MetadataValue.int(count),
        }
    )
