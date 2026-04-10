"""Dagster job definitions for OncoExtract."""

from dagster import AssetSelection, define_asset_job

ingest_and_clean_job = define_asset_job(
    name="ingest_and_clean",
    selection=AssetSelection.assets("raw_pubmed_abstracts", "cleaned_abstracts"),
    description="Run ingestion from PubMed and PySpark cleaning.",
)

full_pipeline_job = define_asset_job(
    name="full_pipeline",
    selection=AssetSelection.all(),
    description="Run the entire pipeline: ingest, clean, extract, summarize.",
)
