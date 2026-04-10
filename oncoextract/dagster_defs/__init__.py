"""Dagster definitions entry point for OncoExtract."""

from dagster import Definitions

from oncoextract.dagster_defs.assets import (
    ai_extractions,
    cleaned_abstracts,
    generated_notes,
    raw_pubmed_abstracts,
    s3_raw_archive,
)
from oncoextract.dagster_defs.jobs import full_pipeline_job, ingest_and_clean_job

defs = Definitions(
    assets=[
        raw_pubmed_abstracts,
        s3_raw_archive,
        cleaned_abstracts,
        ai_extractions,
        generated_notes,
    ],
    jobs=[ingest_and_clean_job, full_pipeline_job],
)
