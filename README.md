# OncoExtract: Clinical Abstraction Pipeline

An end-to-end data pipeline for extracting structured clinical variables from oncology literature. Ingests abstracts from PubMed, cleans with PySpark, models with dbt, extracts clinical variables using BioGPT, and provides a human-in-the-loop (HITL) review interface via Streamlit.

## Architecture

```mermaid
flowchart LR
    subgraph ingestion [Layer 1: Data Engineering]
        A[PubMed API] -->|E-utilities| B[Ingestion Script]
        B -->|Raw JSON| C[(PostgreSQL<br/>raw_pubmed)]
        C -->|boto3| S3[(AWS S3<br/>date-partitioned)]
        C -->|JDBC| D[PySpark Cleaning]
        D -->|Structured| E[(PostgreSQL<br/>cleaned_abstracts)]
    end

    subgraph analytics [Layer 2: Analytics Engineering]
        E --> F[dbt Staging]
        F --> G[dbt Intermediate]
        G --> H[dbt Marts]
    end

    subgraph ai [Layer 3: AI Engineering]
        E --> I[BioGPT Extraction]
        I -->|Pydantic validated| J[(PostgreSQL<br/>ai_extractions)]
        J --> K[Note Generation]
        K --> L[(PostgreSQL<br/>generated_notes)]
        J --> M[Streamlit HITL UI]
        M -->|Approve/Reject| J
    end

    subgraph orchestration [Orchestration]
        N[Dagster] -.->|schedules| B
        N -.->|schedules| D
        N -.->|schedules| I
        N -.->|schedules| K
    end
```

## Tech Stack

| Component | Tool | Purpose |
|:---|:---|:---|
| Ingestion | Python + requests | PubMed E-utilities API client |
| Processing | PySpark (Docker) | Text cleaning and normalization |
| Storage | PostgreSQL 16 | Raw, cleaned, and extracted data |
| Orchestration | Dagster | Pipeline scheduling and monitoring |
| Analytics | dbt | Staging, intermediate, and mart models |
| AI Extraction | BioGPT (HuggingFace) | Clinical variable extraction |
| Validation | Pydantic + custom metrics | Precision/recall/F1 per field |
| HITL Review | Streamlit | Human review and correction interface |
| Cloud Storage | AWS S3 | Raw data archival with date-partitioned keys |
| Infrastructure | Docker Compose | Multi-container orchestration (Postgres + Spark) |

## Quick Start

### Prerequisites

- Python 3.10+
- Docker (for PostgreSQL + Spark -- no local Java required)
- [uv](https://docs.astral.sh/uv/) package manager

### Setup

```bash
# Clone the repo
git clone https://github.com/your-username/oncoextract.git
cd oncoextract

# Start PostgreSQL + Spark (multi-container stack)
docker compose up -d

# Install dependencies
uv sync --all-extras

# Run all tests
uv run pytest
```

### Run the Pipeline

```bash
# Option 1: Run via Dagster UI
uv run dagster dev -m oncoextract.dagster_defs

# Option 2: Run individual steps
uv run python -m oncoextract.ingest.pubmed     # Ingest from PubMed
docker compose exec spark bash /app/spark-entrypoint.sh  # Clean with PySpark (in Docker)
uv run python -m oncoextract.ai.extract         # AI extraction
uv run python -m oncoextract.ai.summarize       # Generate notes
```

### Run dbt Models

```bash
cd dbt_oncoextract
uv run dbt build --profiles-dir . --project-dir .
uv run dbt docs generate --profiles-dir . --project-dir .
uv run dbt docs serve --profiles-dir . --project-dir .
```

### Launch HITL Review UI

```bash
uv run streamlit run streamlit_app/app.py
```

## Project Structure

```
oncoextract/
├── pyproject.toml              # Dependencies and project config
├── docker-compose.yml          # PostgreSQL + Spark containers
├── init.sql                    # Database schema with indexes
├── dagster.yaml                # Dagster configuration
│
├── oncoextract/
│   ├── ingest/pubmed.py        # PubMed API client with rate limiting
│   ├── spark/clean.py          # PySpark text cleaning job
│   ├── ai/extract.py           # BioGPT clinical extraction + rule-based fallback
│   ├── ai/summarize.py         # Clinical note generation + validation metrics
│   ├── dagster_defs/           # Dagster assets, jobs, definitions
│   └── db/models.py            # Database connection helpers
│
├── dbt_oncoextract/
│   ├── models/staging/         # stg_pubmed_abstracts
│   ├── models/intermediate/    # int_abstracts_parsed (biomarker/treatment flags)
│   └── models/marts/           # mart_cancer_studies, mart_treatment_outcomes
│
├── streamlit_app/app.py        # HITL review interface
│
└── tests/                      # 32 unit tests
    ├── test_ingest.py           # XML parsing tests
    ├── test_spark.py            # Text normalization tests
    ├── test_extract.py          # Extraction logic tests
    └── test_summarize.py        # Summarization + metrics tests
```

## Database Schema

| Table | Purpose |
|:---|:---|
| `raw_pubmed` | Raw JSON from PubMed API |
| `cleaned_abstracts` | Normalized text with GIN index on MeSH terms |
| `ai_extractions` | Structured clinical variables + confidence scores |
| `generated_notes` | AI-generated clinical summaries |
| `validation_runs` | Precision/recall/F1 metrics over time |

## Configuration

Copy `.env.example` to `.env` and fill in your PubMed API key:

```bash
cp .env.example .env
```

### PySpark via Docker

PySpark runs inside a `bitnami/spark:3.5` container -- no local Java or
Hadoop installation required. The Spark master UI is available at
`http://localhost:8080` when the stack is running.

```bash
# Start the full stack
docker compose up -d

# Run the Spark cleaning job
docker compose exec spark bash /app/spark-entrypoint.sh
```

When triggered via Dagster, the Spark job is automatically submitted to
the Docker container.

## License

MIT
