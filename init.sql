CREATE TABLE IF NOT EXISTS raw_pubmed (
    pmid TEXT PRIMARY KEY,
    raw_json JSONB NOT NULL,
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS cleaned_abstracts (
    pmid TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    abstract_text TEXT NOT NULL,
    authors JSONB DEFAULT '[]',
    pub_date DATE,
    mesh_terms JSONB DEFAULT '[]',
    journal TEXT,
    cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_cleaned_pub_date ON cleaned_abstracts (pub_date);
CREATE INDEX idx_cleaned_mesh_terms ON cleaned_abstracts USING GIN (mesh_terms);

CREATE TABLE IF NOT EXISTS ai_extractions (
    pmid TEXT PRIMARY KEY REFERENCES cleaned_abstracts(pmid),
    extracted_json JSONB NOT NULL,
    original_extracted_json JSONB,
    confidence_score FLOAT,
    model_version TEXT NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    human_verified BOOLEAN DEFAULT FALSE,
    reviewer_notes TEXT
);

CREATE TABLE IF NOT EXISTS generated_notes (
    pmid TEXT PRIMARY KEY REFERENCES cleaned_abstracts(pmid),
    summary_text TEXT NOT NULL,
    model_version TEXT NOT NULL,
    generated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS validation_runs (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    model_version TEXT NOT NULL,
    sample_size INTEGER NOT NULL,
    metrics JSONB NOT NULL
);
