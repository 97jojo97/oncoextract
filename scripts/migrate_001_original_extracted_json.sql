-- Run once if your database was created before original_extracted_json existed:
--   docker compose exec -T postgres psql -U oncoextract -d oncoextract -f scripts/migrate_001_original_extracted_json.sql

ALTER TABLE ai_extractions ADD COLUMN IF NOT EXISTS original_extracted_json JSONB;

UPDATE ai_extractions
SET original_extracted_json = extracted_json
WHERE original_extracted_json IS NULL;
