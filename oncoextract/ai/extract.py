"""LLM-based clinical variable extraction from oncology abstracts."""

import json
import logging
import re
from typing import Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field
from sqlalchemy import text

from oncoextract.db.models import get_engine

load_dotenv()

logger = logging.getLogger(__name__)

MODEL_NAME = "microsoft/biogpt"
MODEL_VERSION = "biogpt-v1"
BATCH_SIZE = 50


class ClinicalExtraction(BaseModel):
    """Structured output from clinical variable extraction."""

    tnm_stage: str | None = Field(None, description="TNM stage (e.g., T2N1M0, Stage III)")
    treatment_modality: list[str] = Field(
        default_factory=list,
        description="Treatment types mentioned (chemotherapy, radiation, immunotherapy, surgery)",
    )
    biomarkers: list[str] = Field(
        default_factory=list,
        description="Biomarkers mentioned (EBV DNA, PD-L1, p53, etc.)",
    )
    sample_size: int | None = Field(None, description="Number of patients/subjects if mentioned")
    cancer_type: str | None = Field(None, description="Specific cancer type mentioned")


def _build_extraction_prompt(abstract_text: str) -> str:
    return (
        "Extract clinical variables from the following oncology abstract. "
        "Return a JSON object with these fields:\n"
        '- "tnm_stage": TNM stage or clinical stage (e.g., "Stage III", "T2N1M0") or null\n'
        '- "treatment_modality": list of treatments (e.g., ["chemotherapy", "radiation"])\n'
        '- "biomarkers": list of biomarkers mentioned (e.g., ["EBV DNA", "PD-L1"])\n'
        '- "sample_size": number of patients/subjects or null\n'
        '- "cancer_type": specific cancer type or null\n\n'
        f"Abstract: {abstract_text[:1500]}\n\n"
        "JSON output:"
    )


def _rule_based_extraction(abstract_text: str) -> dict[str, Any]:
    """Fallback rule-based extraction using regex patterns."""
    text_lower = abstract_text.lower()
    result: dict[str, Any] = {
        "tnm_stage": None,
        "treatment_modality": [],
        "biomarkers": [],
        "sample_size": None,
        "cancer_type": None,
    }

    # TNM stage detection
    tnm_match = re.search(r"(T[0-4][a-b]?N[0-3]M[0-1])", abstract_text)
    if tnm_match:
        result["tnm_stage"] = tnm_match.group(1)
    else:
        stage_match = re.search(r"stage\s+(I{1,3}V?[AB]?|[1-4][AB]?)", abstract_text, re.IGNORECASE)
        if stage_match:
            result["tnm_stage"] = f"Stage {stage_match.group(1).upper()}"

    # Treatment modalities
    treatments = []
    chemo_terms = ["chemotherapy", "cisplatin", "carboplatin", "gemcitabine", "docetaxel", "5-fu"]
    if any(t in text_lower for t in chemo_terms):
        treatments.append("chemotherapy")
    rad_terms = ["radiation", "radiotherapy", "imrt", "proton therapy"]
    if any(t in text_lower for t in rad_terms):
        treatments.append("radiation")
    immuno_terms = [
        "immunotherapy", "checkpoint inhibitor", "pembrolizumab", "nivolumab", "anti-pd",
    ]
    if any(t in text_lower for t in immuno_terms):
        treatments.append("immunotherapy")
    if any(t in text_lower for t in ["surgery", "surgical", "resection", "dissection"]):
        treatments.append("surgery")
    result["treatment_modality"] = treatments

    # Biomarkers
    biomarkers = []
    if any(b in text_lower for b in ["ebv", "epstein-barr", "ebv dna"]):
        biomarkers.append("EBV DNA")
    if any(b in text_lower for b in ["pd-l1", "programmed death-ligand"]):
        biomarkers.append("PD-L1")
    if any(b in text_lower for b in ["p53", "tp53"]):
        biomarkers.append("p53")
    if any(b in text_lower for b in ["vegf", "vascular endothelial"]):
        biomarkers.append("VEGF")
    if any(b in text_lower for b in ["her2", "erbb2"]):
        biomarkers.append("HER2")
    result["biomarkers"] = biomarkers

    # Sample size
    size_match = re.search(
        r"(\d{1,5})\s*(?:patients|subjects|cases|participants|individuals)",
        abstract_text,
        re.IGNORECASE,
    )
    if size_match:
        result["sample_size"] = int(size_match.group(1))

    # Cancer type
    if "nasopharyngeal" in text_lower:
        result["cancer_type"] = "Nasopharyngeal Carcinoma"
    elif "lung" in text_lower and ("cancer" in text_lower or "carcinoma" in text_lower):
        result["cancer_type"] = "Lung Cancer"
    elif "breast" in text_lower and ("cancer" in text_lower or "carcinoma" in text_lower):
        result["cancer_type"] = "Breast Cancer"

    return result


def _parse_llm_output(raw_output: str, abstract_text: str) -> dict[str, Any]:
    """Parse LLM output into a structured dict, falling back to rule-based."""
    json_match = re.search(r"\{[^{}]*\}", raw_output, re.DOTALL)
    if json_match:
        try:
            parsed = json.loads(json_match.group())
            extraction = ClinicalExtraction(**parsed)
            return extraction.model_dump()
        except (json.JSONDecodeError, Exception):
            pass

    return _rule_based_extraction(abstract_text)


def _compute_confidence(extraction: dict[str, Any]) -> float:
    """Heuristic confidence score based on how many fields were extracted."""
    score = 0.0
    total_fields = 5
    if extraction.get("tnm_stage"):
        score += 1
    if extraction.get("treatment_modality"):
        score += 1
    if extraction.get("biomarkers"):
        score += 1
    if extraction.get("sample_size"):
        score += 1
    if extraction.get("cancer_type"):
        score += 1
    return round(score / total_fields, 2)


class ClinicalExtractor:
    """Extracts clinical variables using BioGPT with rule-based fallback."""

    def __init__(self, use_gpu: bool = True):
        self._use_gpu = use_gpu
        self.device = "cpu"
        self.model = None
        self.tokenizer = None
        self.generator = None

    def load_model(self) -> None:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer, pipeline

        self.device = "cuda" if self._use_gpu and torch.cuda.is_available() else "cpu"
        logger.info("Loading %s on %s", MODEL_NAME, self.device)
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        if self.tokenizer.pad_token_id is None:
            self.tokenizer.pad_token_id = self.tokenizer.eos_token_id
        self.model = AutoModelForCausalLM.from_pretrained(MODEL_NAME).to(self.device)
        # Avoid passing max_new_tokens here: it clashes with tokenizer default max_length in
        # generate() and spams warnings; pass max_new_tokens only on each call instead.
        self.generator = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
            device=0 if self.device == "cuda" else -1,
        )
        logger.info("Model loaded successfully")

    def extract(self, abstract_text: str) -> tuple[dict[str, Any], float]:
        """Extract clinical variables from an abstract.

        Returns (extraction_dict, confidence_score).
        """
        if not abstract_text or len(abstract_text) < 20:
            empty = ClinicalExtraction().model_dump()
            return empty, 0.0

        # Try LLM extraction first
        if self.generator:
            try:
                prompt = _build_extraction_prompt(abstract_text)
                outputs = self.generator(
                    prompt,
                    max_new_tokens=256,
                    do_sample=False,
                    pad_token_id=self.tokenizer.pad_token_id,
                )
                raw = outputs[0]["generated_text"][len(prompt):]
                extraction = _parse_llm_output(raw, abstract_text)
            except Exception as e:
                logger.warning("LLM extraction failed, using rule-based: %s", e)
                extraction = _rule_based_extraction(abstract_text)
        else:
            extraction = _rule_based_extraction(abstract_text)

        confidence = _compute_confidence(extraction)
        return extraction, confidence


def reset_ai_outputs(engine) -> None:
    """Remove LLM outputs so extraction can rerun (e.g. switch rule-based → BioGPT)."""
    logger.warning(
        "Deleting all ai_extractions and generated_notes (including any HITL reviewer edits)."
    )
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM generated_notes"))
        conn.execute(text("DELETE FROM ai_extractions"))
    logger.info("Removed all rows from generated_notes and ai_extractions")


def run_extraction(use_gpu: bool = True, reset_ai_outputs_first: bool = False) -> int:
    """Run extraction on all unprocessed cleaned abstracts.

    If ``reset_ai_outputs_first`` is True, delete existing ``ai_extractions`` and
    ``generated_notes`` first so every cleaned abstract is reprocessed (e.g. after fixing Torch).
    """
    engine = get_engine()
    if reset_ai_outputs_first:
        reset_ai_outputs(engine)

    extractor = ClinicalExtractor(use_gpu=use_gpu)

    try:
        extractor.load_model()
    except Exception as e:
        logger.warning("Could not load LLM model, using rule-based only: %s", e)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT c.pmid, c.abstract_text
            FROM cleaned_abstracts c
            LEFT JOIN ai_extractions a ON c.pmid = a.pmid
            WHERE a.pmid IS NULL
            ORDER BY c.pmid
        """)).fetchall()

    logger.info("Found %d abstracts to process", len(rows))
    processed = 0

    with engine.begin() as conn:
        for pmid, abstract_text in rows:
            extraction, confidence = extractor.extract(abstract_text)
            payload = json.dumps(extraction)
            conn.execute(
                text("""
                    INSERT INTO ai_extractions
                        (pmid, extracted_json, original_extracted_json,
                         confidence_score, model_version)
                    VALUES (:pmid, :extracted_json, :original_json,
                            :confidence, :model_version)
                    ON CONFLICT (pmid) DO UPDATE SET
                        extracted_json = EXCLUDED.extracted_json,
                        confidence_score = EXCLUDED.confidence_score,
                        model_version = EXCLUDED.model_version,
                        extracted_at = NOW(),
                        original_extracted_json = COALESCE(
                            ai_extractions.original_extracted_json,
                            EXCLUDED.original_extracted_json
                        )
                """),
                {
                    "pmid": pmid,
                    "extracted_json": payload,
                    "original_json": payload,
                    "confidence": confidence,
                    "model_version": MODEL_VERSION,
                },
            )
            processed += 1
            if processed % 100 == 0:
                logger.info("Processed %d/%d abstracts", processed, len(rows))

    logger.info("Extraction complete: %d abstracts processed", processed)
    return processed


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Clinical extraction (BioGPT + rule fallback)")
    parser.add_argument(
        "--reset-ai",
        action="store_true",
        help="Delete all ai_extractions and generated_notes, then re-extract everything",
    )
    parser.add_argument(
        "--no-gpu",
        action="store_true",
        help="Force CPU even if CUDA is available",
    )
    args = parser.parse_args()
    count = run_extraction(
        use_gpu=not args.no_gpu,
        reset_ai_outputs_first=args.reset_ai,
    )
    print(f"Extracted variables from {count} abstracts")
