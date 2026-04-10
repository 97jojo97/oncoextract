"""Clinical note generation and validation metrics."""

import json
import logging
from typing import Any

from dotenv import load_dotenv
from sqlalchemy import text

from oncoextract.db.models import get_engine

load_dotenv()

logger = logging.getLogger(__name__)

MODEL_VERSION = "template-v1"


def generate_summary(extraction: dict[str, Any], abstract_text: str) -> str:
    """Generate a patient-ready clinical summary from extracted variables."""
    cancer = extraction.get("cancer_type") or "oncology condition"
    stage = extraction.get("tnm_stage") or "unspecified stage"
    treatments = extraction.get("treatment_modality") or []
    biomarkers = extraction.get("biomarkers") or []
    sample_size = extraction.get("sample_size")

    treatment_str = ", ".join(treatments) if treatments else "unspecified treatment"
    biomarker_str = ", ".join(biomarkers) if biomarkers else "none reported"

    parts = []

    if sample_size:
        parts.append(
            f"This study examined {sample_size} patients with {stage} {cancer}, "
            f"treated with {treatment_str}."
        )
    else:
        parts.append(
            f"This study investigated {cancer} ({stage}), "
            f"with treatment involving {treatment_str}."
        )

    parts.append(f"Biomarkers assessed: {biomarker_str}.")

    if abstract_text and len(abstract_text) > 100:
        # Extract a conclusion-like sentence from the end of the abstract
        sentences = [s.strip() for s in abstract_text.split(".") if len(s.strip()) > 20]
        if sentences:
            last = sentences[-1].rstrip(".")
            parts.append(f"Key finding: {last}.")

    return " ".join(parts)


def compute_validation_metrics(
    predictions: list[dict[str, Any]],
    gold_labels: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compute precision, recall, F1 per extraction field.

    Each entry in predictions/gold_labels should have the same structure
    as ClinicalExtraction.model_dump().
    """
    field_metrics: dict[str, dict[str, float]] = {}

    for field in ["tnm_stage", "cancer_type"]:
        tp = fp = fn = 0
        for pred, gold in zip(predictions, gold_labels):
            p_val = pred.get(field)
            g_val = gold.get(field)
            if p_val and g_val and p_val.lower() == g_val.lower():
                tp += 1
            elif p_val and (not g_val or p_val.lower() != g_val.lower()):
                fp += 1
            elif g_val and not p_val:
                fn += 1
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        field_metrics[field] = {
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "f1": round(f1, 3),
        }

    for field in ["treatment_modality", "biomarkers"]:
        tp = fp = fn = 0
        for pred, gold in zip(predictions, gold_labels):
            p_set = set(s.lower() for s in (pred.get(field) or []))
            g_set = set(s.lower() for s in (gold.get(field) or []))
            tp += len(p_set & g_set)
            fp += len(p_set - g_set)
            fn += len(g_set - p_set)
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        field_metrics[field] = {
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "f1": round(f1, 3),
        }

    # Sample size: within-10% tolerance
    correct = total = 0
    for pred, gold in zip(predictions, gold_labels):
        p_size = pred.get("sample_size")
        g_size = gold.get("sample_size")
        if g_size is not None:
            total += 1
            if p_size is not None and abs(p_size - g_size) <= max(0.1 * g_size, 1):
                correct += 1
    field_metrics["sample_size"] = {
        "accuracy": round(correct / total, 3) if total > 0 else 0.0,
        "total_evaluated": total,
    }

    return field_metrics


def run_summarization() -> int:
    """Generate summaries for all extractions that don't have notes yet."""
    engine = get_engine()

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT a.pmid, a.extracted_json, c.abstract_text
            FROM ai_extractions a
            JOIN cleaned_abstracts c ON a.pmid = c.pmid
            LEFT JOIN generated_notes g ON a.pmid = g.pmid
            WHERE g.pmid IS NULL
        """)).fetchall()

    logger.info("Found %d extractions needing summaries", len(rows))
    generated = 0

    with engine.begin() as conn:
        for pmid, extracted_json, abstract_text in rows:
            extraction = (
                json.loads(extracted_json)
                if isinstance(extracted_json, str)
                else extracted_json
            )
            summary = generate_summary(extraction, abstract_text)

            conn.execute(
                text("""
                    INSERT INTO generated_notes (pmid, summary_text, model_version)
                    VALUES (:pmid, :summary, :model_version)
                    ON CONFLICT (pmid) DO UPDATE SET
                        summary_text = :summary,
                        model_version = :model_version,
                        generated_at = NOW()
                """),
                {"pmid": pmid, "summary": summary, "model_version": MODEL_VERSION},
            )
            generated += 1

    logger.info("Generated %d summaries", generated)
    return generated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    count = run_summarization()
    print(f"Generated {count} clinical notes")
