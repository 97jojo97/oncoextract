"""Compare AI extractions to human-corrected values for HITL evaluation."""

from __future__ import annotations

import json
from typing import Any


def _norm_str(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    return s if s else None


def _lists_match(a: Any, b: Any) -> bool:
    la = [x.strip().lower() for x in (a or []) if str(x).strip()]
    lb = [x.strip().lower() for x in (b or []) if str(x).strip()]
    return set(la) == set(lb)


def _sample_match(ai: Any, human: Any) -> bool:
    if ai == human:
        return True
    try:
        ai_n = int(ai) if ai is not None else None
        hu_n = int(human) if human is not None else None
    except (TypeError, ValueError):
        return False
    if ai_n is None or hu_n is None:
        return ai_n == hu_n
    return abs(ai_n - hu_n) <= max(1, int(0.1 * hu_n))


def field_agreement(original: dict[str, Any], final: dict[str, Any]) -> dict[str, bool]:
    """Per-field match after human review (final is ground truth for reviewed rows)."""
    return {
        "tnm_stage": _norm_str(original.get("tnm_stage"))
        == _norm_str(final.get("tnm_stage")),
        "cancer_type": _norm_str(original.get("cancer_type"))
        == _norm_str(final.get("cancer_type")),
        "treatment_modality": _lists_match(
            original.get("treatment_modality"), final.get("treatment_modality")
        ),
        "biomarkers": _lists_match(original.get("biomarkers"), final.get("biomarkers")),
        "sample_size": _sample_match(original.get("sample_size"), final.get("sample_size")),
    }


def aggregate_field_accuracy(rows: list[tuple[dict[str, Any], dict[str, Any]]]) -> dict[str, float]:
    """Rows are (original_dict, final_dict). Returns fraction correct per field."""
    if not rows:
        return {}

    fields = ["tnm_stage", "cancer_type", "treatment_modality", "biomarkers", "sample_size"]
    counts = {f: 0 for f in fields}
    for orig, fin in rows:
        agree = field_agreement(orig, fin)
        for f in fields:
            if agree[f]:
                counts[f] += 1

    n = len(rows)
    return {f: round(counts[f] / n, 4) for f in fields}


def parse_jsonb(val: Any) -> dict[str, Any]:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    return json.loads(val)
