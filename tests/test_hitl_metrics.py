"""Tests for HITL field agreement metrics."""

from oncoextract.ai.hitl_metrics import aggregate_field_accuracy, field_agreement


def test_field_agreement_perfect_match():
    d = {
        "tnm_stage": "Stage III",
        "cancer_type": "Nasopharyngeal Carcinoma",
        "treatment_modality": ["chemotherapy", "radiation"],
        "biomarkers": ["EBV DNA"],
        "sample_size": 100,
    }
    agree = field_agreement(d, d)
    assert all(agree.values())


def test_field_agreement_stage_mismatch():
    base = {
        "treatment_modality": [],
        "biomarkers": [],
        "sample_size": None,
    }
    o = {"tnm_stage": "Stage I", "cancer_type": "NPC", **base}
    f = {"tnm_stage": "Stage III", "cancer_type": "NPC", **base}
    agree = field_agreement(o, f)
    assert agree["tnm_stage"] is False
    assert agree["cancer_type"] is True
    f2 = {**o, "cancer_type": "Nasopharyngeal Carcinoma"}
    agree2 = field_agreement(o, f2)
    assert agree2["cancer_type"] is False


def test_aggregate_field_accuracy():
    o1 = {
        "tnm_stage": "A",
        "cancer_type": "X",
        "treatment_modality": ["a"],
        "biomarkers": [],
        "sample_size": 10,
    }
    f1 = dict(o1)
    o2 = {**o1, "tnm_stage": "B"}
    f2 = {**o1, "tnm_stage": "B"}
    acc = aggregate_field_accuracy([(o1, f1), (o2, f2)])
    assert acc["tnm_stage"] == 1.0
    assert acc["cancer_type"] == 1.0
