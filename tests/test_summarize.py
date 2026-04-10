"""Tests for clinical note generation and validation metrics."""

from oncoextract.ai.summarize import compute_validation_metrics, generate_summary


class TestGenerateSummary:
    def test_full_extraction(self):
        extraction = {
            "tnm_stage": "Stage III",
            "treatment_modality": ["chemotherapy", "radiation"],
            "biomarkers": ["EBV DNA", "PD-L1"],
            "sample_size": 245,
            "cancer_type": "Nasopharyngeal Carcinoma",
        }
        abstract = "This is a study about NPC. Results showed improved survival."
        result = generate_summary(extraction, abstract)

        assert "245 patients" in result
        assert "Stage III" in result
        assert "Nasopharyngeal Carcinoma" in result
        assert "chemotherapy" in result
        assert "EBV DNA" in result

    def test_minimal_extraction(self):
        extraction = {
            "tnm_stage": None,
            "treatment_modality": [],
            "biomarkers": [],
            "sample_size": None,
            "cancer_type": None,
        }
        result = generate_summary(extraction, "Short.")
        assert "oncology condition" in result
        assert "unspecified" in result

    def test_with_key_finding(self):
        extraction = {
            "tnm_stage": "Stage II",
            "treatment_modality": ["immunotherapy"],
            "biomarkers": [],
            "sample_size": 100,
            "cancer_type": "Lung Cancer",
        }
        abstract = (
            "Background: Lung cancer is common. "
            "Methods: We enrolled patients. "
            "Results: Immunotherapy showed a 40% response rate in advanced cases"
        )
        result = generate_summary(extraction, abstract)
        assert "Key finding:" in result


class TestValidationMetrics:
    def test_perfect_predictions(self):
        preds = [
            {"tnm_stage": "Stage III", "cancer_type": "NPC",
             "treatment_modality": ["chemo"], "biomarkers": ["EBV"],
             "sample_size": 100},
        ]
        golds = [
            {"tnm_stage": "Stage III", "cancer_type": "NPC",
             "treatment_modality": ["chemo"], "biomarkers": ["EBV"],
             "sample_size": 100},
        ]
        metrics = compute_validation_metrics(preds, golds)
        assert metrics["tnm_stage"]["f1"] == 1.0
        assert metrics["cancer_type"]["f1"] == 1.0
        assert metrics["treatment_modality"]["f1"] == 1.0
        assert metrics["sample_size"]["accuracy"] == 1.0

    def test_wrong_predictions(self):
        preds = [
            {"tnm_stage": "Stage I", "cancer_type": "Lung",
             "treatment_modality": ["surgery"], "biomarkers": [],
             "sample_size": 50},
        ]
        golds = [
            {"tnm_stage": "Stage III", "cancer_type": "NPC",
             "treatment_modality": ["chemo"], "biomarkers": ["EBV"],
             "sample_size": 100},
        ]
        metrics = compute_validation_metrics(preds, golds)
        assert metrics["tnm_stage"]["f1"] == 0.0
        assert metrics["biomarkers"]["recall"] == 0.0
        assert metrics["sample_size"]["accuracy"] == 0.0

    def test_empty_inputs(self):
        metrics = compute_validation_metrics([], [])
        assert metrics["tnm_stage"]["f1"] == 0.0
        assert metrics["sample_size"]["accuracy"] == 0.0
