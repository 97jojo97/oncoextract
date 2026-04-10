"""Tests for clinical variable extraction logic."""

from oncoextract.ai.extract import (
    ClinicalExtraction,
    _compute_confidence,
    _parse_llm_output,
    _rule_based_extraction,
)

SAMPLE_NPC_ABSTRACT = (
    "BACKGROUND: Nasopharyngeal carcinoma (NPC) is endemic in Southern China. "
    "This study enrolled 245 patients with Stage III NPC (T2N1M0). "
    "METHODS: Patients received concurrent chemoradiotherapy with cisplatin-based chemotherapy. "
    "EBV DNA levels were measured before and after treatment. "
    "PD-L1 expression was assessed by immunohistochemistry. "
    "RESULTS: Complete response was achieved in 78% of patients. "
    "Higher baseline EBV DNA (>1500 copies/mL) was associated with poorer prognosis."
)


class TestRuleBasedExtraction:
    def test_detects_tnm_stage(self):
        result = _rule_based_extraction(SAMPLE_NPC_ABSTRACT)
        assert result["tnm_stage"] == "T2N1M0"

    def test_detects_treatment_modalities(self):
        result = _rule_based_extraction(SAMPLE_NPC_ABSTRACT)
        assert "chemotherapy" in result["treatment_modality"]
        assert "radiation" in result["treatment_modality"]

    def test_detects_biomarkers(self):
        result = _rule_based_extraction(SAMPLE_NPC_ABSTRACT)
        assert "EBV DNA" in result["biomarkers"]
        assert "PD-L1" in result["biomarkers"]

    def test_detects_sample_size(self):
        result = _rule_based_extraction(SAMPLE_NPC_ABSTRACT)
        assert result["sample_size"] == 245

    def test_detects_cancer_type(self):
        result = _rule_based_extraction(SAMPLE_NPC_ABSTRACT)
        assert result["cancer_type"] == "Nasopharyngeal Carcinoma"

    def test_empty_abstract(self):
        result = _rule_based_extraction("")
        assert result["tnm_stage"] is None
        assert result["treatment_modality"] == []
        assert result["biomarkers"] == []

    def test_stage_roman_numeral(self):
        result = _rule_based_extraction("Patients with Stage IVA disease were enrolled.")
        assert result["tnm_stage"] == "Stage IVA"

    def test_immunotherapy_detection(self):
        result = _rule_based_extraction(
            "Pembrolizumab was administered as first-line immunotherapy."
        )
        assert "immunotherapy" in result["treatment_modality"]


class TestConfidenceScore:
    def test_full_extraction(self):
        full = {
            "tnm_stage": "Stage III",
            "treatment_modality": ["chemotherapy"],
            "biomarkers": ["EBV DNA"],
            "sample_size": 100,
            "cancer_type": "NPC",
        }
        assert _compute_confidence(full) == 1.0

    def test_empty_extraction(self):
        empty = {
            "tnm_stage": None,
            "treatment_modality": [],
            "biomarkers": [],
            "sample_size": None,
            "cancer_type": None,
        }
        assert _compute_confidence(empty) == 0.0

    def test_partial_extraction(self):
        partial = {
            "tnm_stage": None,
            "treatment_modality": ["radiation"],
            "biomarkers": [],
            "sample_size": 50,
            "cancer_type": "NPC",
        }
        assert _compute_confidence(partial) == 0.6


class TestParseLLMOutput:
    def test_valid_json(self):
        raw = (
            '{"tnm_stage": "Stage III", "treatment_modality": ["chemo"],'
            ' "biomarkers": ["EBV"], "sample_size": 100, "cancer_type": "NPC"}'
        )
        result = _parse_llm_output(raw, "some abstract")
        assert result["tnm_stage"] == "Stage III"

    def test_json_with_surrounding_text(self):
        raw = (
            'Here is the extraction:\n'
            '{"tnm_stage": "Stage II", "treatment_modality": [],'
            ' "biomarkers": [], "sample_size": null, "cancer_type": null}'
            '\nDone.'
        )
        result = _parse_llm_output(raw, "some abstract")
        assert result["tnm_stage"] == "Stage II"

    def test_invalid_json_falls_back(self):
        raw = "This is not valid JSON at all"
        abstract = "245 patients with nasopharyngeal carcinoma received chemotherapy"
        result = _parse_llm_output(raw, abstract)
        assert result["cancer_type"] == "Nasopharyngeal Carcinoma"
        assert result["sample_size"] == 245


class TestPydanticModel:
    def test_defaults(self):
        ext = ClinicalExtraction()
        assert ext.tnm_stage is None
        assert ext.treatment_modality == []
        assert ext.biomarkers == []

    def test_from_dict(self):
        data = {"tnm_stage": "Stage III", "biomarkers": ["EBV DNA"]}
        ext = ClinicalExtraction(**data)
        assert ext.tnm_stage == "Stage III"
        assert ext.treatment_modality == []
