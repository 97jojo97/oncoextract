"""Tests for the PySpark cleaning logic."""

from oncoextract.spark.clean import normalize_text


class TestNormalizeText:
    def test_strips_html_tags(self):
        assert normalize_text("<b>Hello</b> world") == "Hello world"

    def test_decodes_html_entities(self):
        assert normalize_text("5 &gt; 3 &amp; 2 &lt; 4") == "5 > 3 & 2 < 4"

    def test_collapses_whitespace(self):
        assert normalize_text("too   many    spaces\n\nhere") == "too many spaces here"

    def test_handles_none(self):
        assert normalize_text(None) == ""

    def test_handles_empty(self):
        assert normalize_text("") == ""

    def test_real_clinical_text(self):
        raw = (
            "BACKGROUND: Nasopharyngeal carcinoma (NPC) is a &quot;rare&quot; "
            "malignancy in\nWestern countries   but <i>endemic</i> in Southern China."
        )
        result = normalize_text(raw)
        assert "&quot;" not in result
        assert "<i>" not in result
        assert "  " not in result
        assert result.startswith("BACKGROUND:")
        assert "endemic" in result

    def test_nested_html(self):
        raw = "<p><span>Patient had <b>stage III</b> disease</span></p>"
        assert normalize_text(raw) == "Patient had stage III disease"
