"""Tests for PubMed ingestion logic."""

from oncoextract.ingest.pubmed import _parse_pubmed_xml

SAMPLE_XML = """<?xml version="1.0" ?>
<!DOCTYPE PubmedArticleSet PUBLIC "-//NLM//DTD PubMedArticle, 1st January 2024//EN"
  "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_240101.dtd">
<PubmedArticleSet>
  <PubmedArticle>
    <MedlineCitation>
      <PMID Version="1">12345678</PMID>
      <Article>
        <Journal>
          <Title>Journal of Oncology</Title>
          <JournalIssue>
            <PubDate><Year>2023</Year><Month>Mar</Month><Day>15</Day></PubDate>
          </JournalIssue>
        </Journal>
        <ArticleTitle>EBV DNA and TNM staging in nasopharyngeal carcinoma</ArticleTitle>
        <Abstract>
          <AbstractText Label="BACKGROUND">NPC is endemic in Southern China.</AbstractText>
          <AbstractText Label="METHODS">We enrolled 200 patients with stage III NPC.</AbstractText>
          <AbstractText Label="RESULTS">EBV DNA levels correlated with tumor stage.</AbstractText>
        </Abstract>
        <AuthorList>
          <Author><LastName>Zhang</LastName><ForeName>Wei</ForeName></Author>
          <Author><LastName>Li</LastName><ForeName>Hua</ForeName></Author>
        </AuthorList>
      </Article>
      <MeshHeadingList>
        <MeshHeading><DescriptorName>Nasopharyngeal Neoplasms</DescriptorName></MeshHeading>
        <MeshHeading><DescriptorName>Herpesvirus 4, Human</DescriptorName></MeshHeading>
      </MeshHeadingList>
    </MedlineCitation>
  </PubmedArticle>
</PubmedArticleSet>"""


def test_parse_pubmed_xml_basic():
    articles = _parse_pubmed_xml(SAMPLE_XML)
    assert len(articles) == 1

    art = articles[0]
    assert art["pmid"] == "12345678"
    assert "nasopharyngeal carcinoma" in art["title"].lower()
    assert "BACKGROUND: NPC is endemic" in art["abstract_text"]
    assert art["authors"] == ["Zhang, Wei", "Li, Hua"]
    assert art["journal"] == "Journal of Oncology"
    assert art["pub_date"] == "2023-03-15"
    assert "Nasopharyngeal Neoplasms" in art["mesh_terms"]


def test_parse_pubmed_xml_missing_abstract():
    xml = """<PubmedArticleSet>
      <PubmedArticle>
        <MedlineCitation>
          <PMID>99999</PMID>
          <Article>
            <Journal><Title>Test</Title>
              <JournalIssue><PubDate><Year>2020</Year></PubDate></JournalIssue>
            </Journal>
            <ArticleTitle>No abstract here</ArticleTitle>
          </Article>
        </MedlineCitation>
      </PubmedArticle>
    </PubmedArticleSet>"""
    articles = _parse_pubmed_xml(xml)
    assert len(articles) == 1
    assert articles[0]["abstract_text"] == ""
    assert articles[0]["pub_date"] == "2020-01-01"


def test_parse_pubmed_xml_empty():
    xml = "<PubmedArticleSet></PubmedArticleSet>"
    articles = _parse_pubmed_xml(xml)
    assert articles == []
