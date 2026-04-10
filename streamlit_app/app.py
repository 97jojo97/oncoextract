"""Streamlit HITL interface for reviewing AI extractions."""

import json
import sys
from pathlib import Path

import streamlit as st
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from oncoextract.db.models import get_engine

st.set_page_config(
    page_title="OncoExtract - Clinical Review",
    page_icon="🔬",
    layout="wide",
)

engine = get_engine()


def get_review_queue():
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT
                a.pmid,
                c.title,
                c.abstract_text,
                a.extracted_json,
                a.confidence_score,
                a.human_verified,
                a.reviewer_notes,
                g.summary_text
            FROM ai_extractions a
            JOIN cleaned_abstracts c ON a.pmid = c.pmid
            LEFT JOIN generated_notes g ON a.pmid = g.pmid
            ORDER BY a.human_verified ASC, a.confidence_score ASC
        """)).fetchall()


def get_dashboard_stats():
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM ai_extractions")).scalar()
        verified = conn.execute(
            text("SELECT COUNT(*) FROM ai_extractions WHERE human_verified = true")
        ).scalar()
        avg_conf = conn.execute(
            text("SELECT ROUND(AVG(confidence_score)::numeric, 2) FROM ai_extractions")
        ).scalar()
        return {
            "total": total or 0,
            "verified": verified or 0,
            "avg_confidence": float(avg_conf) if avg_conf else 0.0,
        }


def approve_extraction(pmid: str, notes: str = ""):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE ai_extractions
                SET human_verified = true, reviewer_notes = :notes
                WHERE pmid = :pmid
            """),
            {"pmid": pmid, "notes": notes},
        )


def reject_extraction(pmid: str, notes: str = ""):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE ai_extractions
                SET human_verified = false, reviewer_notes = :notes
                WHERE pmid = :pmid
            """),
            {"pmid": pmid, "notes": notes},
        )


def update_extraction(pmid: str, updated_json: dict, notes: str = ""):
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE ai_extractions
                SET extracted_json = :extracted_json,
                    human_verified = true,
                    reviewer_notes = :notes
                WHERE pmid = :pmid
            """),
            {"pmid": pmid, "extracted_json": json.dumps(updated_json), "notes": notes},
        )


# --- Sidebar navigation ---
page = st.sidebar.radio("Navigation", ["Review Queue", "Dashboard"])

if page == "Dashboard":
    st.title("OncoExtract Dashboard")
    st.markdown("---")

    try:
        stats = get_dashboard_stats()
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Extractions", stats["total"])
        col2.metric("Human Verified", stats["verified"])
        col3.metric(
            "Verification Rate",
            f"{100 * stats['verified'] / stats['total']:.1f}%" if stats["total"] > 0 else "N/A",
        )
        col4.metric("Avg Confidence", f"{stats['avg_confidence']:.2f}")

        st.markdown("---")
        st.subheader("Verification Progress")
        if stats["total"] > 0:
            st.progress(stats["verified"] / stats["total"])
        else:
            st.info("No extractions yet. Run the pipeline first.")

        st.markdown("---")
        st.subheader("Confidence Distribution")
        with engine.connect() as conn:
            conf_data = conn.execute(text("""
                SELECT
                    CASE
                        WHEN confidence_score >= 0.8 THEN 'High (0.8-1.0)'
                        WHEN confidence_score >= 0.4 THEN 'Medium (0.4-0.8)'
                        ELSE 'Low (0.0-0.4)'
                    END as confidence_band,
                    COUNT(*) as count
                FROM ai_extractions
                GROUP BY confidence_band
                ORDER BY confidence_band
            """)).fetchall()
        if conf_data:
            for band, count in conf_data:
                st.write(f"**{band}**: {count} extractions")
        else:
            st.info("No data available.")

    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        st.info("Make sure PostgreSQL is running: `docker compose up -d`")

elif page == "Review Queue":
    st.title("Clinical Extraction Review")
    st.markdown("Review, approve, or correct AI-generated clinical variable extractions.")
    st.markdown("---")

    try:
        rows = get_review_queue()
    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        st.info("Make sure PostgreSQL is running: `docker compose up -d`")
        st.stop()

    if not rows:
        st.info("No extractions to review. Run the pipeline first.")
        st.stop()

    # Filter controls
    filter_col1, filter_col2 = st.columns(2)
    with filter_col1:
        show_verified = st.checkbox("Show already verified", value=False)
    with filter_col2:
        min_confidence = st.slider("Min confidence", 0.0, 1.0, 0.0, 0.1)

    filtered = [
        r for r in rows
        if (show_verified or not r[5])
        and (r[4] or 0) >= min_confidence
    ]

    st.write(f"Showing {len(filtered)} of {len(rows)} extractions")

    for row in filtered:
        pmid, title, abstract_text, extracted_json, confidence, verified, notes, summary = row
        extraction = (
            json.loads(extracted_json)
            if isinstance(extracted_json, str)
            else extracted_json
        )

        status_icon = "✅" if verified else "⏳"
        with st.expander(f"{status_icon} PMID {pmid} — {title[:80]}... (conf: {confidence:.2f})"):
            col_left, col_right = st.columns([1, 1])

            with col_left:
                st.subheader("Abstract")
                txt = abstract_text or ""
                display = txt[:500] + "..." if len(txt) > 500 else txt
                st.write(display)

                if summary:
                    st.subheader("Generated Summary")
                    st.info(summary)

            with col_right:
                st.subheader("AI Extraction")

                edited_stage = st.text_input(
                    "TNM Stage", extraction.get("tnm_stage") or "", key=f"stage_{pmid}"
                )
                edited_treatments = st.text_input(
                    "Treatment Modalities (comma-separated)",
                    ", ".join(extraction.get("treatment_modality", [])),
                    key=f"treat_{pmid}",
                )
                edited_biomarkers = st.text_input(
                    "Biomarkers (comma-separated)",
                    ", ".join(extraction.get("biomarkers", [])),
                    key=f"bio_{pmid}",
                )
                edited_sample = st.text_input(
                    "Sample Size",
                    str(extraction.get("sample_size") or ""),
                    key=f"sample_{pmid}",
                )
                edited_cancer = st.text_input(
                    "Cancer Type",
                    extraction.get("cancer_type") or "",
                    key=f"cancer_{pmid}",
                )
                reviewer_notes = st.text_area(
                    "Reviewer Notes", notes or "", key=f"notes_{pmid}"
                )

                btn_col1, btn_col2, btn_col3 = st.columns(3)

                with btn_col1:
                    if st.button("Approve", key=f"approve_{pmid}", type="primary"):
                        treatments = [
                            t.strip() for t in edited_treatments.split(",") if t.strip()
                        ]
                        bios = [
                            b.strip() for b in edited_biomarkers.split(",") if b.strip()
                        ]
                        updated = {
                            "tnm_stage": edited_stage or None,
                            "treatment_modality": treatments,
                            "biomarkers": bios,
                            "sample_size": int(edited_sample) if edited_sample.isdigit() else None,
                            "cancer_type": edited_cancer or None,
                        }
                        update_extraction(pmid, updated, reviewer_notes)
                        st.success(f"PMID {pmid} approved!")
                        st.rerun()

                with btn_col2:
                    if st.button("Reject", key=f"reject_{pmid}"):
                        reject_extraction(pmid, reviewer_notes)
                        st.warning(f"PMID {pmid} rejected for re-extraction.")
                        st.rerun()

                with btn_col3:
                    if st.button("Skip", key=f"skip_{pmid}"):
                        pass

    st.markdown("---")
    st.caption("OncoExtract Clinical Abstraction Pipeline — HITL Review Interface")
