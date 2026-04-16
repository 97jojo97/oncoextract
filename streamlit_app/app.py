"""Streamlit HITL interface for reviewing AI extractions."""

import json
import os
import sys
from pathlib import Path

import streamlit as st
from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))
load_dotenv(_REPO_ROOT / ".env")

st.set_page_config(
    page_title="OncoExtract - Clinical Review",
    page_icon="🔬",
    layout="wide",
)


def _apply_streamlit_secrets() -> None:
    """Copy Streamlit Cloud secrets into os.environ before DB modules load.

    Streamlit stores values in ``st.secrets`` only; ``python-dotenv`` does not read them.
    Supports either flat keys (``POSTGRES_HOST``, …), a single ``DATABASE_URL``, or a
    nested table ``[postgres]`` with host, port, database, user, password.

    Local runs have no ``secrets.toml``; ``in st.secrets`` raises — skip and use ``.env``.
    """
    try:
        from streamlit.errors import StreamlitSecretNotFoundError
    except ImportError:
        StreamlitSecretNotFoundError = type("StreamlitSecretNotFoundError", (Exception,), {})

    try:
        sec = st.secrets
    except Exception:
        return

    try:
        _copy_streamlit_secrets_to_environ(sec)
    except StreamlitSecretNotFoundError:
        return


def _copy_streamlit_secrets_to_environ(sec: object) -> None:
    if "DATABASE_URL" in sec:
        os.environ["DATABASE_URL"] = str(sec["DATABASE_URL"])
    if "POSTGRES_URL" in sec:
        os.environ["POSTGRES_URL"] = str(sec["POSTGRES_URL"])

    for block_name in ("postgres", "POSTGRES", "database"):
        if block_name not in sec:
            continue
        pg = sec[block_name]
        mapping = (
            ("host", "POSTGRES_HOST"),
            ("port", "POSTGRES_PORT"),
            ("database", "POSTGRES_DB"),
            ("dbname", "POSTGRES_DB"),
            ("user", "POSTGRES_USER"),
            ("username", "POSTGRES_USER"),
            ("password", "POSTGRES_PASSWORD"),
        )
        for src, dst in mapping:
            if src in pg:
                os.environ[dst] = str(pg[src])
        break

    for key in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ):
        if key in sec:
            os.environ[key] = str(sec[key])


_apply_streamlit_secrets()


def _db_troubleshoot_hint() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    if host in ("localhost", "127.0.0.1", "::1"):
        return (
            "On **Streamlit Community Cloud**, `localhost` is the app container, not your PC. "
            "Use a **hosted** Postgres (Neon, Supabase, RDS, …) and set secrets: "
            "`POSTGRES_HOST` (hostname), `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, "
            "`POSTGRES_PASSWORD`, or a single `DATABASE_URL`. "
            "Also ensure this app calls `_apply_streamlit_secrets()` **before** importing "
            "`get_engine` (see `streamlit_app/app.py`)."
        )
    return "Make sure PostgreSQL is running: `docker compose up -d`"


from sqlalchemy import text  # noqa: E402

from oncoextract.ai.hitl_metrics import aggregate_field_accuracy, field_agreement, parse_jsonb  # noqa: E402
from oncoextract.db.models import get_engine  # noqa: E402

engine = get_engine()


def get_review_queue():
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT
                a.pmid,
                c.title,
                c.abstract_text,
                a.extracted_json,
                a.original_extracted_json,
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


def get_verified_pairs_for_metrics():
    """Rows where human reviewed and we have an AI snapshot to compare."""
    with engine.connect() as conn:
        return conn.execute(text("""
            SELECT original_extracted_json, extracted_json
            FROM ai_extractions
            WHERE human_verified = true
              AND original_extracted_json IS NOT NULL
        """)).fetchall()


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
page = st.sidebar.radio(
    "Navigation",
    ["Review Queue", "Dashboard", "Evaluation"],
)

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
        st.info(_db_troubleshoot_hint())

elif page == "Evaluation":
    st.title("AI vs Human Evaluation")
    st.markdown(
        "Field-level **agreement rate** between the first AI output (`original_extracted_json`) "
        "and the human-reviewed record (`extracted_json`). "
        "Human labels are treated as reference after approval."
    )
    st.markdown("---")

    try:
        pairs = get_verified_pairs_for_metrics()
        parsed = [
            (parse_jsonb(o), parse_jsonb(f))
            for o, f in pairs
            if o is not None
        ]

        if not parsed:
            st.warning(
                "No reviewed rows with an AI snapshot yet. "
                "Approve items in **Review Queue** (or run migration + backfill — see README). "
                "New extractions store `original_extracted_json` automatically."
            )
        else:
            st.metric("Reviewed pairs (with AI snapshot)", len(parsed))
            acc = aggregate_field_accuracy(parsed)
            st.subheader("Agreement rate by field")
            st.caption("1.0 = AI matched human without edits; lower = more corrections needed.")

            cols = st.columns(5)
            field_labels = {
                "tnm_stage": "TNM / stage",
                "cancer_type": "Cancer type",
                "treatment_modality": "Treatments",
                "biomarkers": "Biomarkers",
                "sample_size": "Sample size",
            }
            for i, (key, label) in enumerate(field_labels.items()):
                with cols[i % 5]:
                    v = acc.get(key, 0.0)
                    st.metric(label, f"{100 * v:.1f}%")

            st.markdown("---")
            st.subheader("Sample disagreements (first 5)")
            shown = 0
            for orig, fin in parsed:
                agree = field_agreement(orig, fin)
                if all(agree.values()):
                    continue
                with st.expander(f"PMID — fields differ: {[k for k, v in agree.items() if not v]}"):
                    st.json({"ai_original": orig, "human_final": fin, "per_field_match": agree})
                shown += 1
                if shown >= 5:
                    break
            if shown == 0:
                st.info("No disagreements in sample — model matches human on reviewed rows.")

    except Exception as e:
        st.error(f"Could not load evaluation data: {e}")

elif page == "Review Queue":
    st.title("Clinical Extraction Review")
    st.markdown("Review, approve, or correct AI-generated clinical variable extractions.")
    st.markdown("---")

    try:
        rows = get_review_queue()
    except Exception as e:
        st.error(f"Could not connect to database: {e}")
        st.info(_db_troubleshoot_hint())
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
        if (show_verified or not r[6])
        and (r[5] or 0) >= min_confidence
    ]

    st.write(f"Showing {len(filtered)} of {len(rows)} extractions")

    for row in filtered:
        (
            pmid,
            title,
            abstract_text,
            extracted_json,
            _orig_json,
            confidence,
            verified,
            notes,
            summary,
        ) = row
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
