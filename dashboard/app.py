"""
Streamlit dashboard — engagement trends, SLA tracker, data quality, privacy summary.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
QUALITY_REPORT = REPO_ROOT / "data/quality/last_run.json"


@st.cache_data(ttl=60)
def load_quality_report() -> list[dict]:
    if not QUALITY_REPORT.exists():
        return []
    try:
        return json.loads(QUALITY_REPORT.read_text(encoding="utf-8"))
    except Exception:
        return []


def synthetic_engagement_trends() -> pd.DataFrame:
    rng = pd.date_range(end=datetime.now(timezone.utc), periods=8, freq="W")
    rows = []
    for ts in rng:
        rows.append({"week": ts.date(), "app": "Facebook", "dau_millions": 1950 + (hash(str(ts)) % 40)})
        rows.append({"week": ts.date(), "app": "Instagram", "dau_millions": 1620 + (hash(str(ts)) % 35)})
        rows.append({"week": ts.date(), "app": "Threads", "dau_millions": 130 + (hash(str(ts)) % 12)})
        rows.append({"week": ts.date(), "app": "WhatsApp", "dau_millions": 2100 + (hash(str(ts)) % 30)})
    return pd.DataFrame(rows)


def synthetic_sla() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"dataset": "fb_engagement_edges", "freshness_sla_h": 2, "completeness_pct": 99.4, "status": "Healthy"},
            {"dataset": "ig_story_interactions", "freshness_sla_h": 2, "completeness_pct": 99.1, "status": "Healthy"},
            {"dataset": "threads_post_signals", "freshness_sla_h": 4, "completeness_pct": 97.6, "status": "Healthy"},
            {"dataset": "wa_msg_graph", "freshness_sla_h": 2, "completeness_pct": 99.3, "status": "Healthy"},
            {"dataset": "xapp_session_stitched", "freshness_sla_h": 6, "completeness_pct": 98.2, "status": "Healthy"},
        ]
    )


def synthetic_privacy() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"control": "k-Anonymity (k=100)", "coverage": "100%", "last_audit_utc": "2026-04-01T08:00:00Z"},
            {"control": "Laplace DP (ε tiers)", "coverage": "Demographic + standard metrics", "last_audit_utc": "2026-04-02T01:15:00Z"},
            {"control": "PII tokenization", "coverage": "Ingest path", "last_audit_utc": "2026-04-03T12:30:00Z"},
        ]
    )


st.set_page_config(page_title="Engagement Intelligence", layout="wide")
st.title("Meta-scale engagement intelligence (simulation)")
st.caption("Synthetic demo metrics + optional `data/quality/last_run.json` from quality_runner.")

tab_trends, tab_sla, tab_quality, tab_privacy = st.tabs(
    ["Engagement trends", "SLA tracker", "Data quality", "Privacy model"]
)

with tab_trends:
    st.subheader("Weekly DAU by app (illustrative)")
    df = synthetic_engagement_trends()
    st.line_chart(df.pivot(index="week", columns="app", values="dau_millions"))

with tab_sla:
    st.subheader("Dataset SLAs (targets from orchestration/sla_monitor.py)")
    st.dataframe(synthetic_sla(), use_container_width=True)

with tab_quality:
    st.subheader("Latest quality run")
    rep = load_quality_report()
    if rep:
        st.json(rep)
    else:
        st.info("Run `python quality/quality_runner.py --dataset all --json-out data/quality/last_run.json` to populate.")
    st.dataframe(
        pd.DataFrame(rep) if rep else synthetic_sla()[["dataset", "completeness_pct"]].rename(
            columns={"completeness_pct": "illustrative_score"}
        ),
        use_container_width=True,
    )

with tab_privacy:
    st.subheader("Active privacy controls")
    st.dataframe(synthetic_privacy(), use_container_width=True)
