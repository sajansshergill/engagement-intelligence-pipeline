"""
Transform DAG — orchestrates PySpark ETL modules (local: spark-submit or python -m).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_ROOT = Path(os.environ.get("PIPELINE_ROOT", Path(__file__).resolve().parents[2]))
PYTHON = os.environ.get("PIPELINE_PYTHON", "python3")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="transform_dag",
    default_args=default_args,
    description="Session stitch, dedup, edge weights, privacy transforms",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["engagement", "transform"],
) as dag:
    # Placeholder paths — override with Airflow Variables/Params in production
    in_path = os.environ.get("STAGING_EVENTS_PATH", str(REPO_ROOT / "data/staging/events"))
    out_dedup = os.environ.get("DEDUP_OUT", str(REPO_ROOT / "data/curated/deduped"))
    out_sessions = os.environ.get("SESSION_OUT", str(REPO_ROOT / "data/curated/sessions"))
    out_edges = os.environ.get("EDGES_OUT", str(REPO_ROOT / "data/curated/edges"))

    dedup = BashOperator(
        task_id="deduplicator",
        bash_command=f"cd {REPO_ROOT} && {PYTHON} -m etl.deduplicator --input {in_path} --output {out_dedup}",
    )

    sessions = BashOperator(
        task_id="session_stitcher",
        bash_command=f"cd {REPO_ROOT} && {PYTHON} -m etl.session_stitcher --input {out_dedup} --output {out_sessions}",
    )

    edges = BashOperator(
        task_id="edge_weighter",
        bash_command=f"cd {REPO_ROOT} && {PYTHON} -m etl.edge_weighter --input {out_dedup} --output {out_edges}",
    )

    dedup >> [sessions, edges]
