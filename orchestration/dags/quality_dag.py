"""
Daily quality DAG — runs Great Expectations / pandas-backed quality_runner.
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
    "retry_delay": timedelta(minutes=15),
}

with DAG(
    dag_id="quality_dag",
    default_args=default_args,
    description="Nightly data quality evaluation",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["engagement", "quality"],
) as dag:
    BashOperator(
        task_id="run_quality_runner",
        bash_command=(
            f"cd {REPO_ROOT} && {PYTHON} quality/quality_runner.py "
            f"--dataset all --json-out {REPO_ROOT / 'data/quality/last_run.json'}"
        ),
    )
