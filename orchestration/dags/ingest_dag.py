"""
Ingestion DAG — validates streaming path and SLA hooks (Airflow 2.x).
Set PIPELINE_ROOT to this repository root when deploying.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

REPO_ROOT = Path(os.environ.get("PIPELINE_ROOT", Path(__file__).resolve().parents[2]))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orchestration.sla_monitor import slack_sla_miss_callback

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _log_ingest_heartbeat():
    import logging

    logging.info("Ingest DAG tick — Kafka/Flink jobs run out-of-band or on cluster.")


with DAG(
    dag_id="ingest_dag",
    default_args=default_args,
    description="Engagement raw ingest checkpoint + SLA wiring",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["engagement", "ingest"],
    sla_miss_callback=slack_sla_miss_callback,
) as dag:
    heartbeat = PythonOperator(
        task_id="ingest_heartbeat",
        python_callable=_log_ingest_heartbeat,
        sla=timedelta(minutes=90),
    )

    validate_stub = BashOperator(
        task_id="validate_repo_layout",
        bash_command=f"test -d {REPO_ROOT / 'ingestion'} && echo OK",
    )

    heartbeat >> validate_stub
