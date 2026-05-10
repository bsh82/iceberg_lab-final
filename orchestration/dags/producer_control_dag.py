from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


NAMESPACE = "criteo-lakehouse"


with DAG(
    dag_id="criteo_producer_control_plane",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["criteo", "producer", "kafka"],
) as dag:
    run_producer_once = BashOperator(
        task_id="run_producer_once",
        bash_command=(
            "kubectl -n "
            + NAMESPACE
            + " create job --from=cronjob/criteo-producer "
            + "criteo-producer-{{ logical_date.strftime('%Y%m%d%H%M%S') }}"
        ),
    )
