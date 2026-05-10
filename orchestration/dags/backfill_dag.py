from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from lib.spark_application import submit_spark_application, wait_spark_application


NAMESPACE = "criteo-lakehouse"
APP_DIR = "/opt/airflow/spark-applications"


with DAG(
    dag_id="criteo_backfill_and_gold_rebuild",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "event_start_date": Param("2026-05-01", type="string"),
        "event_end_date": Param("2026-05-01", type="string"),
    },
    tags=["criteo", "backfill", "reprocess"],
) as dag:
    submit_backfill = PythonOperator(
        task_id="submit_silver_backfill",
        python_callable=submit_spark_application,
        op_kwargs={
            "template_path": f"{APP_DIR}/backfill-silver.yaml",
            "namespace": NAMESPACE,
            "name": "criteo-backfill-silver-{{ logical_date.strftime('%Y%m%d%H%M%S') }}",
            "arguments": [
                "--event-start-date",
                "{{ params.event_start_date }}",
                "--event-end-date",
                "{{ params.event_end_date }}",
            ],
            "delete_existing": True,
        },
    )

    wait_backfill = PythonOperator(
        task_id="wait_silver_backfill",
        python_callable=wait_spark_application,
        op_kwargs={
            "namespace": NAMESPACE,
            "name": "criteo-backfill-silver-{{ logical_date.strftime('%Y%m%d%H%M%S') }}",
            "timeout_seconds": 14400,
        },
    )

    submit_gold_rebuild = PythonOperator(
        task_id="submit_gold_rebuild",
        python_callable=submit_spark_application,
        op_kwargs={
            "template_path": f"{APP_DIR}/gold-batch.yaml",
            "namespace": NAMESPACE,
            "name": "criteo-gold-rebuild-{{ logical_date.strftime('%Y%m%d%H%M%S') }}",
            "arguments": [
                "--start-date",
                "{{ params.event_start_date }}",
                "--end-date",
                "{{ params.event_end_date }}",
            ],
            "delete_existing": True,
        },
    )

    wait_gold_rebuild = PythonOperator(
        task_id="wait_gold_rebuild",
        python_callable=wait_spark_application,
        op_kwargs={
            "namespace": NAMESPACE,
            "name": "criteo-gold-rebuild-{{ logical_date.strftime('%Y%m%d%H%M%S') }}",
            "timeout_seconds": 14400,
        },
    )

    submit_backfill >> wait_backfill >> submit_gold_rebuild >> wait_gold_rebuild
