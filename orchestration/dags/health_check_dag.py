from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.spark_application import submit_spark_application, wait_spark_application


NAMESPACE = "criteo-lakehouse"
APP_DIR = "/opt/airflow/spark-applications"


with DAG(
    dag_id="criteo_operational_health_check",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["criteo", "health"],
) as dag:
    submit = PythonOperator(
        task_id="submit_health_check",
        python_callable=submit_spark_application,
        op_kwargs={
            "template_path": f"{APP_DIR}/health-check.yaml",
            "namespace": NAMESPACE,
            "name": "criteo-health-check-{{ logical_date.strftime('%Y%m%d%H%M%S') }}",
            "delete_existing": True,
        },
    )

    wait = PythonOperator(
        task_id="wait_health_check",
        python_callable=wait_spark_application,
        op_kwargs={
            "namespace": NAMESPACE,
            "name": "criteo-health-check-{{ logical_date.strftime('%Y%m%d%H%M%S') }}",
            "timeout_seconds": 1800,
        },
    )

    submit >> wait
