from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.spark_application import submit_spark_application, wait_spark_application


NAMESPACE = "criteo-lakehouse"
APP_DIR = "/opt/airflow/spark-applications"


with DAG(
    dag_id="criteo_iceberg_maintenance",
    start_date=datetime(2026, 1, 1),
    schedule="0 18 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["criteo", "iceberg", "maintenance"],
) as dag:
    submit = PythonOperator(
        task_id="submit_compact_expire_orphan_cleanup",
        python_callable=submit_spark_application,
        op_kwargs={
            "template_path": f"{APP_DIR}/maintenance.yaml",
            "namespace": NAMESPACE,
            "name": "criteo-iceberg-maintenance-{{ ds_nodash }}",
            "arguments": ["--action", "all"],
            "delete_existing": True,
        },
    )

    wait = PythonOperator(
        task_id="wait_maintenance",
        python_callable=wait_spark_application,
        op_kwargs={
            "namespace": NAMESPACE,
            "name": "criteo-iceberg-maintenance-{{ ds_nodash }}",
            "timeout_seconds": 7200,
        },
    )

    submit >> wait
