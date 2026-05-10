from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.spark_application import submit_spark_application, wait_spark_application


NAMESPACE = "criteo-lakehouse"
APP_DIR = "/opt/airflow/spark-applications"


with DAG(
    dag_id="criteo_gold_daily_aggregation",
    start_date=datetime(2026, 1, 1),
    schedule="20 1 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["criteo", "gold", "batch"],
) as dag:
    submit = PythonOperator(
        task_id="submit_gold_aggregation",
        python_callable=submit_spark_application,
        op_kwargs={
            "template_path": f"{APP_DIR}/gold-batch.yaml",
            "namespace": NAMESPACE,
            "name": "criteo-silver-to-gold-{{ ds_nodash }}",
            "arguments": ["--start-date", "{{ ds }}", "--end-date", "{{ ds }}"],
            "delete_existing": True,
        },
    )

    wait = PythonOperator(
        task_id="wait_gold_aggregation",
        python_callable=wait_spark_application,
        op_kwargs={
            "namespace": NAMESPACE,
            "name": "criteo-silver-to-gold-{{ ds_nodash }}",
            "timeout_seconds": 7200,
        },
    )

    submit >> wait
