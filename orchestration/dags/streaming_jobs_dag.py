from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.spark_application import submit_spark_application, wait_spark_application


NAMESPACE = "criteo-lakehouse"
APP_DIR = "/opt/airflow/spark-applications"


with DAG(
    dag_id="criteo_streaming_control_plane",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["criteo", "streaming", "control-plane"],
) as dag:
    bootstrap_catalog = PythonOperator(
        task_id="bootstrap_catalog",
        python_callable=submit_spark_application,
        op_kwargs={
            "template_path": f"{APP_DIR}/bootstrap-catalog.yaml",
            "namespace": NAMESPACE,
            "delete_existing": True,
        },
    )

    wait_bootstrap_catalog = PythonOperator(
        task_id="wait_bootstrap_catalog",
        python_callable=wait_spark_application,
        op_kwargs={
            "namespace": NAMESPACE,
            "name": "criteo-bootstrap-catalog",
            "timeout_seconds": 1800,
        },
    )

    start_bronze_stream = PythonOperator(
        task_id="start_or_restart_kafka_to_bronze",
        python_callable=submit_spark_application,
        op_kwargs={
            "template_path": f"{APP_DIR}/bronze-stream.yaml",
            "namespace": NAMESPACE,
            "delete_existing": True,
        },
    )

    start_silver_stream = PythonOperator(
        task_id="start_or_restart_bronze_to_silver",
        python_callable=submit_spark_application,
        op_kwargs={
            "template_path": f"{APP_DIR}/silver-stream.yaml",
            "namespace": NAMESPACE,
            "delete_existing": True,
        },
    )

    bootstrap_catalog >> wait_bootstrap_catalog >> start_bronze_stream >> start_silver_stream
