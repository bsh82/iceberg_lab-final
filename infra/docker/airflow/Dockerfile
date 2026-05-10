FROM apache/airflow:2.10.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -L -o /usr/local/bin/kubectl https://dl.k8s.io/release/v1.30.6/bin/linux/amd64/kubectl \
    && chmod +x /usr/local/bin/kubectl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-cncf-kubernetes==10.1.0 \
    pyyaml==6.0.2

COPY orchestration/dags /opt/airflow/dags
COPY orchestration/spark-applications /opt/airflow/spark-applications
COPY configs /opt/airflow/configs
