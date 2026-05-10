FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/project

RUN pip install --no-cache-dir \
    huggingface-hub==0.24.6 \
    kafka-python==2.0.2 \
    pyyaml==6.0.2

WORKDIR /opt/project
COPY code /opt/project/code
COPY configs /opt/project/configs
COPY scripts/download_dataset.py /opt/project/scripts/download_dataset.py

ENTRYPOINT ["python", "-m", "code.producers.criteo_kafka_producer"]
