SHELL := /bin/bash

K8S_NAMESPACE ?= criteo-lakehouse
SPARK_IMAGE ?= criteo-spark:local
AIRFLOW_IMAGE ?= criteo-airflow:local
PRODUCER_IMAGE ?= criteo-producer:local
SUPERSET_IMAGE ?= criteo-superset:local

.PHONY: build-images kind-load deploy undeploy aws-secret validate

build-images:
	docker build -t $(SPARK_IMAGE) -f infra/docker/spark/Dockerfile .
	docker build -t $(AIRFLOW_IMAGE) -f infra/docker/airflow/Dockerfile .
	docker build -t $(PRODUCER_IMAGE) -f infra/docker/producer/Dockerfile .
	docker build -t $(SUPERSET_IMAGE) -f infra/docker/superset/Dockerfile .

kind-load:
	kind load docker-image $(SPARK_IMAGE)
	kind load docker-image $(AIRFLOW_IMAGE)
	kind load docker-image $(PRODUCER_IMAGE)
	kind load docker-image $(SUPERSET_IMAGE)

aws-secret:
	pwsh -ExecutionPolicy Bypass -File scripts/create_aws_secret.ps1 -Namespace $(K8S_NAMESPACE)

deploy:
	kubectl apply -k infra/k8s

undeploy:
	kubectl delete -k infra/k8s --ignore-not-found=true

validate:
	python scripts/validate_project.py
