# infra/k8s/base 디렉토리

Kubernetes 기본 리소스 manifest를 보관합니다.

## 파일 역할

- `00-namespace.yaml`: `criteo-lakehouse` namespace 생성
- `10-rbac.yaml`: Spark와 Airflow가 SparkApplication/Pod를 조작하기 위한 권한
- `20-kafka.yaml`: 로컬 Kafka StatefulSet
- `30-postgres.yaml`: Airflow metadata DB
- `40-airflow.yaml`: Airflow webserver/scheduler
- `50-trino.yaml`: Iceberg table 조회용 SQL gateway
- `60-superset.yaml`: BI dashboard
- `70-producer.yaml`: Producer CronJob template
- `80-autoscaler-cronjob.yaml`: Kafka lag/batch duration 기반 autoscaler CronJob

## 리소스 타입 기준

- Producer 실행은 Kubernetes Job입니다. Airflow가 CronJob template에서 Job을 생성합니다.
- Autoscaler는 Kubernetes CronJob입니다.
- Kafka와 Postgres는 상태가 있으므로 StatefulSet입니다.
- Airflow, Trino, Superset은 Deployment입니다.
- Spark pipeline은 여기의 기본 manifest가 아니라 `orchestration/spark-applications/`의 SparkApplication으로 실행됩니다.

## 주의점

로컬 Kafka는 단일 broker, replication factor 1에 가깝기 때문에 운영 수준의 내구성을 제공하지 않습니다. 운영 이전 시 MSK 또는 MSK Serverless로 대체해야 합니다.

