# AI 작업 컨텍스트

이 문서는 이 프로젝트를 이어서 작업하는 AI나 새 팀원이 빠르게 구조를 이해하도록 만든 요약 문서입니다. 최상위 `README.md`는 발표/평가용 설명이고, 이 파일은 코드 수정 전 반드시 확인해야 할 엔지니어링 맥락입니다.

## 프로젝트 목적

`criteo/criteo-attribution-dataset`을 광고 이벤트 스트림처럼 재생하고, Kafka, Spark, S3, Glue, Iceberg, Airflow, Trino, Superset을 사용해 로컬 Kubernetes 기반 Lakehouse를 구현합니다.

핵심 목표는 단순 ETL이 아니라 다음을 보여주는 것입니다.

- Bronze / Silver / Gold 메달리온 아키텍처
- Silver / Gold Iceberg 테이블 운영
- Kafka + Spark Structured Streaming 기반 실시간 처리
- Airflow를 통한 Producer, Streaming, Batch, Backfill, Maintenance, Health Check 오케스트레이션
- 멱등성, 재처리 가능성, 백필, 운영 헬스체크
- 로컬 kind 환경에서 시작하되 EMR on EKS로 이식 가능한 구조

## 절대 지켜야 할 설계 원칙

- Bronze는 Iceberg가 아니라 S3 raw Parquet입니다.
- Silver와 Gold만 Iceberg 테이블입니다.
- Airflow는 데이터를 직접 처리하지 않습니다. Kubernetes Job 또는 SparkApplication을 제출하는 control plane입니다.
- Producer는 Airflow가 지시하지만 실제 실행 단위는 Kubernetes Job입니다.
- Spark 작업은 SparkApplication으로 선언되고 Spark Operator가 driver/executor pod를 생성합니다.
- 운영 쿼리는 Spark SQL로 실행해 Ops Iceberg table에 저장하고, Trino/Superset은 그 결과를 조회합니다.
- AWS credential 파일은 커밋 금지입니다. 로컬에서는 Kubernetes Secret으로 mount하고, 클라우드에서는 IRSA/EKS Pod Identity로 대체해야 합니다.

## 데이터 흐름

```text
Criteo Dataset
→ Kafka Producer Kubernetes Job
→ Kafka topic: criteo-attribution-events
→ Spark Streaming: Kafka to Bronze
→ S3 Bronze raw Parquet
→ Spark Streaming / Backfill: Bronze to Silver
→ Silver Iceberg events
→ Spark Batch: Silver to Gold
→ Gold Iceberg campaign KPI tables
→ Trino / Superset
```

## Airflow 오케스트레이션 흐름

```text
criteo_producer_control_plane
→ Kubernetes Job
→ Producer Pod
→ Criteo Dataset
→ Kafka

criteo_streaming_control_plane
→ bootstrap SparkApplication
→ kafka-to-bronze SparkApplication
→ bronze-to-silver SparkApplication
→ Glue Catalog / Bronze S3 / Silver Iceberg

criteo_gold_daily_aggregation
→ gold-batch SparkApplication
→ Spark Batch Pod
→ Silver Iceberg
→ Gold Iceberg

criteo_backfill_and_gold_rebuild
→ backfill-silver SparkApplication
→ Spark Batch Pod
→ Bronze S3
→ Silver Iceberg
→ gold-batch SparkApplication
→ Gold Iceberg

criteo_iceberg_maintenance
→ maintenance SparkApplication
→ Spark Batch Pod
→ Iceberg Compaction
→ Expire Snapshots
→ Orphan Cleanup

criteo_operational_health_check
→ health-check SparkApplication
→ Spark Batch Pod
→ Health SQL
→ Ops Iceberg Tables
→ Superset Ops Dashboard
```

## 멱등성 설계

멱등성의 중심은 `event_id + Iceberg MERGE + checkpoint + raw 보존`입니다.

- Producer는 원본 row의 주요 필드를 기반으로 `event_id`를 생성합니다.
- Bronze는 raw payload와 Kafka metadata를 보존합니다.
- Kafka to Bronze는 Spark checkpoint로 Kafka offset 진행 상태를 보존합니다.
- Bronze to Silver는 Spark checkpoint로 처리한 Bronze 파일 진행 상태를 보존합니다.
- Silver는 `event_id` 기준 Iceberg MERGE를 사용합니다.
- Gold는 집계 grain 기준 MERGE를 사용합니다.
- Backfill은 Bronze raw를 다시 읽고 같은 key로 Silver/Gold를 갱신합니다.

중요한 표현:

```text
중복이 절대 발생하지 않는다는 보장이 아니라,
중복 또는 재처리가 발생해도 최종 Silver/Gold 결과가 멱등적으로 수렴하도록 설계한다.
```

## 파티셔닝과 버킷팅

- Bronze: `ingest_date`, `ingest_hour`
- Silver: `days(event_time)`, `bucket(64, campaign_id)`
- Gold: `event_date`, `bucket(32, campaign_id)`
- Ops streaming metrics: `days(metric_ts)`, `query_name`
- Ops health results: `days(run_ts)`, `severity`

Bronze는 적재/복구 범위를 관리하기 위해 ingest time 기준입니다. Silver/Gold는 광고 분석의 기본 축인 이벤트 날짜와 캠페인 기준 조회를 고려합니다. `campaign_id`를 원본값 파티션으로 쓰지 않고 bucket transform을 쓰는 이유는 high-cardinality 파티션 폭발을 막기 위해서입니다.

## 주요 디렉토리

- `configs/`: 공통 환경 설정
- `code/common/`: Spark, config, schema, metrics 공통 모듈
- `code/producers/`: Criteo replay producer
- `code/pipelines/`: Spark streaming, batch, maintenance, health, autoscale 코드
- `code/ddl/`: Glue/Iceberg namespace와 table DDL
- `code/health-queries/`: 운영 헬스체크 SQL
- `orchestration/dags/`: Airflow DAG
- `orchestration/spark-applications/`: SparkApplication YAML
- `infra/k8s/`: 로컬 Kubernetes 매니페스트
- `infra/docker/`: 커스텀 이미지 Dockerfile
- `dashboard/`: Superset 대시보드 정의와 SQL
- `docs/diagrams/`: draw.io 구조도

## 로컬과 클라우드 매핑

| 로컬 | 클라우드 이전 |
|---|---|
| kind Kubernetes | Amazon EKS |
| Spark Operator | EMR on EKS 또는 Spark Operator on EKS |
| Kafka StatefulSet | Amazon MSK 또는 MSK Serverless |
| Airflow on K8s | MWAA 또는 Airflow on EKS |
| Kubernetes Secret `.aws` mount | IRSA 또는 EKS Pod Identity |
| Trino/Superset local | Athena/QuickSight 또는 Trino/Superset on EKS |
| S3/Glue/Iceberg | 그대로 유지 |

## AI가 수정할 때 주의할 점

- `.git`, `.venv`, `data`, `artifacts`는 작업 대상에서 제외합니다.
- AWS credential, kubeconfig, 실제 dataset 원본은 문서화만 하고 커밋하지 않습니다.
- Bronze를 Iceberg로 바꾸면 요구사항과 현재 설계가 깨집니다.
- Silver/Gold table 이름과 catalog 설정은 `configs/pipeline.yaml`과 `code/common/config.py`를 함께 확인해야 합니다.
- SparkApplication YAML을 바꿀 때는 Airflow DAG가 참조하는 파일명과 `metadata.name`을 함께 확인해야 합니다.
- Backfill과 snapshot expiration 정책은 서로 충돌할 수 있으므로 retention을 짧게 줄이면 안 됩니다.
- Autoscaler는 Kafka를 확장하는 것이 아니라 Kafka lag와 batch duration을 읽어 Spark executor 정책을 조정하는 컴포넌트입니다.

