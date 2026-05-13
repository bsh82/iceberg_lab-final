# orchestration/dags 디렉토리

Airflow DAG 정의를 보관합니다.

## DAG 목록

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

## 설계 고려

Bronze 적재와 Silver 변환은 같은 `criteo_streaming_control_plane` DAG에 묶여 있지만, 같은 Spark job은 아닙니다. 각각 별도 SparkApplication입니다. 같은 DAG에 둔 이유는 streaming stack의 기동 순서와 의존성을 한 곳에서 관리하기 위해서입니다.

Backfill DAG는 Silver 복구와 Gold 재집계를 한 흐름에 묶습니다. 정제 로직 버그를 수정한 뒤 대시보드 정합성까지 회복하는 운영 시나리오를 반영합니다.

