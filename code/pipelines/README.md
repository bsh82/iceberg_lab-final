# code/pipelines 디렉토리

Spark 기반 데이터 처리와 운영 제어 로직을 담습니다. 이 디렉토리의 파일들은 대부분 `orchestration/spark-applications/*.yaml`에서 `mainApplicationFile`로 참조됩니다.

## 파일 역할

- `bootstrap_catalog.py`: Glue namespace와 Iceberg table DDL 초기화
- `kafka_to_bronze.py`: Kafka topic을 읽어 Bronze raw S3 Parquet으로 append
- `bronze_to_silver.py`: Bronze payload를 파싱하고 Silver Iceberg에 MERGE
- `silver_to_gold.py`: Silver 이벤트를 Gold campaign KPI로 집계
- `backfill_silver.py`: 과거 Bronze raw를 읽어 Silver를 재처리
- `maintenance.py`: Iceberg compaction, expire snapshots, orphan cleanup
- `health_check.py`: health query 실행 후 Ops table에 결과 저장
- `autoscale_streaming.py`: Kafka lag와 batch duration을 보고 SparkApplication executor 정책 조정

## 실시간 처리

`kafka_to_bronze.py`와 `bronze_to_silver.py`는 Spark Structured Streaming job입니다. 둘을 하나의 Spark job으로 합치지 않고 분리한 이유는 장애 격리와 재처리 가능성 때문입니다.

```text
Kafka to Bronze 실패: Kafka checkpoint와 topic retention 기준 복구
Bronze to Silver 실패: Bronze raw를 보존하므로 Silver만 재처리 가능
```

## 배치 처리

`silver_to_gold.py`, `backfill_silver.py`, `maintenance.py`, `health_check.py`, `bootstrap_catalog.py`는 배치성 Spark job입니다. 비즈니스 집계 배치는 Silver to Gold가 대표이고, 나머지는 운영/복구/초기화 배치입니다.

## 멱등성

- Bronze는 append-only raw zone입니다.
- Silver는 `event_id` 기준 MERGE를 수행합니다.
- Gold는 날짜/시간/campaign grain 기준 MERGE를 수행합니다.
- Backfill은 append가 아니라 동일 key 기반 갱신을 목표로 합니다.

## Autoscaling 해석

`autoscale_streaming.py`는 Kafka를 확장하지 않습니다. Kafka lag와 Spark streaming batch duration을 신호로 읽고 SparkApplication의 executor 정책을 조정합니다.

