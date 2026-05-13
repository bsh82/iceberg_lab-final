# code/ddl 디렉토리

Glue Catalog namespace와 Iceberg table을 생성하는 DDL을 보관합니다. `bootstrap_catalog.py`가 이 SQL들을 읽어 Spark SQL로 실행합니다.

## 파일 역할

- `00_namespaces.sql`: Silver, Gold, Ops namespace 생성
- `01_silver_events.sql`: 정제 이벤트 fact table
- `02_gold_campaign_hourly.sql`: 시간별 캠페인 KPI table
- `03_gold_campaign_daily.sql`: 일별 캠페인 KPI table
- `04_ops_streaming_batch_metrics.sql`: streaming batch 운영 metric table
- `05_ops_health_check_results.sql`: health check 결과 table
- `athena/bronze_external_table.sql`: Bronze raw zone 조회용 외부 테이블 예시

## 파티셔닝 기준

- Silver: `days(event_time)`, `bucket(64, campaign_id)`
- Gold: `event_date`, `bucket(32, campaign_id)`
- Ops metric: `days(metric_ts)`, `query_name`
- Ops health: `days(run_ts)`, `severity`

Silver와 Gold의 campaign bucket은 campaign_id 원본값 파티션 폭발을 피하면서, 특정 캠페인 backfill, 검증, 재집계의 scan 범위를 줄이기 위한 선택입니다.

## Iceberg 설정

테이블은 `format-version=2`를 사용합니다. 이는 MERGE, row-level operation, snapshot 기반 운영을 전제로 한 선택입니다.

`write.target-file-size-bytes`는 작은 파일 문제를 줄이기 위한 목표 파일 크기입니다. 실제 파일 크기는 micro-batch 크기, Spark task 수, compaction 주기에 따라 달라집니다.

