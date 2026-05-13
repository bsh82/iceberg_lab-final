# dashboard/sql 디렉토리

Superset virtual dataset 또는 SQL Lab에서 사용할 쿼리를 보관합니다.

## 쿼리 구성

- `business_campaign_hourly.sql`: 시간별 캠페인 KPI
- `business_campaign_daily.sql`: 일별 캠페인 KPI
- `operations_health_latest.sql`: 최신 health check 결과
- `operations_streaming_batches.sql`: streaming micro-batch 운영 지표

## 설계 고려

Dashboard SQL은 복잡한 raw transformation을 수행하지 않습니다. 무거운 정제와 집계는 Spark가 Silver/Gold/Ops 테이블로 미리 계산합니다. BI SQL은 운영자와 분석가가 빠르게 필터링하고 시각화할 수 있는 형태로 유지합니다.

## 지표 해석

광고 도메인에서는 campaign 단위 의사결정이 핵심입니다. 따라서 Gold SQL은 `campaign_id`, `event_date`, `event_hour` 기준 조회가 자연스럽습니다.

Ops SQL은 비즈니스 KPI가 아니라 파이프라인 신뢰성을 보여줍니다. freshness, row count, file size, snapshot growth, batch duration이 핵심입니다.

