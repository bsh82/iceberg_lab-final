# dashboard 디렉토리

Superset 대시보드 정의와 조회 SQL을 보관합니다. BI 계층은 Gold KPI와 Ops metric을 사람이 빠르게 확인하는 마지막 소비 계층입니다.

## 역할

- Business KPI: 캠페인별 spend, clicks, conversions, attributed conversions, conversion rate, cost per attributed conversion
- Operations: Silver/Gold freshness, file count, average file size, small file ratio, snapshot count, manifest count, streaming batch duration

Superset은 직접 S3 파일을 읽지 않습니다. Trino를 SQL gateway로 사용하고, Trino는 Glue Catalog에 등록된 Iceberg 테이블을 조회합니다.

## 로컬 접속

```text
http://localhost:8088
```

기본 계정:

```text
admin / admin
```

Port-forward:

```powershell
kubectl -n criteo-lakehouse port-forward svc/superset 8088:8088
kubectl -n criteo-lakehouse port-forward svc/trino 8081:8080
```

Superset SQLAlchemy URI:

```text
trino://admin@trino.criteo-lakehouse.svc.cluster.local:8080/iceberg
```

## 설계 고려

Gold 테이블은 dashboard serving을 위해 미리 집계됩니다. Superset이 매번 Silver fact table 전체를 스캔하지 않도록 `campaign_hourly_kpis`, `campaign_daily_kpis`를 사용합니다.

Ops 탭은 운영자가 5분 안에 파이프라인 상태를 확인하는 목적입니다. `ops.health_check_results`와 `ops.streaming_batch_metrics`를 중심으로 구성합니다.
