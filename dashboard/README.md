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

## 이미지 산출물

발표와 README에 바로 넣을 수 있는 대시보드 스냅샷 이미지는 `dashboard/snapshots/`에 저장합니다. 이 이미지는 Superset이 조회하는 Gold/Ops SQL과 같은 Trino 결과를 기반으로 생성합니다.

```text
dashboard/snapshots/
├── business_top_campaign_spend.png
├── business_hourly_funnel.png
├── business_conversion_efficiency.png
├── operations_health_latest.png
├── operations_streaming_batches.png
└── data/
    ├── daily_top_campaigns.csv
    ├── hourly_kpis.csv
    ├── conversion_efficiency.csv
    ├── operations_health_latest.csv
    └── operations_streaming_batches.csv
```

`snapshots/data/`의 CSV는 차트 이미지를 만든 근거 데이터입니다. 발표 자료에는 PNG를 넣고, 수치 검증이나 질의 대응이 필요할 때 CSV를 근거로 설명합니다.
