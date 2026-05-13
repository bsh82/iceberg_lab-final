# code/common 디렉토리

공통 모듈은 pipeline 간 중복을 줄이고, 로컬 Kubernetes와 EMR on EKS 이식성을 유지하기 위한 기반입니다.

## 파일 역할

- `config.py`: `configs/pipeline.yaml`과 환경 변수를 읽어 Lakehouse 설정을 구성합니다.
- `spark.py`: Iceberg Spark extension, GlueCatalog, S3FileIO, S3A 설정을 포함한 SparkSession을 생성합니다.
- `schema.py`: Producer event schema와 Bronze raw schema를 정의합니다.
- `metrics.py`: streaming batch metric을 Ops Iceberg table에 기록합니다.
- `criteo_columns.py`: Criteo TSV 컬럼 순서를 표준화합니다.

## 설계 고려

`spark.py`는 이 프로젝트의 클라우드 이식성에서 가장 중요한 파일입니다. Spark job이 로컬 pod에서 실행되든 EMR on EKS에서 실행되든, GlueCatalog와 S3 warehouse를 같은 방식으로 바라봐야 합니다.

Schema는 파이프라인 간 계약입니다. Producer가 내보내는 JSON, Bronze가 보존하는 raw schema, Silver가 기대하는 typed schema가 어긋나면 backfill과 health check까지 연쇄적으로 깨질 수 있습니다.

## 멱등성과 운영성

- `metrics.py`는 streaming job의 `batch_id`, `row_count`, `batch_duration_ms`, `status`를 Ops 테이블에 남깁니다.
- 이 metric은 autoscaler와 운영 대시보드의 입력입니다.
- config에서 checkpoint, warehouse, table name을 일관되게 관리해 재시작/재처리 경로를 명확히 합니다.

