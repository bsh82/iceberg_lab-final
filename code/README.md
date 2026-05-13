# code 디렉토리

이 디렉토리는 프로젝트의 실제 애플리케이션 로직을 담습니다. Dockerfile, Kubernetes YAML, Airflow DAG는 실행 환경을 정의하고, 이곳의 Python/SQL 파일은 데이터가 어떻게 생성, 변환, 검증, 유지보수되는지를 정의합니다.

## 하위 구조

- `common/`: 설정 로딩, SparkSession 생성, schema, metric 기록 공통 모듈
- `producers/`: Criteo 원본 파일을 Kafka 이벤트로 재생하는 producer
- `pipelines/`: Spark Streaming, batch, backfill, maintenance, health check, autoscale 코드
- `ddl/`: Glue Catalog namespace와 Iceberg table DDL
- `health-queries/`: 운영 헬스체크 SQL

## 핵심 설계 고려

- 데이터 처리 로직은 환경에 종속되지 않게 작성했습니다. 로컬 kind와 EMR on EKS 모두 같은 Python entrypoint를 재사용할 수 있어야 합니다.
- AWS/S3/Glue 관련 세부 설정은 코드에 하드코딩하지 않고 `configs/pipeline.yaml`과 환경 변수로 주입합니다.
- Bronze raw와 Silver/Gold Iceberg의 책임을 코드 레벨에서 분리했습니다.
- 멱등성은 `event_id`, checkpoint, Iceberg MERGE로 보장합니다.
- 운영성은 pipeline 내부 metric 기록과 `health-queries/`를 통해 별도 Ops 계층으로 분리합니다.

## 변경 시 주의점

Pipeline 코드를 바꾸면 반드시 다음을 같이 확인해야 합니다.

- `orchestration/spark-applications/*.yaml`의 `mainApplicationFile`
- `code/common/schema.py`의 schema 정의
- `code/ddl/*.sql`의 table schema
- `dashboard/sql/*.sql`의 컬럼 참조
- `code/health-queries/*.sql`의 운영 쿼리

