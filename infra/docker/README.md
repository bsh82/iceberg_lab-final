# infra/docker 디렉토리

로컬 Kubernetes에서 사용할 커스텀 이미지를 빌드하는 Dockerfile들을 보관합니다.

## 이미지 역할

- `spark/`: Spark pipeline 실행 이미지
- `airflow/`: DAG와 SparkApplication YAML을 포함한 Airflow 이미지
- `producer/`: Criteo dataset replay producer 이미지
- `superset/`: Trino SQLAlchemy connector를 포함한 Superset 이미지

## 설계 고려

이미지에는 코드와 dependency만 포함합니다. AWS credential, dataset 원본, kubeconfig는 이미지에 넣지 않습니다.

Spark 이미지는 Iceberg, AWS Glue, S3, Kafka 연동 jar가 필요합니다. 이 부분이 깨지면 GlueCatalog, S3FileIO, Kafka source가 모두 영향을 받습니다.

## 변경 시 주의점

Dockerfile을 바꾸면 `scripts/bootstrap_kind.ps1`의 이미지 빌드/로드 흐름도 함께 확인해야 합니다.

