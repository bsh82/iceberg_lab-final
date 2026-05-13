# docs/diagrams 디렉토리

draw.io 구조도를 보관합니다.

## 파일

- `current-local-architecture.drawio`: 현재 로컬 Kubernetes compute + AWS S3/Glue/Iceberg 구조
- `cloud-migration-architecture.drawio`: AWS 완전 마이그레이션 구조

## 그림 해석 기준

- 실선: 실제 데이터 흐름
- 점선: control, orchestration, catalog, metric, maintenance 흐름
- Airflow/MWAA: 데이터를 직접 처리하지 않는 orchestration layer
- Autoscaler: Kafka를 확장하는 컴포넌트가 아니라 Kafka lag와 Spark batch duration을 보고 Spark executor/node 확장을 판단하는 컴포넌트

## 클라우드 구조도 기준

VPC 안에는 네트워크 격리 안에서 실행되는 runtime을 배치합니다. 예를 들어 MSK, EMR on EKS, Airflow/MWAA 연결 영역, autoscaling controller가 여기에 해당합니다.

VPC 밖 AWS Cloud 안에는 S3, Glue, Athena, QuickSight, CloudWatch, IAM 같은 AWS managed service를 배치합니다. 이들은 VPC 내부 서버는 아니지만 VPC endpoint나 IAM을 통해 private하게 접근할 수 있습니다.

