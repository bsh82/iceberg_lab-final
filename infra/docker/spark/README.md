# infra/docker/spark 디렉토리

Spark pipeline 실행 이미지를 정의합니다.

## 포함해야 하는 것

- Spark 3.5 계열 runtime
- Python pipeline 코드
- Iceberg Spark runtime jar
- AWS bundle / S3 연동 jar
- Spark Kafka source 관련 jar
- Python dependency

## 설계 고려

Spark 이미지는 이 프로젝트의 핵심 실행 단위입니다. Streaming, batch, backfill, maintenance, health check 모두 이 이미지에서 실행됩니다.

GlueCatalog와 S3FileIO를 사용하므로 AWS SDK/jar 호환성이 매우 중요합니다. Spark, Scala, Iceberg, AWS bundle 버전이 맞지 않으면 런타임 오류가 발생할 수 있습니다.

## 클라우드 이전

EMR on EKS로 이전할 때도 이 이미지 구조를 최대한 유지할 수 있습니다. 단, credential은 Kubernetes Secret 대신 IRSA 또는 EKS Pod Identity로 바꿔야 합니다.

