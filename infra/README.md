# infra 디렉토리

로컬 실행 환경과 컨테이너 이미지를 정의합니다.

## 하위 구조

- `docker/`: Spark, Airflow, Producer, Superset 커스텀 이미지
- `k8s/`: 로컬 kind cluster에 배포할 Kubernetes manifest

## 설계 고려

컴퓨트 환경은 로컬 Kubernetes에서 실행하지만, storage와 catalog는 AWS S3/Glue를 사용합니다. 따라서 인프라 코드는 "로컬 재현성"과 "클라우드 이식성"을 동시에 고려합니다.

컨테이너 이미지에는 실행에 필요한 코드와 dependency를 담고, AWS credential은 이미지에 포함하지 않습니다. credential은 Kubernetes Secret으로 mount합니다.

## 클라우드 이전 방향

- Kafka StatefulSet → Amazon MSK 또는 MSK Serverless
- Spark Operator/Spark image → EMR on EKS job image
- Airflow Deployment → MWAA 또는 Airflow on EKS
- Kubernetes Secret `.aws` → IRSA 또는 EKS Pod Identity
- Trino/Superset → Athena/QuickSight 또는 EKS 운영형 Trino/Superset

