# infra/k8s 디렉토리

로컬 kind cluster에 배포할 Kubernetes 리소스를 정의합니다.

## 구성

- `base/`: namespace, RBAC, Kafka, Postgres, Airflow, Trino, Superset, Producer, Autoscaler manifest
- `secrets/`: AWS credential secret 생성 안내
- `kustomization.yaml`: base 리소스를 묶는 Kustomize 진입점
- `spark-operator-values.yaml`: Spark Operator Helm values

## 설계 고려

로컬 환경에서는 모든 compute 컴포넌트를 하나의 Kubernetes namespace에 배치합니다. 다만 S3와 Glue는 Kubernetes 안이 아니라 AWS managed service입니다.

Spark 작업은 Deployment가 아니라 SparkApplication으로 생성됩니다. Airflow는 SparkApplication을 Kubernetes API에 제출하고, Spark Operator가 driver/executor pod를 동적으로 생성합니다.

## 클라우드 이전

EKS로 이전할 때도 namespace, RBAC, service account, image, config mount 개념은 유지됩니다. 단 Kafka, credential, autoscaling, BI는 managed service로 대체될 수 있습니다.

