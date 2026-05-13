# infra/docker/airflow 디렉토리

Airflow 커스텀 이미지를 정의합니다.

## 포함 내용

- Airflow base image
- Kubernetes provider
- `kubectl`
- `orchestration/dags`
- `orchestration/spark-applications`
- `configs`

## 설계 고려

Airflow는 데이터 처리를 직접 수행하지 않습니다. 이 이미지가 필요한 이유는 DAG가 Kubernetes API에 접근해 Producer Job과 SparkApplication을 제출해야 하기 때문입니다.

Airflow 컨테이너가 heavy data processing을 수행하지 않도록 유지하는 것이 중요합니다. 데이터 처리는 Producer Pod 또는 Spark driver/executor pod에서 수행됩니다.

