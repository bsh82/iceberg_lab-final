# orchestration 디렉토리

Airflow DAG와 SparkApplication YAML을 보관합니다. 이 디렉토리는 "무엇을 언제 어떤 순서로 실행할 것인가"를 정의합니다.

## 하위 구조

- `dags/`: Airflow DAG
- `spark-applications/`: Spark Operator가 실행할 SparkApplication manifest

## 핵심 원칙

Airflow는 데이터를 직접 처리하지 않습니다. Airflow는 Kubernetes API를 통해 Kubernetes Job 또는 SparkApplication을 제출하고 상태를 추적합니다.

실제 데이터 처리는 다음 Pod에서 수행됩니다.

- Producer Pod
- Spark driver pod
- Spark executor pod

## 운영 관점

Streaming DAG는 장기 실행 SparkApplication을 시작/재시작하는 control plane입니다. Batch DAG는 실행 후 종료되는 SparkApplication을 제출하고 완료 상태를 기다립니다.

