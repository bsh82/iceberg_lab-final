# orchestration/dags/lib 디렉토리

Airflow DAG에서 공통으로 사용하는 helper를 보관합니다.

## 핵심 파일

- `spark_application.py`: SparkApplication YAML을 읽고 Kubernetes CustomObjects API로 생성/패치/삭제/상태 대기

## 흐름

```text
Airflow PythonOperator
→ submit_spark_application()
→ Kubernetes API create/patch SparkApplication
→ Spark Operator watch
→ Spark driver/executor pod 생성
```

## 주의점

Spark Operator는 Airflow metadata DB를 읽지 않습니다. Airflow는 Kubernetes API에 SparkApplication 리소스를 제출하고, Spark Operator는 Kubernetes API를 watch하다가 해당 리소스를 발견해 Spark pod를 생성합니다.

