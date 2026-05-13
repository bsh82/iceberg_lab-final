# code/producers 디렉토리

Criteo 원본 TSV/GZ 파일을 Kafka 이벤트처럼 재생하는 producer 코드입니다.

## 실행 방식

Producer 자체는 Airflow task 안에서 직접 실행되지 않습니다. Airflow가 Kubernetes Job을 만들고, 그 Job의 Pod가 producer 코드를 실행합니다.

```text
Airflow DAG
→ Kubernetes Job
→ Producer Pod
→ Criteo Dataset
→ Kafka topic
```

## 이벤트 생성 방식

`criteo_kafka_producer.py`는 Criteo row를 읽어 JSON 이벤트로 변환합니다.

주요 변환:

- TSV 컬럼을 `timestamp`, `uid`, `campaign`, `click`, `conversion`, `attribution`, `cost` 등으로 매핑
- 원본 timestamp를 현재 실행 시점 기준 event_time으로 재해석
- 주요 필드 기반 `event_id` 생성
- Kafka key를 `event_id`로 설정

## 멱등성 고려

같은 원본 row를 같은 방식으로 재생하면 같은 `event_id`가 생성됩니다. 이는 Silver MERGE와 결합해 재처리 시 중복 insert를 줄이는 장치입니다.

다만 같은 데이터셋 구간을 여러 번 "새 이벤트"처럼 쌓고 싶다면 `start-line`을 다르게 주거나 event_id 생성 정책에 replay run id를 포함해야 합니다.

