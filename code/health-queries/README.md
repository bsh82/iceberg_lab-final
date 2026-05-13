# code/health-queries 디렉토리

운영자가 파이프라인 상태를 빠르게 판단하기 위한 SQL 모음입니다. `health_check.py`가 이 SQL들을 Spark SQL로 실행하고 결과를 Ops Iceberg table에 저장합니다.

## 쿼리 범주

- Freshness: Silver/Gold가 최근 데이터까지 반영되었는지 확인
- Volume: 일자별 row 수가 비정상적으로 줄거나 늘었는지 확인
- File health: Iceberg 파일 수, 평균 파일 크기, small file 비율 확인
- Metadata health: snapshot 증가량, manifest 수 확인
- Correctness: 중복 `event_id` 확인
- Streaming health: batch duration 확인

## 운영 관점

이 쿼리들은 장애가 발생한 뒤 원인을 찾기 위한 도구이기도 하지만, 더 중요한 목적은 매일 5분 안에 이상 여부를 판단하는 것입니다.

예를 들어 `Silver freshness`는 ingestion 지연을, `small file ratio`는 compaction 필요성을, `duplicate event_id`는 멱등성 설계가 깨졌는지를 보여줍니다.

## 실행 엔진

현재 운영 쿼리는 Trino가 아니라 Spark SQL로 실행합니다. Spark는 이미 GlueCatalog와 Iceberg 설정을 가지고 있고, 결과를 Ops Iceberg table에 append할 수 있기 때문입니다.

