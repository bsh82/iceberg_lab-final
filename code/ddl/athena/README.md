# code/ddl/athena 디렉토리

Athena 또는 외부 SQL 엔진에서 Bronze raw zone을 조회하기 위한 보조 DDL을 둡니다.

## Bronze가 Iceberg가 아닌 이유

Bronze는 원본 보존 계층입니다. Kafka payload, topic, partition, offset, timestamp를 최대한 그대로 S3 Parquet에 append합니다. 이 계층은 분석 최적화보다 장애 복구와 재처리 가능성이 더 중요합니다.

## 사용 목적

- Bronze raw 파일이 정상 적재되었는지 빠르게 확인
- Kafka offset, payload hash, ingest partition 검증
- Silver 로직 버그 발생 시 원본 데이터 조사

## 주의점

`bronze_external_table.sql`은 운영 테이블의 중심이 아니라 검사/탐색용입니다. 메인 Lakehouse table management는 Silver/Gold Iceberg에서 수행합니다.

