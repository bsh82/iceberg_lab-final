# orchestration/spark-applications 디렉토리

Spark Operator가 실행할 SparkApplication YAML을 보관합니다.

## 파일 역할

- `bootstrap-catalog.yaml`: Glue namespace와 Iceberg table 생성
- `bronze-stream.yaml`: Kafka to Bronze streaming
- `silver-stream.yaml`: Bronze to Silver streaming
- `gold-batch.yaml`: Silver to Gold batch aggregation
- `backfill-silver.yaml`: Bronze raw 기반 Silver backfill
- `maintenance.yaml`: Iceberg data file compaction, position delete file compaction, snapshot expiration, orphan cleanup
- `health-check.yaml`: 운영 health query 실행

## Spark pod 생성 방식

SparkApplication은 일반 Kubernetes Job이 아니라 Spark Operator CRD입니다.

```text
SparkApplication 생성
→ Spark Operator 감지
→ Spark driver pod 생성
→ driver가 executor pod 생성
→ Spark 코드 실행
```

Streaming SparkApplication은 생성은 동적이지만 장기 실행됩니다. Batch SparkApplication은 실행 후 Completed 상태가 됩니다.

## 리소스와 확장

로컬 kind에서는 executor pod 수를 늘려도 물리 노드는 고정입니다. EKS 이전 후에는 Spark dynamic allocation과 Karpenter/Cluster Autoscaler를 연결해 executor pod 증가가 node scale-out으로 이어지게 할 수 있습니다.
