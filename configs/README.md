# configs 디렉토리

파이프라인 전체의 환경 설정을 보관합니다. 현재 핵심 파일은 `pipeline.yaml`입니다.

## 주요 설정

- AWS profile, region, bucket
- S3 warehouse, Bronze path, checkpoint path
- Glue/Iceberg catalog와 database 이름
- Kafka bootstrap server, topic, consumer group
- Streaming trigger, watermark, maxOffsetsPerTrigger
- Iceberg maintenance retention
- Autoscaling threshold

## 설계 의도

코드에 환경 값을 하드코딩하지 않고 설정 파일과 환경 변수로 분리했습니다. 이 덕분에 로컬 kind에서 EMR on EKS로 이전할 때 코드를 크게 바꾸지 않고 실행 환경 설정만 바꿀 수 있습니다.

## 주의점

`checkpoint_path`는 streaming job의 복구 기준입니다. 경로를 바꾸면 Spark는 기존 진행 상태를 잃고 다시 읽을 수 있습니다.

`warehouse`와 `catalog`는 Silver/Gold/Ops Iceberg 테이블의 정체성을 결정합니다. 임의 변경 시 Glue metadata와 S3 metadata path가 어긋날 수 있습니다.

