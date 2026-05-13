# infra/docker/superset 디렉토리

Superset 커스텀 이미지를 정의합니다.

## 역할

Superset은 Gold KPI와 Ops metric을 시각화합니다. 직접 S3 파일을 읽지 않고 Trino를 통해 Glue/Iceberg table을 조회합니다.

## 설계 고려

Superset 이미지에는 Trino 연결을 위한 Python package가 필요합니다. Dashboard는 데이터 처리 로직을 담지 않고, 이미 계산된 Gold/Ops table을 조회하는 역할에 집중합니다.

클라우드 이전 시에는 Superset을 계속 EKS에서 운영하거나 QuickSight로 대체할 수 있습니다.

