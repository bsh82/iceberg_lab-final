# infra/docker/producer 디렉토리

Criteo dataset replay producer 이미지를 정의합니다.

## 역할

이 이미지는 Kubernetes Job으로 실행되어 Criteo TSV/GZ 파일을 읽고 Kafka topic에 JSON 이벤트를 전송합니다.

## 설계 고려

Producer는 장기 실행 서비스가 아니라 한 번 실행하고 종료되는 작업입니다. 따라서 Deployment가 아니라 Kubernetes Job으로 실행합니다.

이벤트 수, 시작 라인, 전송 속도는 실행 인자로 조정할 수 있어야 합니다. 이를 통해 smoke test에서는 100건만 만들고, 발표 데모에서는 더 많은 이벤트를 생성할 수 있습니다.

