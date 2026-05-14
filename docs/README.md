# docs 디렉토리

프로젝트 설명을 보조하는 문서와 구조도를 보관합니다.

## 현재 구성

- `diagrams/`: draw.io 아키텍처 구조도

대시보드 스냅샷과 차트 근거 CSV는 BI 산출물 성격이 강하므로 `dashboard/snapshots/`에서 관리합니다.

## 문서화 기준

문서는 단순 사용법보다 의사결정 근거를 남기는 것이 중요합니다. 특히 이 프로젝트에서는 다음 질문에 답할 수 있어야 합니다.

- 왜 Bronze는 raw S3이고 Silver/Gold만 Iceberg인가?
- 왜 Silver와 Gold에 campaign bucket을 두었는가?
- 왜 Airflow는 데이터 경로가 아니라 control plane인가?
- 장애가 발생하면 어디에서 복구하는가?
- 10x/100x 성장 시 어느 계층이 먼저 병목이 되는가?
