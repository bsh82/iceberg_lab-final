# scripts 디렉토리

로컬 개발과 검증을 돕는 스크립트를 보관합니다.

## 파일 역할

- `bootstrap_kind.ps1`: kind cluster 생성, 이미지 빌드/로드, Kubernetes 리소스 배포 보조
- `create_aws_secret.ps1`: 로컬 `.aws` profile을 Kubernetes Secret으로 생성
- `download_dataset.py`: Hugging Face Criteo dataset 다운로드
- `load_dataset_to_pvc.ps1`: dataset을 Kubernetes PVC로 적재
- `validate_project.py`: 필수 파일 존재 여부 등 프로젝트 구조 검증

## 운영 주의점

스크립트는 로컬 bootstrap 편의를 위한 도구입니다. 운영 환경에서는 Terraform/CDK, Helm, GitOps, MWAA/EMR on EKS 배포 파이프라인으로 대체하는 것이 자연스럽습니다.

AWS credential이나 dataset 원본을 스크립트 결과물로 Git에 추가하지 않도록 주의합니다.

