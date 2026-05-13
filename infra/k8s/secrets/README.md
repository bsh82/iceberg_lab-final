# infra/k8s/secrets 디렉토리

AWS credential을 Kubernetes Secret으로 주입하는 방법을 설명합니다.

## 원칙

AWS credential은 절대 Git에 커밋하지 않습니다. 로컬에서는 `C:\Users\BaeSangHoon\.aws`의 `iceberg-lab` profile을 Kubernetes Secret으로 만들고, Pod에는 read-only `.aws` 디렉토리로 mount합니다.

## Secret 생성

```powershell
powershell -ExecutionPolicy Bypass -File scripts/create_aws_secret.ps1 -Namespace criteo-lakehouse
```

Pod에서 사용하는 값:

- `AWS_PROFILE=iceberg-lab`
- `AWS_SHARED_CREDENTIALS_FILE=/home/spark/.aws/credentials` 또는 `/opt/airflow/.aws/credentials`
- `AWS_CONFIG_FILE=/home/spark/.aws/config` 또는 `/opt/airflow/.aws/config`

## 클라우드 이전

EMR on EKS에서는 이 Secret을 유지하지 않고 IRSA 또는 EKS Pod Identity로 대체합니다. 애플리케이션 코드는 AWS default credential provider chain을 사용하므로 credential 주입 방식만 바뀌고 Spark 코드 자체는 바뀌지 않아야 합니다.
