# AWS credentials secret

Do not commit AWS credentials.

Create a local Kubernetes secret from the existing `iceberg-lab` profile:

```powershell
pwsh -ExecutionPolicy Bypass -File scripts/create_aws_secret.ps1 -Namespace criteo-lakehouse
```

The pods mount the secret as a read-only `.aws` directory and use:

- `AWS_PROFILE=iceberg-lab`
- `AWS_SHARED_CREDENTIALS_FILE=/home/spark/.aws/credentials` or `/opt/airflow/.aws/credentials`
- `AWS_CONFIG_FILE=/home/spark/.aws/config` or `/opt/airflow/.aws/config`

On EMR on EKS, replace this secret with IRSA or EKS Pod Identity. Application code should not change.
