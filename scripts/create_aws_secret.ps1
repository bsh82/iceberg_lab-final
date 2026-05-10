param(
    [string]$Namespace = "criteo-lakehouse",
    [string]$AwsDir = "$HOME\.aws"
)

$ErrorActionPreference = "Stop"

$configPath = Join-Path $AwsDir "config"
$credentialPath = Join-Path $AwsDir "credentials"

if (!(Test-Path $configPath)) {
    throw "AWS config file not found: $configPath"
}
if (!(Test-Path $credentialPath)) {
    throw "AWS credentials file not found: $credentialPath"
}

kubectl create namespace $Namespace --dry-run=client -o yaml | kubectl apply -f -
kubectl -n $Namespace create secret generic aws-credentials `
    --from-file=config=$configPath `
    --from-file=credentials=$credentialPath `
    --dry-run=client -o yaml | kubectl apply -f -

Write-Host "Created/updated secret aws-credentials in namespace $Namespace"
