param(
    [string]$ClusterName = "criteo-lakehouse",
    [string]$Namespace = "criteo-lakehouse"
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)]
        [string]$File,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Arguments
    )

    & $File @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $File $($Arguments -join ' ')"
    }
}

Push-Location $ProjectRoot
try {
    $clusters = & kind get clusters
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to list kind clusters. Is kind installed and available in PATH?"
    }

    if ($clusters -notcontains $ClusterName) {
        Invoke-Native kind create cluster --name $ClusterName
    }

    Invoke-Native docker build -t criteo-spark:local -f infra/docker/spark/Dockerfile .
    Invoke-Native docker build -t criteo-airflow:local -f infra/docker/airflow/Dockerfile .
    Invoke-Native docker build -t criteo-producer:local -f infra/docker/producer/Dockerfile .
    Invoke-Native docker build -t criteo-superset:local -f infra/docker/superset/Dockerfile .
    Invoke-Native docker pull apache/kafka:3.7.1

    Invoke-Native kind load docker-image criteo-spark:local --name $ClusterName
    Invoke-Native kind load docker-image criteo-airflow:local --name $ClusterName
    Invoke-Native kind load docker-image criteo-producer:local --name $ClusterName
    Invoke-Native kind load docker-image criteo-superset:local --name $ClusterName
    Invoke-Native kind load docker-image apache/kafka:3.7.1 --name $ClusterName

    & (Join-Path $PSScriptRoot "create_aws_secret.ps1") -Namespace $Namespace
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create AWS credentials secret."
    }

    Invoke-Native kubectl apply -k infra/k8s

    Write-Host "Base stack applied. Install the Spark Operator before submitting SparkApplication resources:"
    Write-Host "helm repo add spark-operator https://kubeflow.github.io/spark-operator"
    Write-Host "helm upgrade --install spark-operator spark-operator/spark-operator --namespace spark-operator --create-namespace -f infra/k8s/spark-operator-values.yaml"
}
finally {
    Pop-Location
}
