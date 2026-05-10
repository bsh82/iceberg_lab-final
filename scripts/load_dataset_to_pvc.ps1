param(
    [string]$Namespace = "criteo-lakehouse",
    [string]$DatasetPath = "data\criteo_attribution_dataset.tsv.gz"
)

$ErrorActionPreference = "Stop"

if (!(Test-Path $DatasetPath)) {
    throw "Dataset file not found: $DatasetPath. Run python scripts/download_dataset.py --output-dir data first."
}

$manifest = @"
apiVersion: v1
kind: Pod
metadata:
  name: dataset-loader
spec:
  restartPolicy: Never
  containers:
    - name: loader
      image: busybox:1.36
      command: ["sh", "-c", "sleep 3600"]
      volumeMounts:
        - name: dataset
          mountPath: /data
  volumes:
    - name: dataset
      persistentVolumeClaim:
        claimName: criteo-dataset
"@

$manifest | kubectl -n $Namespace apply -f -

kubectl -n $Namespace wait --for=condition=Ready pod/dataset-loader --timeout=120s
kubectl -n $Namespace cp $DatasetPath dataset-loader:/data/criteo_attribution_dataset.tsv.gz
kubectl -n $Namespace delete pod dataset-loader

Write-Host "Loaded dataset into PVC criteo-dataset"
