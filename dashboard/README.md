# Superset dashboard guide

Superset should connect to Trino, and Trino should query the Iceberg tables registered in AWS Glue.

Local URL:

```text
http://localhost:8088
```

Default local account:

```text
admin / admin
```

Port-forward:

```powershell
kubectl -n criteo-lakehouse port-forward svc/superset 8088:8088
kubectl -n criteo-lakehouse port-forward svc/trino 8081:8080
```

Superset SQLAlchemy URI:

```text
trino://admin@trino.criteo-lakehouse.svc.cluster.local:8080/iceberg
```

Recommended tabs:

- Business KPI: spend, clicks, conversions, attributed conversions, conversion rate, cost per attributed conversion by campaign and day/hour.
- Operations: Silver freshness, Gold freshness, file count, average file size, small file ratio, snapshot count, manifest count, streaming batch duration.

Use the SQL snippets in `dashboard/sql/` as virtual datasets.
