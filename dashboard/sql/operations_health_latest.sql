WITH ranked AS (
  SELECT
    metric_name,
    metric_value,
    severity,
    details,
    run_ts,
    row_number() OVER (PARTITION BY metric_name ORDER BY run_ts DESC) AS rn
  FROM iceberg.ad_attribution_ops.health_check_results
)
SELECT
  metric_name,
  metric_value,
  severity,
  details,
  run_ts
FROM ranked
WHERE rn = 1;
