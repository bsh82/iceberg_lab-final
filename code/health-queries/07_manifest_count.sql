SELECT
  'silver_manifest_count' AS metric_name,
  CAST(count(*) AS DOUBLE) AS metric_value,
  CASE
    WHEN count(*) > 1000 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat('manifests=', CAST(count(*) AS STRING), ', avg_manifest_length_mb=', CAST(COALESCE(avg(length) / 1048576.0, 0.0) AS STRING)) AS details
FROM ${catalog}.${silver_db}.events.manifests;
