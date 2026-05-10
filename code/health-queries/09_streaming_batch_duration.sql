SELECT
  'streaming_max_batch_duration_ms_15m' AS metric_name,
  CAST(COALESCE(max(batch_duration_ms), 0) AS DOUBLE) AS metric_value,
  CASE
    WHEN max(batch_duration_ms) > 120000 THEN 'CRITICAL'
    WHEN max(batch_duration_ms) > 60000 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat('recent_batches=', CAST(count(*) AS STRING), ', failed_batches=', CAST(sum(CASE WHEN status <> 'success' THEN 1 ELSE 0 END) AS STRING)) AS details
FROM ${catalog}.${ops_db}.streaming_batch_metrics
WHERE metric_ts >= current_timestamp() - INTERVAL 15 MINUTES;
