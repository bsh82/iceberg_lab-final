SELECT
  'silver_freshness_minutes' AS metric_name,
  CAST(COALESCE((unix_timestamp(current_timestamp()) - unix_timestamp(max(event_time))) / 60.0, 999999.0) AS DOUBLE) AS metric_value,
  CASE
    WHEN max(event_time) IS NULL THEN 'CRITICAL'
    WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(max(event_time))) / 60.0 > 30 THEN 'CRITICAL'
    WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(max(event_time))) / 60.0 > 10 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat('max_event_time=', CAST(max(event_time) AS STRING)) AS details
FROM ${catalog}.${silver_db}.events;
