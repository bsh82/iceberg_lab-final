SELECT
  'silver_snapshots_last_24h' AS metric_name,
  CAST(count(*) AS DOUBLE) AS metric_value,
  CASE
    WHEN count(*) > 300 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat('latest_snapshot_id=', CAST(max(snapshot_id) AS STRING), ', snapshots_24h=', CAST(count(*) AS STRING)) AS details
FROM ${catalog}.${silver_db}.events.snapshots
WHERE committed_at >= current_timestamp() - INTERVAL 24 HOURS;
