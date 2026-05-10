SELECT
  'gold_freshness_minutes' AS metric_name,
  CAST(COALESCE((unix_timestamp(current_timestamp()) - unix_timestamp(max(gold_updated_at))) / 60.0, 999999.0) AS DOUBLE) AS metric_value,
  CASE
    WHEN max(gold_updated_at) IS NULL THEN 'CRITICAL'
    WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(max(gold_updated_at))) / 60.0 > 180 THEN 'CRITICAL'
    WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(max(gold_updated_at))) / 60.0 > 90 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat('max_gold_updated_at=', CAST(max(gold_updated_at) AS STRING)) AS details
FROM ${catalog}.${gold_db}.campaign_hourly_kpis;
