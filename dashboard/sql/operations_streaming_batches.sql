SELECT
  metric_ts,
  query_name,
  batch_id,
  row_count,
  batch_duration_ms,
  status,
  detail
FROM iceberg.ad_attribution_ops.streaming_batch_metrics
WHERE metric_ts >= current_timestamp - INTERVAL '6' hour;
