CREATE TABLE IF NOT EXISTS ${catalog}.${ops_db}.streaming_batch_metrics (
  metric_ts TIMESTAMP NOT NULL,
  query_name STRING NOT NULL,
  batch_id BIGINT NOT NULL,
  row_count BIGINT,
  batch_duration_ms BIGINT,
  status STRING,
  detail STRING
)
USING iceberg
PARTITIONED BY (days(metric_ts), query_name)
TBLPROPERTIES (
  'format-version' = '2',
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read',
  'write.target-file-size-bytes' = '67108864',
  'write.parquet.compression-codec' = 'zstd'
);
