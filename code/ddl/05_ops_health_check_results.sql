CREATE TABLE IF NOT EXISTS ${catalog}.${ops_db}.health_check_results (
  metric_name STRING NOT NULL,
  metric_value DOUBLE,
  severity STRING,
  details STRING,
  run_ts TIMESTAMP NOT NULL,
  query_file STRING NOT NULL
)
USING iceberg
PARTITIONED BY (days(run_ts), severity)
TBLPROPERTIES (
  'format-version' = '2',
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read',
  'write.target-file-size-bytes' = '67108864',
  'write.parquet.compression-codec' = 'zstd'
);
