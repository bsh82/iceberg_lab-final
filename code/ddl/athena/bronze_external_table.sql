-- Optional Athena/Glue external table for inspecting the raw Bronze zone.
-- Bronze remains append-only raw S3 storage, not an Iceberg table.
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_criteo_attribution_events (
  topic string,
  kafka_partition bigint,
  kafka_offset bigint,
  kafka_timestamp timestamp,
  kafka_key string,
  payload string,
  payload_hash string,
  bronze_ingest_ts timestamp
)
PARTITIONED BY (
  ingest_date string,
  ingest_hour string
)
STORED AS PARQUET
LOCATION '${bronze_path}';
