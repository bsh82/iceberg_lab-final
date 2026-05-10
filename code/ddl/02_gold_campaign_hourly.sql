CREATE TABLE IF NOT EXISTS ${catalog}.${gold_db}.campaign_hourly_kpis (
  event_date DATE NOT NULL,
  event_hour INT NOT NULL,
  campaign_id BIGINT NOT NULL,
  events BIGINT,
  clicks BIGINT,
  conversions BIGINT,
  attributed_conversions BIGINT,
  spend DOUBLE,
  avg_event_cost DOUBLE,
  unique_users BIGINT,
  ctr_proxy DOUBLE,
  conversion_rate DOUBLE,
  cost_per_attributed_conversion DOUBLE,
  gold_updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date, bucket(32, campaign_id))
TBLPROPERTIES (
  'format-version' = '2',
  'write.distribution-mode' = 'hash',
  'write.target-file-size-bytes' = '134217728',
  'write.parquet.compression-codec' = 'zstd',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '20'
);
