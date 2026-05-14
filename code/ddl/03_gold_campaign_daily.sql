CREATE TABLE IF NOT EXISTS ${catalog}.${gold_db}.campaign_daily_kpis (
  event_date DATE NOT NULL,
  campaign_id BIGINT NOT NULL,
  events BIGINT,
  clicks BIGINT,
  conversions BIGINT,
  attributed_conversions BIGINT,
  spend DOUBLE,
  unique_users_approx BIGINT,
  ctr_proxy DOUBLE,
  conversion_rate DOUBLE,
  cost_per_attributed_conversion DOUBLE,
  gold_updated_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (event_date, bucket(32, campaign_id))
TBLPROPERTIES (
  'format-version' = '2',
  'write.merge.mode' = 'merge-on-read',
  'write.update.mode' = 'merge-on-read',
  'write.delete.mode' = 'merge-on-read',
  'write.distribution-mode' = 'hash',
  'write.target-file-size-bytes' = '134217728',
  'write.parquet.compression-codec' = 'zstd',
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max' = '20'
);
