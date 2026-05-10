SELECT
  'silver_small_file_ratio' AS metric_name,
  CAST(COALESCE(sum(CASE WHEN file_size_in_bytes < 32 * 1024 * 1024 THEN 1 ELSE 0 END) / count(*), 0.0) AS DOUBLE) AS metric_value,
  CASE
    WHEN count(*) = 0 THEN 'CRITICAL'
    WHEN sum(CASE WHEN file_size_in_bytes < 32 * 1024 * 1024 THEN 1 ELSE 0 END) / count(*) > 0.7 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat(
    'small_files=',
    sum(CASE WHEN file_size_in_bytes < 32 * 1024 * 1024 THEN 1 ELSE 0 END),
    ', total_files=',
    count(*)
  ) AS details
FROM ${catalog}.${silver_db}.events.files;
