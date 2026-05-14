WITH data_files AS (
  SELECT file_size_in_bytes
  FROM ${catalog}.${silver_db}.events.files
  WHERE CAST(content AS STRING) IN ('0', 'DATA')
)
SELECT
  'silver_avg_file_size_mb' AS metric_name,
  CAST(COALESCE(avg(file_size_in_bytes) / 1048576.0, 0.0) AS DOUBLE) AS metric_value,
  CASE
    WHEN count(*) = 0 THEN 'CRITICAL'
    WHEN avg(file_size_in_bytes) < 16 * 1024 * 1024 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat('files=', count(*), ', avg_file_size_mb=', CAST(COALESCE(avg(file_size_in_bytes) / 1048576.0, 0.0) AS STRING)) AS details
FROM data_files;
