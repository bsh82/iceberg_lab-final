WITH file_counts AS (
  SELECT
    sum(CASE WHEN CAST(content AS STRING) IN ('1', '2', 'POSITION_DELETES', 'EQUALITY_DELETES') THEN 1 ELSE 0 END) AS delete_files,
    sum(CASE WHEN CAST(content AS STRING) IN ('0', 'DATA') THEN 1 ELSE 0 END) AS data_files,
    sum(CASE WHEN CAST(content AS STRING) IN ('1', '2', 'POSITION_DELETES', 'EQUALITY_DELETES') THEN file_size_in_bytes ELSE 0 END) AS delete_bytes
  FROM ${catalog}.${silver_db}.events.files
)
SELECT
  'silver_delete_file_count' AS metric_name,
  CAST(COALESCE(delete_files, 0) AS DOUBLE) AS metric_value,
  CASE
    WHEN COALESCE(delete_files, 0) > 1000 THEN 'CRITICAL'
    WHEN COALESCE(delete_files, 0) > GREATEST(COALESCE(data_files, 0), 1) * 0.5 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat(
    'delete_files=',
    COALESCE(delete_files, 0),
    ', data_files=',
    COALESCE(data_files, 0),
    ', delete_file_mb=',
    CAST(COALESCE(delete_bytes, 0) / 1048576.0 AS STRING)
  ) AS details
FROM file_counts;
