WITH duplicates AS (
  SELECT event_id, count(*) AS rows
  FROM ${catalog}.${silver_db}.events
  WHERE event_date >= current_date() - INTERVAL 7 DAYS
  GROUP BY event_id
  HAVING count(*) > 1
)
SELECT
  'silver_duplicate_event_ids_7d' AS metric_name,
  CAST(count(*) AS DOUBLE) AS metric_value,
  CASE
    WHEN count(*) > 0 THEN 'CRITICAL'
    ELSE 'OK'
  END AS severity,
  concat('duplicate_event_ids=', CAST(count(*) AS STRING)) AS details
FROM duplicates;
