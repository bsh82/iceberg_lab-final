WITH daily AS (
  SELECT event_date, count(*) AS rows
  FROM ${catalog}.${silver_db}.events
  WHERE event_date >= current_date() - INTERVAL 7 DAYS
  GROUP BY event_date
),
stats AS (
  SELECT
    count(*) AS active_days,
    min(rows) AS min_rows,
    max(rows) AS max_rows,
    avg(rows) AS avg_rows
  FROM daily
)
SELECT
  'silver_daily_row_count_min_7d' AS metric_name,
  CAST(COALESCE(min_rows, 0) AS DOUBLE) AS metric_value,
  CASE
    WHEN active_days = 0 THEN 'CRITICAL'
    WHEN min_rows < avg_rows * 0.3 THEN 'WARN'
    ELSE 'OK'
  END AS severity,
  concat('active_days=', active_days, ', min_rows=', min_rows, ', avg_rows=', CAST(avg_rows AS BIGINT), ', max_rows=', max_rows) AS details
FROM stats;
