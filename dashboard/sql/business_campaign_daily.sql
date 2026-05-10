SELECT
  event_date,
  campaign_id,
  events,
  clicks,
  conversions,
  attributed_conversions,
  spend,
  conversion_rate,
  cost_per_attributed_conversion
FROM iceberg.ad_attribution_gold.campaign_daily_kpis
WHERE event_date >= current_date - INTERVAL '30' day;
