from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone

from pyspark.sql import functions as F

from code.common.config import load_config
from code.common.spark import get_spark


def _date_range(start: date, end: date) -> list[str]:
    days: list[str] = []
    current = start
    while current <= end:
        days.append(current.isoformat())
        current += timedelta(days=1)
    return days


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate Silver events into Gold campaign KPIs.")
    parser.add_argument("--start-date", help="Inclusive yyyy-mm-dd. Defaults to yesterday UTC.")
    parser.add_argument("--end-date", help="Inclusive yyyy-mm-dd. Defaults to start-date.")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark("criteo-silver-to-gold", cfg)

    default_day = (datetime.now(timezone.utc).date() - timedelta(days=1))
    start = date.fromisoformat(args.start_date) if args.start_date else default_day
    end = date.fromisoformat(args.end_date) if args.end_date else start
    days = _date_range(start, end)

    silver = spark.table(cfg.silver_events_table).where(F.col("event_date").isin(days))

    hourly = (
        silver.groupBy("event_date", "event_hour", "campaign_id")
        .agg(
            F.count("*").alias("events"),
            F.sum("click").alias("clicks"),
            F.sum("conversion").alias("conversions"),
            F.sum("attribution").alias("attributed_conversions"),
            F.sum("cost").alias("spend"),
            F.avg("cost").alias("avg_event_cost"),
            F.approx_count_distinct("user_id").alias("unique_users"),
        )
        .withColumn("ctr_proxy", F.col("clicks") / F.when(F.col("events") == 0, None).otherwise(F.col("events")))
        .withColumn("conversion_rate", F.col("conversions") / F.when(F.col("clicks") == 0, None).otherwise(F.col("clicks")))
        .withColumn(
            "cost_per_attributed_conversion",
            F.col("spend") / F.when(F.col("attributed_conversions") == 0, None).otherwise(F.col("attributed_conversions")),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
    )

    hourly.createOrReplaceTempView("campaign_hourly_updates")
    spark.sql(
        f"""
        MERGE INTO {cfg.gold_hourly_table} AS target
        USING campaign_hourly_updates AS source
        ON target.event_date = source.event_date
           AND target.event_hour = source.event_hour
           AND target.campaign_id = source.campaign_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )

    daily = (
        spark.table(cfg.gold_hourly_table)
        .where(F.col("event_date").isin(days))
        .groupBy("event_date", "campaign_id")
        .agg(
            F.sum("events").alias("events"),
            F.sum("clicks").alias("clicks"),
            F.sum("conversions").alias("conversions"),
            F.sum("attributed_conversions").alias("attributed_conversions"),
            F.sum("spend").alias("spend"),
            F.sum("unique_users").alias("unique_users_approx"),
        )
        .withColumn("ctr_proxy", F.col("clicks") / F.when(F.col("events") == 0, None).otherwise(F.col("events")))
        .withColumn("conversion_rate", F.col("conversions") / F.when(F.col("clicks") == 0, None).otherwise(F.col("clicks")))
        .withColumn(
            "cost_per_attributed_conversion",
            F.col("spend") / F.when(F.col("attributed_conversions") == 0, None).otherwise(F.col("attributed_conversions")),
        )
        .withColumn("gold_updated_at", F.current_timestamp())
    )

    daily.createOrReplaceTempView("campaign_daily_updates")
    spark.sql(
        f"""
        MERGE INTO {cfg.gold_daily_table} AS target
        USING campaign_daily_updates AS source
        ON target.event_date = source.event_date
           AND target.campaign_id = source.campaign_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


if __name__ == "__main__":
    main()
