from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from code.common.config import load_config
from code.common.schema import bronze_raw_schema
from code.common.spark import get_spark
from code.pipelines.bronze_to_silver import SILVER_COLUMNS, project_silver


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Silver from raw Bronze files and optionally limit event dates.")
    parser.add_argument("--event-start-date", required=True)
    parser.add_argument("--event-end-date", required=True)
    parser.add_argument("--bronze-start-date")
    parser.add_argument("--bronze-end-date")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark("criteo-backfill-silver", cfg)

    bronze = spark.read.schema(bronze_raw_schema).format("parquet").load(cfg.bronze_path)
    if args.bronze_start_date:
        bronze = bronze.where(F.col("ingest_date") >= F.lit(args.bronze_start_date))
    if args.bronze_end_date:
        bronze = bronze.where(F.col("ingest_date") <= F.lit(args.bronze_end_date))

    silver = (
        project_silver(bronze)
        .where(F.col("event_date").between(args.event_start_date, args.event_end_date))
        .dropDuplicates(["event_id"])
    )
    silver.createOrReplaceTempView("silver_backfill_updates")

    update_assignments = ", ".join(f"{column} = source.{column}" for column in SILVER_COLUMNS)
    insert_columns = ", ".join(SILVER_COLUMNS)
    insert_values = ", ".join(f"source.{column}" for column in SILVER_COLUMNS)
    spark.sql(
        f"""
        MERGE INTO {cfg.silver_events_table} AS target
        USING silver_backfill_updates AS source
        ON target.event_id = source.event_id
        WHEN MATCHED THEN UPDATE SET {update_assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
        """
    )


if __name__ == "__main__":
    main()
