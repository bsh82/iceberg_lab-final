from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from code.common.config import load_config
from code.common.metrics import record_streaming_metric
from code.common.schema import bronze_raw_schema, producer_event_schema
from code.common.spark import get_spark


SILVER_COLUMNS = [
    "event_id",
    "event_time",
    "event_date",
    "event_hour",
    "source_timestamp",
    "user_id",
    "campaign_id",
    "conversion",
    "conversion_timestamp",
    "conversion_id",
    "attribution",
    "click",
    "click_pos",
    "click_nb",
    "cost",
    "cpo",
    "time_since_last_click",
    "cat1",
    "cat2",
    "cat3",
    "cat4",
    "cat5",
    "cat6",
    "cat7",
    "cat8",
    "cat9",
    "producer_ingest_ts",
    "bronze_ingest_ts",
    "silver_ingest_ts",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "payload_hash",
]


def project_silver(bronze_df: DataFrame) -> DataFrame:
    parsed = bronze_df.withColumn("event", F.from_json(F.col("payload"), producer_event_schema))

    return (
        parsed.where(F.col("event.event_id").isNotNull())
        .where(F.col("event.event_time").isNotNull())
        .select(
            F.col("event.event_id").alias("event_id"),
            F.col("event.event_time").cast("timestamp").alias("event_time"),
            F.to_date(F.col("event.event_time")).alias("event_date"),
            F.hour(F.col("event.event_time")).cast("int").alias("event_hour"),
            F.col("event.source_timestamp").cast("long").alias("source_timestamp"),
            F.col("event.uid").cast("long").alias("user_id"),
            F.col("event.campaign").cast("long").alias("campaign_id"),
            F.col("event.conversion").cast("int").alias("conversion"),
            F.col("event.conversion_timestamp").cast("long").alias("conversion_timestamp"),
            F.col("event.conversion_id").cast("long").alias("conversion_id"),
            F.col("event.attribution").cast("int").alias("attribution"),
            F.col("event.click").cast("int").alias("click"),
            F.col("event.click_pos").cast("int").alias("click_pos"),
            F.col("event.click_nb").cast("int").alias("click_nb"),
            F.col("event.cost").cast("double").alias("cost"),
            F.col("event.cpo").cast("double").alias("cpo"),
            F.col("event.time_since_last_click").cast("long").alias("time_since_last_click"),
            F.col("event.cat1").cast("long").alias("cat1"),
            F.col("event.cat2").cast("long").alias("cat2"),
            F.col("event.cat3").cast("long").alias("cat3"),
            F.col("event.cat4").cast("long").alias("cat4"),
            F.col("event.cat5").cast("long").alias("cat5"),
            F.col("event.cat6").cast("long").alias("cat6"),
            F.col("event.cat7").cast("long").alias("cat7"),
            F.col("event.cat8").cast("long").alias("cat8"),
            F.col("event.cat9").cast("long").alias("cat9"),
            F.col("event.producer_ingest_ts").cast("timestamp").alias("producer_ingest_ts"),
            F.col("bronze_ingest_ts").cast("timestamp").alias("bronze_ingest_ts"),
            F.current_timestamp().alias("silver_ingest_ts"),
            F.col("topic").alias("kafka_topic"),
            F.col("kafka_partition").cast("long").alias("kafka_partition"),
            F.col("kafka_offset").cast("long").alias("kafka_offset"),
            F.col("payload_hash").alias("payload_hash"),
        )
    )


def _record_metric_safely(
    spark,
    table: str,
    batch_id: int,
    row_count: int,
    started_at: datetime,
    finished_at: datetime,
    status: str = "success",
    detail: str | None = None,
) -> None:
    try:
        record_streaming_metric(
            spark,
            table,
            query_name="bronze_to_silver",
            batch_id=batch_id,
            row_count=row_count,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            detail=detail,
        )
    except Exception as exc:
        print(f"failed to record bronze_to_silver streaming metric: {exc}", flush=True)


def main() -> None:
    cfg = load_config()
    spark = get_spark("criteo-bronze-to-silver", cfg)

    bronze_stream = (
        spark.readStream.schema(bronze_raw_schema)
        .format("parquet")
        .option("maxFilesPerTrigger", 20)
        .load(cfg.bronze_path)
    )

    silver_stream = project_silver(bronze_stream)

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        started = datetime.now(timezone.utc)
        prepared = batch_df.dropDuplicates(["event_id"]).select(*SILVER_COLUMNS).persist()
        row_count = prepared.count()
        if row_count == 0:
            prepared.unpersist()
            _record_metric_safely(
                spark,
                cfg.ops_streaming_metrics_table,
                batch_id=batch_id,
                row_count=0,
                started_at=started,
                finished_at=datetime.now(timezone.utc),
            )
            return

        try:
            prepared.createOrReplaceTempView("silver_microbatch_updates")
            update_assignments = ", ".join(f"{column} = source.{column}" for column in SILVER_COLUMNS)
            insert_columns = ", ".join(SILVER_COLUMNS)
            insert_values = ", ".join(f"source.{column}" for column in SILVER_COLUMNS)
            spark.sql(
                f"""
                MERGE INTO {cfg.silver_events_table} AS target
                USING silver_microbatch_updates AS source
                ON target.event_id = source.event_id
                WHEN MATCHED THEN UPDATE SET {update_assignments}
                WHEN NOT MATCHED THEN INSERT ({insert_columns})
                VALUES ({insert_values})
                """
            )
            _record_metric_safely(
                spark,
                cfg.ops_streaming_metrics_table,
                batch_id=batch_id,
                row_count=row_count,
                started_at=started,
                finished_at=datetime.now(timezone.utc),
            )
        except Exception as exc:
            _record_metric_safely(
                spark,
                cfg.ops_streaming_metrics_table,
                batch_id=batch_id,
                row_count=row_count,
                started_at=started,
                finished_at=datetime.now(timezone.utc),
                status="failed",
                detail=str(exc)[:1000],
            )
            raise
        finally:
            prepared.unpersist()

    (
        silver_stream.writeStream.queryName("bronze_to_silver")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{cfg.checkpoint_path}/bronze_to_silver_v3")
        .trigger(processingTime=cfg.silver_trigger)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
