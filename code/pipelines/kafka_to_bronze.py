from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from code.common.config import load_config
from code.common.metrics import record_streaming_metric
from code.common.spark import get_spark


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
            query_name="kafka_to_bronze",
            batch_id=batch_id,
            row_count=row_count,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            detail=detail,
        )
    except Exception as exc:
        print(f"failed to record kafka_to_bronze streaming metric: {exc}", flush=True)


def main() -> None:
    cfg = load_config()
    spark = get_spark("criteo-kafka-to-bronze", cfg)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", cfg.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        .load()
    )

    bronze_df = (
        kafka_df.select(
            F.col("topic"),
            F.col("partition").cast("long").alias("kafka_partition"),
            F.col("offset").cast("long").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("payload"),
        )
        .withColumn("payload_hash", F.sha2(F.col("payload"), 256))
        .withColumn("bronze_ingest_ts", F.current_timestamp())
        .withColumn("ingest_date", F.date_format(F.col("bronze_ingest_ts"), "yyyy-MM-dd"))
        .withColumn("ingest_hour", F.date_format(F.col("bronze_ingest_ts"), "HH"))
    )

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        started = datetime.now(timezone.utc)
        row_count = batch_df.count()
        try:
            if row_count:
                (
                    batch_df.write.mode("append")
                    .format("parquet")
                    .partitionBy("ingest_date", "ingest_hour")
                    .save(cfg.bronze_path)
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

    (
        bronze_df.writeStream.queryName("kafka_to_bronze")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{cfg.checkpoint_path}/kafka_to_bronze_v2")
        .trigger(processingTime=cfg.bronze_trigger)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()
