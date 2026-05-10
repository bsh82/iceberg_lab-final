from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession


def record_streaming_metric(
    spark: SparkSession,
    table: str,
    query_name: str,
    batch_id: int,
    row_count: int,
    started_at: datetime,
    finished_at: datetime,
    status: str = "success",
    detail: str | None = None,
) -> None:
    duration_ms = int((finished_at - started_at).total_seconds() * 1000)
    payload = [
        {
            "metric_ts": datetime.now(timezone.utc),
            "query_name": query_name,
            "batch_id": int(batch_id),
            "row_count": int(row_count),
            "batch_duration_ms": int(duration_ms),
            "status": status,
            "detail": detail or "",
        }
    ]
    spark.createDataFrame(payload).writeTo(table).append()
