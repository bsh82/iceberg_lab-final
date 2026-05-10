from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from code.common.criteo_columns import CRITEO_SOURCE_COLUMNS


producer_event_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("source_timestamp", LongType(), False),
        StructField("uid", LongType(), True),
        StructField("campaign", LongType(), True),
        StructField("conversion", LongType(), True),
        StructField("conversion_timestamp", LongType(), True),
        StructField("conversion_id", LongType(), True),
        StructField("attribution", LongType(), True),
        StructField("click", LongType(), True),
        StructField("click_pos", LongType(), True),
        StructField("click_nb", LongType(), True),
        StructField("cost", DoubleType(), True),
        StructField("cpo", DoubleType(), True),
        StructField("time_since_last_click", LongType(), True),
        StructField("cat1", LongType(), True),
        StructField("cat2", LongType(), True),
        StructField("cat3", LongType(), True),
        StructField("cat4", LongType(), True),
        StructField("cat5", LongType(), True),
        StructField("cat6", LongType(), True),
        StructField("cat7", LongType(), True),
        StructField("cat8", LongType(), True),
        StructField("cat9", LongType(), True),
        StructField("producer_ingest_ts", TimestampType(), False),
    ]
)


bronze_raw_schema = StructType(
    [
        StructField("topic", StringType(), False),
        StructField("kafka_partition", LongType(), False),
        StructField("kafka_offset", LongType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("kafka_key", StringType(), True),
        StructField("payload", StringType(), False),
        StructField("payload_hash", StringType(), False),
        StructField("bronze_ingest_ts", TimestampType(), False),
        StructField("ingest_date", StringType(), False),
        StructField("ingest_hour", StringType(), False),
    ]
)
