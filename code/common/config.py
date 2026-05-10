from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG_PATHS = (
    Path("/opt/project/configs/pipeline.yaml"),
    Path("/opt/airflow/configs/pipeline.yaml"),
    Path("configs/pipeline.yaml"),
)


def _load_yaml(path: Path | None = None) -> dict[str, Any]:
    if path is not None:
        with path.open("r", encoding="utf-8") as fp:
            return yaml.safe_load(fp) or {}
    for candidate in DEFAULT_CONFIG_PATHS:
        if candidate.exists():
            with candidate.open("r", encoding="utf-8") as fp:
                return yaml.safe_load(fp) or {}
    return {}


def _env(name: str, default: Any) -> Any:
    return os.getenv(name, default)


@dataclass(frozen=True)
class LakehouseConfig:
    aws_profile: str
    aws_region: str
    s3_bucket: str
    catalog: str
    warehouse: str
    bronze_path: str
    checkpoint_path: str
    silver_db: str
    gold_db: str
    ops_db: str
    kafka_bootstrap_servers: str
    kafka_topic: str
    bronze_consumer_group: str
    silver_consumer_group: str
    bronze_trigger: str
    silver_trigger: str
    watermark: str
    max_offsets_per_trigger: int

    @property
    def silver_events_table(self) -> str:
        return f"{self.catalog}.{self.silver_db}.events"

    @property
    def gold_hourly_table(self) -> str:
        return f"{self.catalog}.{self.gold_db}.campaign_hourly_kpis"

    @property
    def gold_daily_table(self) -> str:
        return f"{self.catalog}.{self.gold_db}.campaign_daily_kpis"

    @property
    def ops_streaming_metrics_table(self) -> str:
        return f"{self.catalog}.{self.ops_db}.streaming_batch_metrics"

    @property
    def ops_health_results_table(self) -> str:
        return f"{self.catalog}.{self.ops_db}.health_check_results"


def load_config(path: Path | None = None) -> LakehouseConfig:
    raw = _load_yaml(path)
    aws = raw.get("aws", {})
    lakehouse = raw.get("lakehouse", {})
    kafka = raw.get("kafka", {})
    streaming = raw.get("streaming", {})

    bucket = _env("S3_BUCKET", aws.get("bucket", "metacode-iceberg-lakehouse"))
    warehouse = _env("S3_WAREHOUSE", lakehouse.get("warehouse", f"s3://{bucket}/warehouse"))
    checkpoint = _env("CHECKPOINT_PATH", lakehouse.get("checkpoint_path", f"s3a://{bucket}/checkpoints"))

    return LakehouseConfig(
        aws_profile=_env("AWS_PROFILE", aws.get("profile", "iceberg-lab")),
        aws_region=_env("AWS_REGION", aws.get("region", "ap-northeast-2")),
        s3_bucket=bucket,
        catalog=_env("ICEBERG_CATALOG", lakehouse.get("catalog", "lakehouse")),
        warehouse=warehouse,
        bronze_path=_env("BRONZE_PATH", lakehouse.get("bronze_path", f"s3a://{bucket}/bronze/criteo_attribution_events")),
        checkpoint_path=checkpoint,
        silver_db=_env("SILVER_DB", lakehouse.get("silver_db", "ad_attribution_silver")),
        gold_db=_env("GOLD_DB", lakehouse.get("gold_db", "ad_attribution_gold")),
        ops_db=_env("OPS_DB", lakehouse.get("ops_db", "ad_attribution_ops")),
        kafka_bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", kafka.get("bootstrap_servers", "kafka:9092")),
        kafka_topic=_env("KAFKA_TOPIC", kafka.get("topic", "criteo-attribution-events")),
        bronze_consumer_group=_env("BRONZE_CONSUMER_GROUP", kafka.get("bronze_consumer_group", "bronze-writer")),
        silver_consumer_group=_env("SILVER_CONSUMER_GROUP", kafka.get("silver_consumer_group", "silver-writer")),
        bronze_trigger=_env("BRONZE_TRIGGER", streaming.get("bronze_trigger", "30 seconds")),
        silver_trigger=_env("SILVER_TRIGGER", streaming.get("silver_trigger", "30 seconds")),
        watermark=_env("STREAMING_WATERMARK", streaming.get("watermark", "2 hours")),
        max_offsets_per_trigger=int(_env("MAX_OFFSETS_PER_TRIGGER", streaming.get("max_offsets_per_trigger", 50000))),
    )
