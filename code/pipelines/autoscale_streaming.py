from __future__ import annotations

import os
from dataclasses import dataclass

from kafka import KafkaConsumer, TopicPartition
from kubernetes import client, config

from code.common.config import load_config
from code.common.spark import get_spark


@dataclass
class Decision:
    lag: int
    batch_duration_ms: int
    target_max_executors: int
    reason: str


def kafka_lag(bootstrap_servers: str, topic: str, group_id: str) -> int:
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=group_id, enable_auto_commit=False)
    partitions = consumer.partitions_for_topic(topic) or set()
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    if not topic_partitions:
        consumer.close()
        return 0
    end_offsets = consumer.end_offsets(topic_partitions)
    total = 0
    for tp in topic_partitions:
        committed = consumer.committed(tp) or 0
        total += max(0, end_offsets.get(tp, 0) - committed)
    consumer.close()
    return total


def latest_batch_duration_ms() -> int:
    cfg = load_config()
    spark = get_spark("criteo-streaming-autoscaler", cfg)
    try:
        rows = spark.sql(
            f"""
            SELECT max(batch_duration_ms) AS duration_ms
            FROM {cfg.ops_streaming_metrics_table}
            WHERE metric_ts >= current_timestamp() - INTERVAL 15 MINUTES
              AND query_name IN ('kafka_to_bronze', 'bronze_to_silver')
            """
        ).collect()
        return int(rows[0]["duration_ms"] or 0)
    except Exception as exc:  # noqa: BLE001
        print(f"batch_duration_lookup_failed={exc}")
        return 0


def decide(lag: int, duration_ms: int, current_max: int) -> Decision:
    min_executors = int(os.getenv("STREAMING_MIN_EXECUTORS", "1"))
    hard_max = int(os.getenv("STREAMING_HARD_MAX_EXECUTORS", "12"))
    scale_up_lag = int(os.getenv("SCALE_UP_LAG_THRESHOLD", "200000"))
    scale_down_lag = int(os.getenv("SCALE_DOWN_LAG_THRESHOLD", "20000"))
    scale_up_duration = int(os.getenv("SCALE_UP_BATCH_DURATION_MS", "120000"))
    scale_down_duration = int(os.getenv("SCALE_DOWN_BATCH_DURATION_MS", "30000"))

    if lag >= scale_up_lag or duration_ms >= scale_up_duration:
        return Decision(lag, duration_ms, min(hard_max, max(current_max + 2, min_executors + 1)), "scale_up")
    if lag <= scale_down_lag and duration_ms <= scale_down_duration:
        return Decision(lag, duration_ms, max(min_executors, current_max - 1), "scale_down")
    return Decision(lag, duration_ms, current_max, "hold")


def patch_spark_application(name: str, namespace: str, target_max: int, reason: str) -> None:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

    api = client.CustomObjectsApi()
    patch = {
        "metadata": {
            "annotations": {
                "autoscaling.criteo-lakehouse/last-reason": reason,
                "autoscaling.criteo-lakehouse/target-max-executors": str(target_max),
            }
        },
        "spec": {
            "dynamicAllocation": {
                "enabled": True,
                "maxExecutors": target_max,
            }
        },
    }
    api.patch_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace=namespace,
        plural="sparkapplications",
        name=name,
        body=patch,
    )


def current_max_executors(name: str, namespace: str) -> int:
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()
    api = client.CustomObjectsApi()
    obj = api.get_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace=namespace,
        plural="sparkapplications",
        name=name,
    )
    return int(obj.get("spec", {}).get("dynamicAllocation", {}).get("maxExecutors", 4))


def main() -> None:
    cfg = load_config()
    namespace = os.getenv("K8S_NAMESPACE", "criteo-lakehouse")
    apps = ["criteo-kafka-to-bronze", "criteo-bronze-to-silver"]
    lag = kafka_lag(cfg.kafka_bootstrap_servers, cfg.kafka_topic, cfg.silver_consumer_group)
    duration_ms = latest_batch_duration_ms()

    for app in apps:
        current_max = current_max_executors(app, namespace)
        decision = decide(lag, duration_ms, current_max)
        if decision.target_max_executors != current_max:
            patch_spark_application(app, namespace, decision.target_max_executors, decision.reason)
        print(
            f"app={app} lag={decision.lag} batch_duration_ms={decision.batch_duration_ms} "
            f"current_max={current_max} target_max={decision.target_max_executors} reason={decision.reason}",
            flush=True,
        )


if __name__ == "__main__":
    main()
