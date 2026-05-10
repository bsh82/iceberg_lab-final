from __future__ import annotations

import os

from pyspark.sql import SparkSession

from code.common.config import LakehouseConfig, load_config


def get_spark(app_name: str, config: LakehouseConfig | None = None) -> SparkSession:
    cfg = config or load_config()
    shuffle_partitions = os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")
    adaptive_enabled = os.getenv("SPARK_SQL_ADAPTIVE_ENABLED", "false")
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{cfg.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{cfg.catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{cfg.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{cfg.catalog}.warehouse", cfg.warehouse)
        .config(f"spark.sql.catalog.{cfg.catalog}.client.region", cfg.aws_region)
        .config(f"spark.sql.catalog.{cfg.catalog}.s3.endpoint", f"https://s3.{cfg.aws_region}.amazonaws.com")
        .config(f"spark.sql.catalog.{cfg.catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{cfg.catalog}.glue.skip-name-validation", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", adaptive_enabled)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint.region", cfg.aws_region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def create_namespaces(spark: SparkSession, cfg: LakehouseConfig) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalog}.{cfg.silver_db}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalog}.{cfg.gold_db}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {cfg.catalog}.{cfg.ops_db}")
