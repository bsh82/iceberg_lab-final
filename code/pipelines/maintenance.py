from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from code.common.config import load_config
from code.common.spark import get_spark


def _timestamp_literal(days_ago: int) -> str:
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_ago)
    return cutoff.strftime("%Y-%m-%d %H:%M:%S")


def compact_table(spark, cfg, table: str, target_file_size_bytes: int) -> None:
    spark.sql(
        f"""
        CALL {cfg.catalog}.system.rewrite_data_files(
          table => '{table}',
          options => map(
            'target-file-size-bytes', '{target_file_size_bytes}',
            'partial-progress.enabled', 'true'
          )
        )
        """
    ).show(truncate=False)


def compact_position_delete_files(spark, cfg, table: str, target_file_size_bytes: int) -> None:
    spark.sql(
        f"""
        CALL {cfg.catalog}.system.rewrite_position_delete_files(
          table => '{table}',
          options => map(
            'target-file-size-bytes', '{target_file_size_bytes}',
            'rewrite-all', 'true'
          )
        )
        """
    ).show(truncate=False)


def expire_snapshots(spark, cfg, table: str, retention_days: int, retain_last: int) -> None:
    spark.sql(
        f"""
        CALL {cfg.catalog}.system.expire_snapshots(
          table => '{table}',
          older_than => TIMESTAMP '{_timestamp_literal(retention_days)}',
          retain_last => {retain_last},
          stream_results => true
        )
        """
    ).show(truncate=False)


def remove_orphans(spark, cfg, table: str, retention_days: int, dry_run: bool) -> None:
    spark.sql(
        f"""
        CALL {cfg.catalog}.system.remove_orphan_files(
          table => '{table}',
          older_than => TIMESTAMP '{_timestamp_literal(retention_days)}',
          dry_run => {str(dry_run).lower()}
        )
        """
    ).show(truncate=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Iceberg table maintenance.")
    parser.add_argument("--action", choices=["compact", "delete-files", "expire", "orphan", "all"], default="all")
    parser.add_argument("--tables", nargs="*", default=None)
    parser.add_argument("--target-file-size-bytes", type=int, default=134217728)
    parser.add_argument("--snapshot-retention-days", type=int, default=7)
    parser.add_argument("--snapshot-retain-last", type=int, default=24)
    parser.add_argument("--orphan-retention-days", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark("criteo-iceberg-maintenance", cfg)
    tables = args.tables or [cfg.silver_events_table, cfg.gold_hourly_table, cfg.gold_daily_table]

    for table in tables:
        if args.action in ("compact", "all"):
            compact_table(spark, cfg, table, args.target_file_size_bytes)
        if args.action in ("delete-files", "all"):
            compact_position_delete_files(spark, cfg, table, args.target_file_size_bytes)
        if args.action in ("expire", "all"):
            expire_snapshots(spark, cfg, table, args.snapshot_retention_days, args.snapshot_retain_last)
        if args.action in ("orphan", "all"):
            remove_orphans(spark, cfg, table, args.orphan_retention_days, args.dry_run)


if __name__ == "__main__":
    main()
