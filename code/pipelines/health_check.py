from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from code.common.config import load_config
from code.common.spark import get_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Run operational health SQL checks and store results.")
    parser.add_argument("--query-dir", default="/opt/project/code/health-queries")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark("criteo-health-check", cfg)
    replacements = {
        "catalog": cfg.catalog,
        "silver_db": cfg.silver_db,
        "gold_db": cfg.gold_db,
        "ops_db": cfg.ops_db,
    }

    query_dir = Path(args.query_dir)
    if not query_dir.exists():
        query_dir = Path("code/health-queries")
    for path in sorted(query_dir.glob("*.sql")):
        query = path.read_text(encoding="utf-8")
        for key, value in replacements.items():
            query = query.replace("${" + key + "}", value)
        result = (
            spark.sql(query)
            .withColumn("run_ts", F.current_timestamp())
            .withColumn("query_file", F.lit(path.name))
        )
        result.writeTo(cfg.ops_health_results_table).append()


if __name__ == "__main__":
    main()
