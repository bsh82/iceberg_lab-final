from __future__ import annotations

import argparse
from pathlib import Path

from code.common.config import load_config
from code.common.spark import create_namespaces, get_spark


def execute_sql_file(path: Path, replacements: dict[str, str]) -> None:
    spark = get_spark(f"bootstrap-{path.stem}")
    sql_text = path.read_text(encoding="utf-8")
    for key, value in replacements.items():
        sql_text = sql_text.replace("${" + key + "}", value)
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    for stmt in statements:
        spark.sql(stmt)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create Glue namespaces and Iceberg tables.")
    parser.add_argument("--ddl-dir", default="/opt/project/code/ddl")
    args = parser.parse_args()

    cfg = load_config()
    spark = get_spark("bootstrap-catalog", cfg)
    create_namespaces(spark, cfg)

    replacements = {
        "catalog": cfg.catalog,
        "silver_db": cfg.silver_db,
        "gold_db": cfg.gold_db,
        "ops_db": cfg.ops_db,
        "bronze_path": cfg.bronze_path,
    }
    ddl_dir = Path(args.ddl_dir)
    if not ddl_dir.exists():
        ddl_dir = Path("code/ddl")
    for path in sorted(ddl_dir.glob("*.sql")):
        execute_sql_file(path, replacements)


if __name__ == "__main__":
    main()
