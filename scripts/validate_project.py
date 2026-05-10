from __future__ import annotations

import ast
from pathlib import Path

import yaml


REQUIRED_PATHS = [
    "README.md",
    "configs/pipeline.yaml",
    "infra/k8s/kustomization.yaml",
    "orchestration/dags/streaming_jobs_dag.py",
    "code/pipelines/kafka_to_bronze.py",
    "code/pipelines/bronze_to_silver.py",
    "code/pipelines/silver_to_gold.py",
    "code/pipelines/maintenance.py",
    "code/pipelines/health_check.py",
]


def main() -> None:
    missing = [path for path in REQUIRED_PATHS if not Path(path).exists()]
    if missing:
        raise SystemExit(f"Missing required files: {missing}")

    for path in Path("code").rglob("*.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for path in Path("orchestration/dags").rglob("*.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for path in list(Path(".").rglob("*.yaml")) + list(Path(".").rglob("*.yml")):
        list(yaml.safe_load_all(path.read_text(encoding="utf-8")))

    health_queries = list(Path("code/health-queries").glob("*.sql"))
    if len(health_queries) < 5:
        raise SystemExit("At least five health queries are required.")

    ddl_files = list(Path("code/ddl").glob("*.sql"))
    if len(ddl_files) < 5:
        raise SystemExit("Expected Iceberg DDL files.")

    print("Project static validation passed.")


if __name__ == "__main__":
    main()
