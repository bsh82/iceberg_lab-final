from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from code.common.criteo_columns import CRITEO_SOURCE_COLUMNS


INT_COLUMNS = {
    "timestamp",
    "uid",
    "campaign",
    "conversion",
    "conversion_timestamp",
    "conversion_id",
    "attribution",
    "click",
    "click_pos",
    "click_nb",
    "time_since_last_click",
    "cat1",
    "cat2",
    "cat3",
    "cat4",
    "cat5",
    "cat6",
    "cat7",
    "cat8",
    "cat9",
}
FLOAT_COLUMNS = {"cost", "cpo"}


def _coerce(row: dict[str, str], line_number: int, base_epoch: float) -> dict[str, object]:
    typed: dict[str, object] = {}
    for key, value in row.items():
        if key in INT_COLUMNS:
            typed[key] = int(value)
        elif key in FLOAT_COLUMNS:
            typed[key] = float(value)
        else:
            typed[key] = value

    source_ts = int(typed["timestamp"])
    event_time = datetime.fromtimestamp(base_epoch + source_ts, tz=timezone.utc)
    identity = "|".join(
        [
            str(line_number),
            str(typed.get("timestamp")),
            str(typed.get("uid")),
            str(typed.get("campaign")),
            str(typed.get("conversion_id")),
            str(typed.get("click_pos")),
        ]
    )

    typed["source_timestamp"] = source_ts
    typed["event_time"] = event_time.isoformat()
    typed["producer_ingest_ts"] = datetime.now(tz=timezone.utc).isoformat()
    typed["event_id"] = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return typed


def iter_rows(path: Path, base_epoch: float, start_line: int = 0) -> Iterable[dict[str, object]]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, mode="rt", encoding="utf-8", newline="") as fp:
        reader = csv.reader(fp, delimiter="\t")
        first = next(reader)
        has_header = first == CRITEO_SOURCE_COLUMNS
        if has_header:
            line_number = 0
        else:
            line_number = 1
            if line_number > start_line:
                yield _coerce(dict(zip(CRITEO_SOURCE_COLUMNS, first)), line_number, base_epoch)

        for values in reader:
            line_number += 1
            if line_number <= start_line:
                continue
            if len(values) != len(CRITEO_SOURCE_COLUMNS):
                continue
            yield _coerce(dict(zip(CRITEO_SOURCE_COLUMNS, values)), line_number, base_epoch)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay Criteo attribution rows into Kafka as real-time events.")
    parser.add_argument("--dataset-path", default=os.getenv("CRITEO_DATASET_PATH", "/data/criteo_attribution_dataset.tsv.gz"))
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "criteo-attribution-events"))
    parser.add_argument("--events-per-second", type=float, default=float(os.getenv("PRODUCER_EVENTS_PER_SECOND", "100")))
    parser.add_argument("--max-events", type=int, default=int(os.getenv("PRODUCER_MAX_EVENTS", "0")))
    parser.add_argument("--start-line", type=int, default=int(os.getenv("PRODUCER_START_LINE", "0")))
    parser.add_argument("--loop", action="store_true", default=os.getenv("PRODUCER_LOOP", "false").lower() == "true")
    return parser


def main() -> None:
    from kafka import KafkaProducer

    args = build_parser().parse_args()
    path = Path(args.dataset_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {path}. Run scripts/download_dataset.py first.")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda v: v.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        linger_ms=50,
        acks="all",
        retries=10,
        client_id="criteo-replay-producer",
    )

    interval = 1.0 / args.events_per_second if args.events_per_second > 0 else 0
    produced = 0
    base_epoch = time.time()

    while True:
        for event in iter_rows(path, base_epoch=base_epoch, start_line=args.start_line):
            producer.send(args.topic, key=str(event["event_id"]), value=event)
            produced += 1
            if produced % 1000 == 0:
                producer.flush()
                print(f"produced={produced}", flush=True)
            if args.max_events and produced >= args.max_events:
                producer.flush()
                return
            if interval:
                time.sleep(interval)
        if not args.loop:
            producer.flush()
            return
        args.start_line = 0


if __name__ == "__main__":
    main()
