"""
Kafka Producer — send invoice JSON data to Kafka topic "invoices".

Run AFTER starting Kafka:
    docker-compose up -d
    python src/utils/kafka_producer.py

Input: data/input/samples.json
       Supports both JSON array and JSON Lines (one object per line)
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

from kafka import KafkaProducer  # type: ignore[import-untyped]

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC             = "invoices"
INPUT_PATH        = "data/input/samples.json"


def load_invoices(path: str) -> list[dict[str, Any]]:
    """
    Load invoices from file — supports two formats:
    - JSON array:  [{ ... }, { ... }]
    - JSON Lines:  one JSON object per line (no wrapping array)
    """
    with open(path) as f:
        content = f.read().strip()

    # Try JSON array first
    try:
        raw: Any = json.loads(content)
        if isinstance(raw, list):
            return cast(list[dict[str, Any]], raw)
        return [cast(dict[str, Any], raw)]
    except json.JSONDecodeError:
        pass

    # Fall back to JSON Lines — parse line by line
    invoices: list[dict[str, Any]] = []
    for line in content.splitlines():
        line = line.strip()
        if line:
            invoices.append(cast(dict[str, Any], json.loads(line)))
    return invoices


def run() -> None:
    producer: Any = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # type: ignore[reportUnknownLambdaType]
    )

    invoices = load_invoices(INPUT_PATH)
    print(f"Sending {len(invoices)} invoices to topic '{TOPIC}'...")

    for invoice in invoices:
        producer.send(TOPIC, value=invoice)
        print(f"  Sent: {invoice.get('InvoiceNumber', '?')}")
        time.sleep(0.5)

    producer.flush()
    print("Done!")


if __name__ == "__main__":
    run()