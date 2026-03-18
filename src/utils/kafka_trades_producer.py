"""
Kafka Producer — send stock trade data to Kafka topic "trades".

Run AFTER starting Kafka:
    docker-compose up -d
    python src/utils/kafka_trades_producer.py

Input: data/input/data Lab 10.1.txt
       Each line is a JSON object: {"CreatedTime": "...", "Type": "BUY/SELL", "Amount": 500, "BrokerCode": "..."}
"""

from __future__ import annotations

import json
import time
from typing import Any, cast

from kafka import KafkaProducer  # type: ignore[import-untyped]

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC             = "trades"
INPUT_PATH        = "data/input/data Lab 10.1.txt"


def load_trades(path: str) -> list[dict[str, Any]]:
    """Load trades from text file — one JSON object per line."""
    trades: list[dict[str, Any]] = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                trades.append(cast(dict[str, Any], json.loads(line)))
    return trades


def run() -> None:
    producer: Any = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # type: ignore[reportUnknownLambdaType]
    )

    trades = load_trades(INPUT_PATH)
    print(f"Sending {len(trades)} trades to topic '{TOPIC}'...")

    for trade in trades:
        producer.send(TOPIC, value=trade)
        print(f"  Sent: {trade.get('Type')} {trade.get('Amount')} @ {trade.get('CreatedTime')}")
        time.sleep(0.5)

    producer.flush()
    print("Done!")


if __name__ == "__main__":
    run()