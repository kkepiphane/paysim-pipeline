"""Consommateur Kafka — atterrit le flux en micro-batches Parquet.

Écrit dans config.STREAMING_BRONZE_DIR, séparé du Bronze batch : mode append
(chaque micro-batch est un nouvel arrivage), à l'opposé du mode overwrite
idempotent d'ingest_bronze qui rejoue tout le fichier source à chaque fois.
Le schéma de sortie (colonnes + partition par `type`) reste compatible avec
le Bronze batch, pour qu'un même Silver puisse en principe lire les deux.
"""
import argparse
import time
from datetime import datetime, timezone

import pandas as pd

from src import config


def flush_batch(rows: list[dict], source_topic: str) -> int:
    """Écrit un micro-batch en Parquet, partitionné par `type`. Retourne le nb de lignes."""
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    df["ingestion_timestamp"] = datetime.now(timezone.utc)
    df["source_file"] = f"kafka:{source_topic}"

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    for transaction_type, group in df.groupby("type"):
        out_dir = config.STREAMING_BRONZE_DIR / f"type={transaction_type}"
        out_dir.mkdir(parents=True, exist_ok=True)
        group.drop(columns=["type"]).to_parquet(out_dir / f"part-{ts}.parquet", index=False)

    return len(df)


def consume(
    bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
    topic: str = config.KAFKA_TOPIC,
    batch_size: int = 200,
    batch_timeout: float = 10.0,
    max_messages: int = 0,
) -> int:
    """Consomme le topic par micro-batches, jusqu'à `batch_timeout`s d'inactivité."""
    import json

    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": "paysim-bronze-streaming",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])
    config.STREAMING_BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    buffer: list[dict] = []
    n_total = 0
    last_message_at = time.monotonic()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                if batch_timeout and (time.monotonic() - last_message_at) >= batch_timeout:
                    break
                continue
            if msg.error():
                continue

            buffer.append(json.loads(msg.value().decode("utf-8")))
            last_message_at = time.monotonic()
            if len(buffer) >= batch_size:
                n_total += flush_batch(buffer, topic)
                buffer = []
            if max_messages and n_total + len(buffer) >= max_messages:
                break
    finally:
        n_total += flush_batch(buffer, topic)
        consumer.close()

    return n_total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument(
        "--batch-timeout", type=float, default=10.0,
        help="secondes sans message avant d'arrêter, 0 = illimité",
    )
    parser.add_argument("--max-messages", type=int, default=0, help="0 = illimité")
    args = parser.parse_args()

    n = consume(
        args.bootstrap_servers, args.topic,
        args.batch_size, args.batch_timeout, args.max_messages,
    )
    print(f"{n:,} messages atterris dans {config.STREAMING_BRONZE_DIR}")
