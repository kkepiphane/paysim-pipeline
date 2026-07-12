"""Producteur Kafka — simule un flux de transactions en (quasi) temps réel.

Rejoue le CSV source ligne à ligne vers Kafka, pour démontrer le motif
d'ingestion streaming en complément du batch (src/bronze/ingest.py).
--rate contrôle le débit simulé ; --limit borne le nombre de messages
(0 = tout le fichier, à éviter en démo : 6,3M lignes).
"""
import argparse
import json
import time

import pandas as pd

from src import config

RAW_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud",
]


def row_to_message(row: pd.Series) -> dict:
    """Convertit une ligne du CSV source en message JSON-sérialisable."""
    return {
        "step": int(row["step"]),
        "type": row["type"],
        "amount": float(row["amount"]),
        "nameOrig": row["nameOrig"],
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
        "nameDest": row["nameDest"],
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
        "isFraud": int(row["isFraud"]),
        "isFlaggedFraud": int(row["isFlaggedFraud"]),
    }


def produce(
    bootstrap_servers: str = config.KAFKA_BOOTSTRAP_SERVERS,
    topic: str = config.KAFKA_TOPIC,
    limit: int = 1000,
    rate: float = 50.0,
) -> int:
    """Publie des transactions du CSV source vers Kafka. Retourne le nombre envoyé."""
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": bootstrap_servers})
    delay = 1.0 / rate if rate > 0 else 0.0
    n_sent = 0
    try:
        for chunk in pd.read_csv(config.SOURCE_PATH, usecols=RAW_COLUMNS, chunksize=1000):
            for _, row in chunk.iterrows():
                payload = json.dumps(row_to_message(row)).encode("utf-8")
                producer.produce(topic, value=payload)
                producer.poll(0)
                n_sent += 1
                if delay:
                    time.sleep(delay)
                if limit and n_sent >= limit:
                    return n_sent
    finally:
        producer.flush()
    return n_sent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=config.KAFKA_TOPIC)
    parser.add_argument("--limit", type=int, default=1000, help="0 = tout le fichier")
    parser.add_argument("--rate", type=float, default=50.0, help="messages/seconde, 0 = sans délai")
    args = parser.parse_args()

    sent = produce(args.bootstrap_servers, args.topic, args.limit, args.rate)
    print(f"{sent:,} messages envoyés sur le topic '{args.topic}'")
