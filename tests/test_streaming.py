"""Tests unitaires de la logique streaming (sans broker Kafka réel).

row_to_message et flush_batch sont des fonctions pures, indépendantes du
client Kafka — testables directement, comme le reste du projet évite de
mocker ce qui peut être testé sur de vraies données en mémoire.
"""
import pandas as pd
import pytest

from src import config
from src.streaming.consumer import flush_batch
from src.streaming.producer import row_to_message


@pytest.fixture
def streaming_dir(tmp_path, monkeypatch):
    path = tmp_path / "bronze_streaming" / "transactions"
    monkeypatch.setattr(config, "STREAMING_BRONZE_DIR", path)
    return path


def test_row_to_message_matches_raw_schema_types():
    row = pd.Series({
        "step": 1, "type": "TRANSFER", "amount": 100.5,
        "nameOrig": "C1", "oldbalanceOrg": 200.0, "newbalanceOrig": 99.5,
        "nameDest": "C2", "oldbalanceDest": 0.0, "newbalanceDest": 100.5,
        "isFraud": 1, "isFlaggedFraud": 0,
    })
    msg = row_to_message(row)

    assert msg["step"] == 1 and isinstance(msg["step"], int)
    assert msg["amount"] == 100.5 and isinstance(msg["amount"], float)
    assert msg["type"] == "TRANSFER"
    assert msg["isFraud"] == 1 and isinstance(msg["isFraud"], int)


def test_flush_batch_partitions_by_type(streaming_dir):
    rows = [
        {"step": 1, "type": "CASH_OUT", "amount": 10.0, "nameOrig": "C1",
         "oldbalanceOrg": 20.0, "newbalanceOrig": 10.0, "nameDest": "C2",
         "oldbalanceDest": 0.0, "newbalanceDest": 10.0, "isFraud": 0, "isFlaggedFraud": 0},
        {"step": 1, "type": "TRANSFER", "amount": 30.0, "nameOrig": "C3",
         "oldbalanceOrg": 30.0, "newbalanceOrig": 0.0, "nameDest": "C4",
         "oldbalanceDest": 0.0, "newbalanceDest": 30.0, "isFraud": 1, "isFlaggedFraud": 0},
    ]

    n = flush_batch(rows, source_topic="paysim-transactions")

    assert n == 2
    assert (streaming_dir / "type=CASH_OUT").is_dir()
    assert (streaming_dir / "type=TRANSFER").is_dir()
    written = list((streaming_dir / "type=TRANSFER").glob("*.parquet"))
    assert len(written) == 1
    df = pd.read_parquet(written[0])
    assert df.iloc[0]["source_file"] == "kafka:paysim-transactions"
    assert "type" not in df.columns  # colonne de partition, pas dupliquée dans le fichier
    assert "ingestion_timestamp" in df.columns


def test_flush_batch_empty_returns_zero_and_writes_nothing(streaming_dir):
    n = flush_batch([], source_topic="paysim-transactions")
    assert n == 0
    assert not streaming_dir.exists()


def test_flush_batch_is_additive_across_calls(streaming_dir):
    """Contrairement au Bronze batch (overwrite), deux micro-batches successifs s'accumulent."""
    row = {"step": 1, "type": "DEBIT", "amount": 5.0, "nameOrig": "C1",
           "oldbalanceOrg": 10.0, "newbalanceOrig": 5.0, "nameDest": "C2",
           "oldbalanceDest": 0.0, "newbalanceDest": 5.0, "isFraud": 0, "isFlaggedFraud": 0}

    flush_batch([row], source_topic="paysim-transactions")
    flush_batch([row], source_topic="paysim-transactions")

    written = list((streaming_dir / "type=DEBIT").glob("*.parquet"))
    assert len(written) == 2
