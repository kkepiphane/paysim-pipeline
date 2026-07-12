"""Tests unitaires du feature engineering pour le scoring de fraude."""
import pytest
from pyspark.sql import SparkSession

from src import config
from src.ml.features import build_features


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests-ml-features")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


SILVER_COLUMNS = [
    "step", "transaction_type", "amount",
    "name_orig", "orig_account_type", "name_dest", "dest_account_type",
    "oldbalance_orig", "newbalance_orig", "oldbalance_dest", "newbalance_dest",
    "is_fraud", "is_flagged_fraud", "balance_inconsistent",
]


def _write_silver(spark, tmp_path, rows):
    df = spark.createDataFrame(rows, SILVER_COLUMNS)
    path = tmp_path / "silver"
    df.write.mode("overwrite").parquet(str(path))
    return path


def test_filters_to_fraud_eligible_types_only(spark, tmp_path, monkeypatch):
    """CASH_IN n'est jamais fraudeur dans PaySim : exclu de la table de features."""
    rows = [
        (1, "TRANSFER", 100.0, "C1", "CUSTOMER", "C2", "CUSTOMER",
         200.0, 100.0, 0.0, 100.0, 0, 0, 0),
        (2, "CASH_IN", 100.0, "C3", "CUSTOMER", "C4", "CUSTOMER",
         200.0, 300.0, 0.0, 0.0, 0, 0, 0),
    ]
    path = _write_silver(spark, tmp_path, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    monkeypatch.setattr(config, "ML_FEATURES_DIR", tmp_path / "ml_features")

    result = build_features(spark)
    types_seen = {r["name_orig"] for r in result.collect()}
    assert types_seen == {"C1"}


def test_error_balance_orig_captures_signed_discrepancy(spark, tmp_path, monkeypatch):
    """error_balance_orig = old - amount - new : positif si le solde a trop baissé."""
    rows = [
        # old=200, amount=100, new=100 -> cohérent, erreur = 0
        (1, "TRANSFER", 100.0, "C1", "CUSTOMER", "C2", "CUSTOMER",
         200.0, 100.0, 0.0, 100.0, 0, 0, 0),
        # old=200, amount=100, new=50 -> le solde a trop baissé, erreur = 50
        (2, "TRANSFER", 100.0, "C3", "CUSTOMER", "C4", "CUSTOMER",
         200.0, 50.0, 0.0, 100.0, 0, 0, 1),
    ]
    path = _write_silver(spark, tmp_path, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    monkeypatch.setattr(config, "ML_FEATURES_DIR", tmp_path / "ml_features")

    result = {r["name_orig"]: r["error_balance_orig"] for r in build_features(spark).collect()}
    assert result["C1"] == 0.0
    assert result["C3"] == 50.0


def test_is_transfer_flag_distinguishes_types(spark, tmp_path, monkeypatch):
    """is_transfer vaut 1 pour TRANSFER, 0 pour CASH_OUT."""
    rows = [
        (1, "TRANSFER", 100.0, "C1", "CUSTOMER", "C2", "CUSTOMER",
         200.0, 100.0, 0.0, 100.0, 0, 0, 0),
        (2, "CASH_OUT", 100.0, "C3", "CUSTOMER", "C4", "CUSTOMER",
         200.0, 100.0, 0.0, 0.0, 0, 0, 0),
    ]
    path = _write_silver(spark, tmp_path, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    monkeypatch.setattr(config, "ML_FEATURES_DIR", tmp_path / "ml_features")

    result = {r["name_orig"]: r["is_transfer"] for r in build_features(spark).collect()}
    assert result["C1"] == 1
    assert result["C3"] == 0


def test_label_preserves_is_fraud(spark, tmp_path, monkeypatch):
    """La colonne label reprend fidèlement is_fraud."""
    rows = [
        (1, "CASH_OUT", 100.0, "C1", "CUSTOMER", "C2", "CUSTOMER",
         200.0, 100.0, 0.0, 100.0, 1, 0, 0),
    ]
    path = _write_silver(spark, tmp_path, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    monkeypatch.setattr(config, "ML_FEATURES_DIR", tmp_path / "ml_features")

    row = build_features(spark).collect()[0]
    assert row["label"] == 1
