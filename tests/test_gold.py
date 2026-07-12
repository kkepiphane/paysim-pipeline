"""Tests unitaires de la couche Gold (schéma en étoile).

On construit un petit Silver synthétique en mémoire et on vérifie la
modélisation dimensionnelle : dérivation de dim_time, dédoublonnage de
dim_account, sélection de fact_transactions et logique des agrégats.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config
from src.gold.model import build_gold


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests-gold")
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


def _read_gold(spark, tmp_path, name):
    return spark.read.parquet(str(tmp_path / "gold" / name))


@pytest.fixture
def gold_paths(tmp_path, monkeypatch):
    """Redirige Silver/Gold vers un répertoire temporaire pour chaque test."""
    monkeypatch.setattr(config, "GOLD_DIR", tmp_path / "gold")
    return tmp_path


def test_build_gold_row_counts_match_actual_output(spark, gold_paths, monkeypatch):
    """Les comptes retournés par build_gold correspondent aux fichiers écrits."""
    rows = [
        (25, "PAYMENT", 100.0, "C1", "CUSTOMER", "M1", "MERCHANT",
         1000.0, 900.0, 0.0, 0.0, 0, 0, 0),
        (26, "CASH_OUT", 200.0, "C2", "CUSTOMER", "C3", "CUSTOMER",
         500.0, 300.0, 100.0, 300.0, 1, 0, 1),
    ]
    path = _write_silver(spark, gold_paths, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)

    counts = build_gold(spark)

    for name, expected in counts.items():
        actual = _read_gold(spark, gold_paths, name).count()
        assert actual == expected, f"{name}: attendu {expected}, écrit {actual}"


def test_dim_time_derives_day_and_hour(spark, gold_paths, monkeypatch):
    """step=25 → day=2 (24 steps/jour), hour_of_day=1 (25 % 24)."""
    rows = [
        (25, "PAYMENT", 100.0, "C1", "CUSTOMER", "M1", "MERCHANT",
         1000.0, 900.0, 0.0, 0.0, 0, 0, 0),
    ]
    path = _write_silver(spark, gold_paths, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    build_gold(spark)

    row = _read_gold(spark, gold_paths, "dim_time").collect()[0]
    assert row["time_key"] == 25
    assert row["day"] == 2
    assert row["hour_of_day"] == 1


def test_dim_account_deduplicates_across_orig_and_dest(spark, gold_paths, monkeypatch):
    """Un compte apparaissant à la fois en émetteur et destinataire n'est compté qu'une fois."""
    rows = [
        (1, "TRANSFER", 50.0, "C1", "CUSTOMER", "C2", "CUSTOMER",
         100.0, 50.0, 0.0, 50.0, 0, 0, 0),
        (2, "TRANSFER", 10.0, "C2", "CUSTOMER", "C1", "CUSTOMER",
         50.0, 40.0, 100.0, 110.0, 0, 0, 0),
    ]
    path = _write_silver(spark, gold_paths, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    build_gold(spark)

    dim_account = _read_gold(spark, gold_paths, "dim_account")
    assert dim_account.count() == 2
    account_ids = {r["account_id"] for r in dim_account.collect()}
    assert account_ids == {"C1", "C2"}


def test_fact_transactions_keeps_business_columns(spark, gold_paths, monkeypatch):
    """fact_transactions renomme step -> time_key et garde les colonnes métier attendues."""
    rows = [
        (25, "PAYMENT", 100.0, "C1", "CUSTOMER", "M1", "MERCHANT",
         1000.0, 900.0, 0.0, 0.0, 0, 0, 0),
    ]
    path = _write_silver(spark, gold_paths, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    build_gold(spark)

    fact = _read_gold(spark, gold_paths, "fact_transactions")
    row = fact.collect()[0]
    assert row["time_key"] == 25
    assert row["amount"] == 100.0
    assert "step" not in fact.columns


def test_agg_fraud_by_amount_band_buckets_correctly(spark, gold_paths, monkeypatch):
    """Un montant de 500 tombe dans la tranche 0-1K, un de 50 000 dans 10K-100K."""
    rows = [
        (1, "PAYMENT", 500.0, "C1", "CUSTOMER", "M1", "MERCHANT",
         1000.0, 500.0, 0.0, 0.0, 0, 0, 0),
        (2, "PAYMENT", 50_000.0, "C2", "CUSTOMER", "M2", "MERCHANT",
         100_000.0, 50_000.0, 0.0, 0.0, 1, 0, 0),
    ]
    path = _write_silver(spark, gold_paths, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    build_gold(spark)

    bands = {
        r["amount_band"]: r["n_fraud"]
        for r in _read_gold(spark, gold_paths, "agg_fraud_by_amount_band").collect()
    }
    assert bands["0-1K"] == 0
    assert bands["10K-100K"] == 1


def test_agg_top_accounts_ordered_by_total_out_desc(spark, gold_paths, monkeypatch):
    """Le compte avec le plus gros volume sortant apparaît en premier."""
    rows = [
        (1, "TRANSFER", 100.0, "C1", "CUSTOMER", "C9", "CUSTOMER",
         1000.0, 900.0, 0.0, 100.0, 0, 0, 0),
        (2, "TRANSFER", 900.0, "C2", "CUSTOMER", "C9", "CUSTOMER",
         1000.0, 100.0, 0.0, 900.0, 0, 0, 0),
    ]
    path = _write_silver(spark, gold_paths, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)
    build_gold(spark)

    top = _read_gold(spark, gold_paths, "agg_top_accounts").orderBy(
        F.desc("total_out")
    ).collect()
    assert top[0]["name_orig"] == "C2"
    assert top[0]["total_out"] == 900.0
