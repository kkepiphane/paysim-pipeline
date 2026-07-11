"""Tests unitaires des règles de transformation Silver.

On teste la LOGIQUE (flag d'incohérence, type de compte) sur un petit
DataFrame en mémoire, sans dépendre du gros fichier source.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_account_type_derivation(spark):
    """Un nom commençant par M est marchand, sinon client."""
    df = spark.createDataFrame(
        [("C123",), ("M456",), ("C789",)], ["name_orig"]
    ).withColumn(
        "orig_account_type",
        F.when(F.col("name_orig").startswith("M"), "MERCHANT")
         .otherwise("CUSTOMER"),
    )
    result = {r["name_orig"]: r["orig_account_type"] for r in df.collect()}
    assert result["C123"] == "CUSTOMER"
    assert result["M456"] == "MERCHANT"


def test_balance_inconsistency_flag(spark):
    """Le flag vaut 1 quand old - amount != new pour un CASH_OUT."""
    rows = [
        # (type, old, amount, new, attendu)
        ("CASH_OUT", 1000.0, 200.0, 800.0, 0),   # cohérent
        ("CASH_OUT", 1000.0, 200.0, 900.0, 1),   # incohérent
        ("CASH_IN", 1000.0, 200.0, 500.0, 0),    # type non concerné
    ]
    df = spark.createDataFrame(
        rows,
        ["transaction_type", "oldbalance_orig", "amount",
         "newbalance_orig", "expected"],
    )
    expected_new = F.col("oldbalance_orig") - F.col("amount")
    df = df.withColumn(
        "balance_inconsistent",
        (
            F.col("transaction_type").isin("CASH_OUT", "TRANSFER", "DEBIT", "PAYMENT")
            & (F.abs(F.col("newbalance_orig") - expected_new) > 0.01)
        ).cast("int"),
    )
    for r in df.collect():
        assert r["balance_inconsistent"] == r["expected"], r


def test_deduplication(spark):
    """Deux lignes identiques sur la clé métier sont réduites à une."""
    rows = [
        (1, "PAYMENT", 100.0, "C1", "M1"),
        (1, "PAYMENT", 100.0, "C1", "M1"),  # doublon
        (2, "PAYMENT", 100.0, "C1", "M1"),
    ]
    cols = ["step", "transaction_type", "amount", "name_orig", "name_dest"]
    df = spark.createDataFrame(rows, cols).dropDuplicates(cols)
    assert df.count() == 2
