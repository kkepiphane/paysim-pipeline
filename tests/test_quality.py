"""Tests unitaires de la gate qualité Silver (seuils par transaction_type).

Un seuil global masquerait une dérive sur un type à faible volume (cf.
décision d'architecture dans le README) : ces tests vérifient que la gate
juge chaque type indépendamment de son propre seuil.
"""
import pytest
from pyspark.sql import SparkSession

from src import config
from src.quality.expectations import validate_silver


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests-quality")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield s
    s.stop()


def _write_silver(spark, tmp_path, rows):
    cols = [
        "step", "transaction_type", "amount", "name_orig", "name_dest",
        "balance_inconsistent",
    ]
    df = spark.createDataFrame(rows, cols)
    path = tmp_path / "silver"
    df.write.mode("overwrite").parquet(str(path))
    return path


def test_validate_silver_passes_within_threshold(spark, tmp_path, monkeypatch):
    """CASH_OUT à 50% d'incohérence reste sous son seuil (0.93) : la gate passe."""
    rows = [
        (i, "CASH_OUT", 10.0, f"C{i}", "C2", i % 2)
        for i in range(10)
    ]
    path = _write_silver(spark, tmp_path, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)

    validate_silver(spark)  # ne doit pas lever


def test_validate_silver_fails_above_threshold(spark, tmp_path, monkeypatch):
    """CASH_OUT à 100% d'incohérence dépasse son seuil (0.93) : la gate échoue."""
    rows = [
        (i, "CASH_OUT", 10.0, f"C{i}", "C2", 1)
        for i in range(10)
    ]
    path = _write_silver(spark, tmp_path, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)

    with pytest.raises(AssertionError, match="CASH_OUT"):
        validate_silver(spark)


def test_validate_silver_low_volume_type_not_masked(spark, tmp_path, monkeypatch):
    """Une dérive sur DEBIT (faible volume) ne doit pas être noyée par CASH_OUT."""
    rows = (
        # CASH_OUT : 10% d'incohérence, bien sous son seuil (0.93).
        [(i, "CASH_OUT", 10.0, f"C{i}", "C2", 1 if i == 0 else 0) for i in range(100)]
        # DEBIT : 100% d'incohérence, très au-dessus de son seuil (0.40).
        + [(1000 + i, "DEBIT", 10.0, f"D{i}", "C2", 1) for i in range(5)]
    )
    path = _write_silver(spark, tmp_path, rows)
    monkeypatch.setattr(config, "SILVER_DIR", path)

    with pytest.raises(AssertionError, match="DEBIT"):
        validate_silver(spark)
