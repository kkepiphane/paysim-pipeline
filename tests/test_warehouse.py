"""Tests unitaires du chargement du warehouse (agrégats Gold -> SQL).

On utilise un moteur SQLAlchemy SQLite en mémoire plutôt qu'un vrai Postgres :
load_warehouse() est agnostique du moteur, seule la logique de chargement est
sous test ici (pas la persistance Postgres elle-même).
"""
import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src import config
from src.warehouse.load import AGGREGATE_TABLES, load_warehouse, read_dataframe


@pytest.fixture
def gold_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "GOLD_DIR", tmp_path / "gold")
    return tmp_path / "gold"


def _write_table(path, name, df):
    out = path / name
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out / "part-0.parquet")


def _seed_gold(gold_paths):
    _write_table(gold_paths, "agg_fraud_by_type_hour", pd.DataFrame({
        "transaction_type": ["CASH_OUT", "TRANSFER"],
        "hour_of_day": [0, 0],
        "n_transactions": [100, 50],
        "n_fraud": [1, 2],
        "fraud_rate": [0.01, 0.04],
    }))
    _write_table(gold_paths, "agg_top_accounts", pd.DataFrame({
        "name_orig": ["C1", "C2"],
        "total_out": [1000.0, 500.0],
        "n_transactions": [5, 3],
        "n_fraud": [0, 1],
    }))
    _write_table(gold_paths, "agg_fraud_by_amount_band", pd.DataFrame({
        "amount_band": ["0-1K", "1K-10K"],
        "n_transactions": [10, 20],
        "n_fraud": [0, 1],
        "fraud_rate": [0.0, 0.05],
    }))
    _write_table(gold_paths, "dim_transaction_type", pd.DataFrame({
        "transaction_type": ["CASH_OUT", "TRANSFER"],
        "type_key": [1, 2],
    }))
    _write_table(gold_paths, "fact_transactions", pd.DataFrame({
        "is_fraud": [0, 0, 1, 0],
        "balance_inconsistent": [0, 1, 1, 0],
    }))


def test_load_warehouse_loads_all_aggregate_tables(gold_paths):
    _seed_gold(gold_paths)
    engine = create_engine("sqlite:///:memory:")

    counts = load_warehouse(engine=engine)

    for name in AGGREGATE_TABLES:
        assert counts[name] > 0
        with engine.connect() as conn:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {name}")).scalar()
        assert n == counts[name]


def test_load_warehouse_kpi_summary_matches_fact_transactions(gold_paths):
    _seed_gold(gold_paths)
    engine = create_engine("sqlite:///:memory:")

    load_warehouse(engine=engine)

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT n_transactions, n_fraud, n_balance_inconsistent FROM kpi_summary"
        )).fetchone()
    assert row.n_transactions == 4
    assert row.n_fraud == 1
    assert row.n_balance_inconsistent == 2


def test_load_warehouse_is_idempotent(gold_paths):
    """Rejouer le chargement ne duplique pas les lignes (drop + create à chaque run)."""
    _seed_gold(gold_paths)
    engine = create_engine("sqlite:///:memory:")

    load_warehouse(engine=engine)
    counts_second_run = load_warehouse(engine=engine)

    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM agg_top_accounts")).scalar()
    assert n == counts_second_run["agg_top_accounts"] == 2


def test_read_dataframe_roundtrips_written_table(gold_paths):
    """Ce que le dashboard lit correspond exactement à ce qui a été chargé."""
    _seed_gold(gold_paths)
    engine = create_engine("sqlite:///:memory:")
    load_warehouse(engine=engine)

    df = read_dataframe(engine, "agg_top_accounts")

    assert set(df["name_orig"]) == {"C1", "C2"}
    assert df.loc[df["name_orig"] == "C1", "total_out"].iloc[0] == 1000.0
