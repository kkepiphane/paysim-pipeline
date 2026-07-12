"""Charge les agrégats Gold dans le warehouse Postgres pour le dashboard.

Seuls les agrégats déjà réduits sont chargés (quelques centaines de lignes au
plus) — jamais fact_transactions ni dim_account en entier : le warehouse sert
de couche de service pour un dashboard à accès concurrent, pas de réplica du
Gold. DuckDB calcule le résumé KPI directement sur les fichiers Parquet sans
tout matérialiser en mémoire Python.

On écrit/lit via SQLAlchemy Core (pas `DataFrame.to_sql`/`read_sql_table`) :
pandas>=2.1 exige SQLAlchemy>=2.0 pour ces raccourcis, incompatible avec
apache-airflow 2.9.3 qui impose SQLAlchemy<2.0 dans ce même requirements.txt.
Rester en Core évite ce conflit de versions tout en gardant un contrôle
explicite du schéma, plutôt que de dépendre de l'inférence de pandas.
"""
import numpy as np
import pandas as pd
from sqlalchemy import (
    BigInteger, Column, Float, MetaData, String, Table, create_engine, text,
)

from src import config

AGGREGATE_TABLES = [
    "agg_fraud_by_type_hour",
    "agg_top_accounts",
    "agg_fraud_by_amount_band",
    "dim_transaction_type",
]


def _sa_type_for(dtype):
    if np.issubdtype(dtype, np.integer):
        return BigInteger()
    if np.issubdtype(dtype, np.floating):
        return Float()
    return String()


def write_dataframe(engine, name: str, df: pd.DataFrame) -> int:
    """Remplace la table `name` par le contenu de df (idempotent)."""
    metadata = MetaData()
    table = Table(
        name, metadata,
        *(Column(col, _sa_type_for(df[col].dtype)) for col in df.columns),
    )
    metadata.drop_all(engine, [table], checkfirst=True)
    metadata.create_all(engine, [table])
    if len(df):
        with engine.begin() as conn:
            conn.execute(table.insert(), df.to_dict(orient="records"))
    return len(df)


def read_dataframe(engine, name: str) -> pd.DataFrame:
    """Relit une table du warehouse en DataFrame."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM {name}"))
        return pd.DataFrame(result.fetchall(), columns=list(result.keys()))


def _kpi_summary() -> pd.DataFrame:
    import duckdb

    con = duckdb.connect()
    fact_path = config.GOLD_DIR / "fact_transactions" / "*.parquet"
    return con.execute(f"""
        SELECT
            COUNT(*)                                    AS n_transactions,
            SUM(is_fraud)                                AS n_fraud,
            ROUND(SUM(is_fraud) * 100.0 / COUNT(*), 4)   AS fraud_pct,
            SUM(balance_inconsistent)                    AS n_balance_inconsistent
        FROM read_parquet('{fact_path}')
    """).fetchdf()


def load_warehouse(engine=None) -> dict[str, int]:
    """Charge les agrégats Gold + un résumé KPI dans Postgres.

    Retourne un dict nom de table -> nombre de lignes chargées.
    """
    engine = engine if engine is not None else create_engine(config.warehouse_url())
    counts: dict[str, int] = {}

    for name in AGGREGATE_TABLES:
        df = pd.read_parquet(config.GOLD_DIR / name)
        counts[name] = write_dataframe(engine, name, df)

    kpi = _kpi_summary()
    counts["kpi_summary"] = write_dataframe(engine, "kpi_summary", kpi)

    return counts


if __name__ == "__main__":
    result = load_warehouse()
    for table_name, n in result.items():
        print(f"  {table_name:<28} {n:>10,} lignes")
