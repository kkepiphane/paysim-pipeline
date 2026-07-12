"""Contrôles qualité intégrés au pipeline.

Volontairement en PySpark natif (assertions explicites) plutôt qu'en
Great Expectations pur : c'est plus lisible pour un lecteur du dépôt et
ça montre qu'on maîtrise la LOGIQUE de qualité, pas juste un outil.
On peut brancher GE par-dessus en Phase 8.

Chaque fonction LÈVE une exception si un contrat est violé → dans
Airflow, la tâche échoue et stoppe le DAG (fail-fast).
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import config


def validate_bronze(spark: SparkSession) -> None:
    """Contrats sur la couche Bronze."""
    df = spark.read.parquet(str(config.BRONZE_DIR))

    # Colonnes obligatoires présentes.
    required = {"step", "amount", "nameOrig", "nameDest", "isFraud"}
    missing = required - set(df.columns)
    assert not missing, f"Colonnes manquantes en Bronze : {missing}"

    # Montants non négatifs.
    n_neg = df.filter(F.col("amount") < 0).count()
    assert n_neg == 0, f"{n_neg} montants négatifs détectés"

    # Types de transaction dans le domaine attendu.
    found = {r["type"] for r in df.select("type").distinct().collect()}
    unexpected = found - set(config.TRANSACTION_TYPES)
    assert not unexpected, f"Types inattendus : {unexpected}"

    print("[OK] Validation Bronze passée.")


def validate_silver(spark: SparkSession) -> None:
    """Contrats sur la couche Silver + gate d'incohérence de solde."""
    df = spark.read.parquet(str(config.SILVER_DIR))
    total = df.count()
    assert total > 0, "Silver vide"

    # Pas de doublons résiduels sur la clé métier.
    keys = ["step", "transaction_type", "amount", "name_orig", "name_dest"]
    n_dup = df.count() - df.dropDuplicates(keys).count()
    assert n_dup == 0, f"{n_dup} doublons résiduels en Silver"

    # Gate : taux d'incohérence sous le seuil, par transaction_type.
    # Un seuil global mélangerait des types au comportement structurellement
    # différent (cf. config.BALANCE_INCONSISTENCY_THRESHOLDS) et masquerait
    # une dérive réelle sur un type à faible volume.
    counts = (
        df.groupBy("transaction_type")
        .agg(
            F.count("*").alias("n"),
            F.sum("balance_inconsistent").alias("n_inconsistent"),
        )
        .collect()
    )
    violations = []
    rates = {}
    for row in counts:
        transaction_type = row["transaction_type"]
        threshold = config.BALANCE_INCONSISTENCY_THRESHOLDS.get(transaction_type)
        if threshold is None:
            continue
        rate = row["n_inconsistent"] / row["n"]
        rates[transaction_type] = rate
        if rate > threshold:
            violations.append(
                f"{transaction_type}: {rate:.1%} > seuil {threshold:.1%}"
            )

    assert not violations, (
        "Taux d'incohérence dépassé — pipeline arrêté : " + "; ".join(violations)
    )

    summary = ", ".join(f"{t}={r:.1%}" for t, r in sorted(rates.items()))
    print(f"[OK] Validation Silver passée ({summary}).")
