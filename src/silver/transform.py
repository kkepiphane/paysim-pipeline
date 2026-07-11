"""Couche SILVER — nettoyage et conformité.

Ici on applique les règles métier de qualité :
  - renommage en snake_case,
  - dédoublonnage,
  - dérivation du type de compte (C = client, M = marchand),
  - flag d'incohérence de solde (sans SUPPRIMER l'information).

Réflexion clé sur les soldes marchands :
  Les comptes destinataires `M` (marchands) ont oldbalanceDest et
  newbalanceDest à 0 dans PaySim — ce n'est PAS une erreur, c'est une
  caractéristique du jeu (les soldes marchands ne sont pas suivis).
  On ne flague donc l'incohérence que sur l'émetteur, et seulement
  pour les types où le solde émetteur doit mécaniquement bouger.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config


def build_silver(spark: SparkSession) -> DataFrame:
    """Lit le Bronze et produit la couche Silver nettoyée."""
    df = spark.read.parquet(str(config.BRONZE_DIR))

    # 1. Renommage snake_case + typage cohérent.
    df = (
        df
        .withColumnRenamed("nameOrig", "name_orig")
        .withColumnRenamed("oldbalanceOrg", "oldbalance_orig")
        .withColumnRenamed("newbalanceOrig", "newbalance_orig")
        .withColumnRenamed("nameDest", "name_dest")
        .withColumnRenamed("oldbalanceDest", "oldbalance_dest")
        .withColumnRenamed("newbalanceDest", "newbalance_dest")
        .withColumnRenamed("isFraud", "is_fraud")
        .withColumnRenamed("isFlaggedFraud", "is_flagged_fraud")
        .withColumnRenamed("type", "transaction_type")
    )

    # 2. Dédoublonnage (une transaction = combinaison unique de ces clés).
    dedup_keys = [
        "step", "transaction_type", "amount",
        "name_orig", "name_dest",
    ]
    df = df.dropDuplicates(dedup_keys)

    # 3. Type de compte à partir du préfixe du nom.
    df = (
        df
        .withColumn(
            "orig_account_type",
            F.when(F.col("name_orig").startswith("M"), "MERCHANT")
             .otherwise("CUSTOMER"),
        )
        .withColumn(
            "dest_account_type",
            F.when(F.col("name_dest").startswith("M"), "MERCHANT")
             .otherwise("CUSTOMER"),
        )
    )

    # 4. Flag d'incohérence de solde émetteur.
    #    Pour un débit du compte émetteur : oldbalance - amount == newbalance.
    #    Tolérance de 0.01 pour les erreurs d'arrondi flottant.
    expected_new_orig = F.col("oldbalance_orig") - F.col("amount")
    df = df.withColumn(
        "balance_inconsistent",
        (
            F.col("transaction_type").isin("CASH_OUT", "TRANSFER", "DEBIT", "PAYMENT")
            & (F.abs(F.col("newbalance_orig") - expected_new_orig) > 0.01)
        ).cast("int"),
    )

    # 5. Écriture Silver (non partitionnée : requêtes analytiques variées).
    df.write.mode("overwrite").parquet(str(config.SILVER_DIR))

    return df


if __name__ == "__main__":
    from src.spark_session import get_spark

    spark = get_spark("silver-transform")
    result = build_silver(spark)
    total = result.count()
    inconsistent = result.filter(F.col("balance_inconsistent") == 1).count()
    print(f"Silver écrit : {total:,} lignes")
    print(f"Incohérences de solde : {inconsistent:,} ({inconsistent/total:.1%})")
    spark.stop()
