"""Feature engineering pour le scoring de fraude.

On se restreint à config.FRAUD_ELIGIBLE_TYPES (TRANSFER, CASH_OUT) : ce sont
les deux seuls types où isFraud vaut 1 dans PaySim (vérifié sur le dataset
complet). Entraîner sur CASH_IN/PAYMENT/DEBIT n'apporterait aucun signal et
biaiserait les métriques par un déséquilibre de classe encore plus extrême.

error_balance_orig / error_balance_dest sont des features dérivées classiques
sur ce dataset : elles quantifient l'écart de solde de façon continue (signée),
en complément du flag booléen balance_inconsistent déjà présent en Silver.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config


def build_features(spark: SparkSession) -> DataFrame:
    """Lit le Silver et produit la table de features pour le scoring de fraude."""
    silver = spark.read.parquet(str(config.SILVER_DIR))

    features = (
        silver
        .filter(F.col("transaction_type").isin(config.FRAUD_ELIGIBLE_TYPES))
        # Défensif : le schéma source autorise le null sur les soldes, même si
        # PaySim n'en produit pas en pratique — un NaN casserait StandardScaler.
        .na.fill(0.0, subset=[
            "amount", "oldbalance_orig", "newbalance_orig",
            "oldbalance_dest", "newbalance_dest",
        ])
        .withColumn("hour_of_day", F.col("step") % config.STEPS_PER_DAY)
        .withColumn(
            "is_transfer",
            (F.col("transaction_type") == "TRANSFER").cast("int"),
        )
        .withColumn(
            "error_balance_orig",
            F.col("oldbalance_orig") - F.col("amount") - F.col("newbalance_orig"),
        )
        .withColumn(
            "error_balance_dest",
            F.col("oldbalance_dest") + F.col("amount") - F.col("newbalance_dest"),
        )
        .select(
            "step", "name_orig", "name_dest",
            "amount", "hour_of_day", "is_transfer",
            "oldbalance_orig", "newbalance_orig",
            "oldbalance_dest", "newbalance_dest",
            "error_balance_orig", "error_balance_dest",
            "balance_inconsistent",
            F.col("is_fraud").alias("label"),
        )
    )
    features.write.mode("overwrite").parquet(str(config.ML_FEATURES_DIR))
    return features


if __name__ == "__main__":
    from src.spark_session import get_spark

    spark = get_spark("ml-features")
    result = build_features(spark)
    total = result.count()
    frauds = result.filter(F.col("label") == 1).count()
    print(f"Features écrites : {total:,} lignes ({frauds:,} fraudes, {frauds/total:.3%})")
    spark.stop()
