"""Couche GOLD — modélisation dimensionnelle (schéma en étoile).

On produit :
  Dimensions : dim_time, dim_transaction_type, dim_account
  Faits      : fact_transactions (avec clés étrangères)
  Agrégats   : agg_fraud_by_type_hour, agg_top_accounts,
               agg_fraud_by_amount_band

Les agrégats sont des tables "prêtes à consommer" pour le BI/dashboard,
calculées une fois au lieu de re-scanner les faits à chaque requête.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config


def _write(df: DataFrame, name: str) -> None:
    df.write.mode("overwrite").parquet(str(config.GOLD_DIR / name))


def build_gold(spark: SparkSession) -> dict[str, int]:
    """Construit toutes les tables Gold. Retourne un dict nom -> nb lignes."""
    silver = spark.read.parquet(str(config.SILVER_DIR))
    counts: dict[str, int] = {}

    # --- dim_time ----------------------------------------------------------
    dim_time = (
        silver.select("step").distinct()
        .withColumn("time_key", F.col("step"))
        .withColumn("day", (F.col("step") / config.STEPS_PER_DAY).cast("int") + 1)
        .withColumn("hour_of_day", F.col("step") % config.STEPS_PER_DAY)
    )
    _write(dim_time, "dim_time")
    counts["dim_time"] = dim_time.count()

    # --- dim_transaction_type ---------------------------------------------
    dim_type = (
        silver.select("transaction_type").distinct()
        .withColumn(
            "type_key",
            F.dense_rank().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.orderBy("transaction_type")
            ),
        )
    )
    _write(dim_type, "dim_transaction_type")
    counts["dim_transaction_type"] = dim_type.count()

    # --- dim_account (union émetteurs + destinataires) --------------------
    orig = silver.select(
        F.col("name_orig").alias("account_id"),
        F.col("orig_account_type").alias("account_type"),
    )
    dest = silver.select(
        F.col("name_dest").alias("account_id"),
        F.col("dest_account_type").alias("account_type"),
    )
    dim_account = (
        orig.union(dest).dropDuplicates(["account_id"])
        .withColumn("account_key", F.monotonically_increasing_id())
    )
    _write(dim_account, "dim_account")
    counts["dim_account"] = dim_account.count()

    # --- fact_transactions ------------------------------------------------
    fact = (
        silver
        .withColumn("transaction_key", F.monotonically_increasing_id())
        .withColumnRenamed("step", "time_key")
        .select(
            "transaction_key", "time_key", "transaction_type",
            "name_orig", "name_dest", "amount",
            "oldbalance_orig", "newbalance_orig",
            "oldbalance_dest", "newbalance_dest",
            "is_fraud", "is_flagged_fraud", "balance_inconsistent",
        )
    )
    _write(fact, "fact_transactions")
    counts["fact_transactions"] = fact.count()

    # --- agg_fraud_by_type_hour -------------------------------------------
    agg_fraud_type_hour = (
        silver
        .withColumn("hour_of_day", F.col("step") % config.STEPS_PER_DAY)
        .groupBy("transaction_type", "hour_of_day")
        .agg(
            F.count("*").alias("n_transactions"),
            F.sum("is_fraud").alias("n_fraud"),
            F.round(F.avg("is_fraud"), 4).alias("fraud_rate"),
        )
    )
    _write(agg_fraud_type_hour, "agg_fraud_by_type_hour")
    counts["agg_fraud_by_type_hour"] = agg_fraud_type_hour.count()

    # --- agg_top_accounts (volume sortant) --------------------------------
    agg_top = (
        silver.groupBy("name_orig")
        .agg(
            F.sum("amount").alias("total_out"),
            F.count("*").alias("n_transactions"),
            F.sum("is_fraud").alias("n_fraud"),
        )
        .orderBy(F.desc("total_out"))
        .limit(1000)
    )
    _write(agg_top, "agg_top_accounts")
    counts["agg_top_accounts"] = agg_top.count()

    # --- agg_fraud_by_amount_band -----------------------------------------
    band = (
        F.when(F.col("amount") < 1_000, "0-1K")
         .when(F.col("amount") < 10_000, "1K-10K")
         .when(F.col("amount") < 100_000, "10K-100K")
         .when(F.col("amount") < 1_000_000, "100K-1M")
         .otherwise("1M+")
    )
    agg_amount = (
        silver.withColumn("amount_band", band)
        .groupBy("amount_band")
        .agg(
            F.count("*").alias("n_transactions"),
            F.sum("is_fraud").alias("n_fraud"),
            F.round(F.avg("is_fraud"), 4).alias("fraud_rate"),
        )
    )
    _write(agg_amount, "agg_fraud_by_amount_band")
    counts["agg_fraud_by_amount_band"] = agg_amount.count()

    return counts


if __name__ == "__main__":
    from src.spark_session import get_spark

    spark = get_spark("gold-model")
    counts = build_gold(spark)
    for name, n in counts.items():
        print(f"  {name:<28} {n:>10,} lignes")
    spark.stop()
