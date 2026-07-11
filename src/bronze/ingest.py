"""Couche BRONZE — ingestion brute.

Principe : le Bronze est IMMUABLE. On ne corrige, ne filtre, ne nettoie
RIEN de métier. On se contente de :
  1. lire le CSV avec un schéma explicite,
  2. ajouter des métadonnées techniques (traçabilité),
  3. écrire en Parquet partitionné.

Pourquoi partitionner par `type` et pas par `step` ?
  - `type` a 5 valeurs (faible cardinalité) → 5 dossiers, gros fichiers.
  - `step` a ~743 valeurs → des centaines de dossiers = "small files
    problem" : Spark lit lentement une myriade de petits fichiers.
On partitionne sur ce qui est peu cardinal ET souvent filtré.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src import config


def ingest_bronze(spark: SparkSession) -> DataFrame:
    """Lit le CSV source et écrit la couche Bronze en Parquet."""
    df = (
        spark.read
        .option("header", True)
        .schema(config.RAW_SCHEMA)  # schéma explicite, pas d'inférence
        .csv(str(config.SOURCE_PATH))
    )

    # Métadonnées techniques : d'où et quand vient la donnée.
    df_bronze = (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.lit(config.SOURCE_FILE))
    )

    (
        df_bronze.write
        .mode("overwrite")           # re-run = remplacement (idempotent)
        .partitionBy("type")
        .parquet(str(config.BRONZE_DIR))
    )

    return df_bronze


if __name__ == "__main__":
    from src.spark_session import get_spark

    spark = get_spark("bronze-ingest")
    result = ingest_bronze(spark)
    print(f"Bronze écrit : {result.count():,} lignes → {config.BRONZE_DIR}")
    spark.stop()
