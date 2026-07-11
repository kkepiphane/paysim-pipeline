"""Factory de SparkSession.

Centraliser la création de la session évite d'éparpiller la config
Spark dans chaque module et facilite les tests (une seule source).
"""
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str = "paysim-pipeline") -> SparkSession:
    """Retourne une SparkSession configurée pour du batch local/conteneur."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        # 6 valeurs de partition suffisent en local : on réduit le shuffle
        # par défaut (200) qui produirait trop de petits fichiers.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Écrase proprement une seule partition lors d'un re-run (idempotence).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
