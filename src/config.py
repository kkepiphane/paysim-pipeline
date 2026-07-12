"""Configuration centrale du pipeline PaySim.

Tout ce qui est chemin, schéma ou constante métier vit ici,
pour ne jamais dupliquer une valeur "en dur" dans le code.
"""
import os
from pathlib import Path

from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

# --- Chemins ---------------------------------------------------------------
DATA_ROOT = Path(os.getenv("DATA_ROOT", "./data"))

RAW_DIR = DATA_ROOT / "raw"
BRONZE_DIR = DATA_ROOT / "bronze" / "transactions"
SILVER_DIR = DATA_ROOT / "silver" / "transactions"
GOLD_DIR = DATA_ROOT / "gold"

ML_FEATURES_DIR = GOLD_DIR / "ml_features"
ML_MODELS_DIR = DATA_ROOT / "ml" / "models"
ML_METRICS_PATH = DATA_ROOT / "ml" / "metrics.json"

SOURCE_FILE = os.getenv("SOURCE_FILE", "PS_20174392719_1491204439457_log.csv")
SOURCE_PATH = RAW_DIR / SOURCE_FILE

# Bronze streaming : répertoire séparé du Bronze batch (bronze/transactions).
# Le consumer Kafka écrit en mode append (chaque micro-batch = un nouvel
# arrivage), incompatible avec le mode overwrite idempotent du Bronze batch —
# les deux origines ne doivent jamais partager un répertoire.
STREAMING_BRONZE_DIR = DATA_ROOT / "bronze_streaming" / "transactions"

# --- Kafka (simulation d'ingestion temps réel) ------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "paysim-transactions")

# --- Warehouse (Postgres de service pour le dashboard) ----------------------
# Instance dédiée, séparée de la metadata DB Airflow : on ne mélange pas la
# base opérationnelle d'Airflow (dag_run, task_instance, ...) avec des tables
# métier consultées par un dashboard BI — deux charges et deux cycles de vie
# différents.
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "warehouse")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "warehouse")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "warehouse")
WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5433")


def warehouse_url() -> str:
    return (
        f"postgresql+psycopg2://{WAREHOUSE_USER}:{WAREHOUSE_PASSWORD}"
        f"@{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"
    )

# --- Seuils métier ---------------------------------------------------------
# Au-delà de ce taux d'incohérence de solde, le DAG échoue (gate qualité).
#
# Calibrés par transaction_type plutôt qu'un seuil global : PaySim est un
# simulateur multi-agents connu pour ne pas toujours répercuter correctement
# oldbalance_orig/newbalance_orig sur CASH_OUT et TRANSFER (~90-95% de taux de
# base sur le dataset complet — pas une anomalie, une caractéristique du
# simulateur, et ce sont justement les deux seuls types où la fraude existe
# dans PaySim). Un seuil unique masquerait une dérive réelle sur un type à
# faible volume comme DEBIT, noyé dans la moyenne pondérée par CASH_OUT/TRANSFER.
BALANCE_INCONSISTENCY_THRESHOLDS = {
    "CASH_OUT": 0.93,
    "TRANSFER": 0.97,
    "PAYMENT": 0.62,
    "DEBIT": 0.40,
}

# 1 step PaySim = 1 heure de simulation.
STEPS_PER_DAY = 24

# Domaine attendu de la colonne `type`.
TRANSACTION_TYPES = ["CASH_IN", "CASH_OUT", "TRANSFER", "PAYMENT", "DEBIT"]

# Dans PaySim, isFraud ne vaut jamais 1 en dehors de ces deux types (vérifié sur
# le dataset complet : 0% de fraude sur CASH_IN/PAYMENT/DEBIT). Le scoring ML
# se restreint donc à cette population plutôt que d'entraîner sur des types où
# le signal est structurellement absent.
FRAUD_ELIGIBLE_TYPES = ["TRANSFER", "CASH_OUT"]

# --- Schéma source (explicite, JAMAIS d'inférence en production) -----------
RAW_SCHEMA = StructType([
    StructField("step", IntegerType(), nullable=False),
    StructField("type", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=False),
    StructField("nameOrig", StringType(), nullable=False),
    StructField("oldbalanceOrg", DoubleType(), nullable=True),
    StructField("newbalanceOrig", DoubleType(), nullable=True),
    StructField("nameDest", StringType(), nullable=False),
    StructField("oldbalanceDest", DoubleType(), nullable=True),
    StructField("newbalanceDest", DoubleType(), nullable=True),
    StructField("isFraud", IntegerType(), nullable=False),
    StructField("isFlaggedFraud", IntegerType(), nullable=False),
])
