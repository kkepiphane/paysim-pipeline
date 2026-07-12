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

SOURCE_FILE = os.getenv("SOURCE_FILE", "PS_20174392719_1491204439457_log.csv")
SOURCE_PATH = RAW_DIR / SOURCE_FILE

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
