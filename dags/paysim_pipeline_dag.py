"""DAG Airflow — pipeline PaySim medallion.

Enchaînement : ingest_bronze → validate_bronze → build_silver
             → validate_silver (gate) → build_gold

Chaque tâche est idempotente (mode overwrite), donc un re-run complet
ne duplique aucune donnée.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.spark_session import get_spark
from src.bronze.ingest import ingest_bronze
from src.silver.transform import build_silver
from src.gold.model import build_gold
from src.quality.expectations import validate_bronze, validate_silver


def _task_ingest_bronze():
    spark = get_spark("bronze")
    try:
        ingest_bronze(spark)
    finally:
        spark.stop()


def _task_validate_bronze():
    spark = get_spark("validate-bronze")
    try:
        validate_bronze(spark)
    finally:
        spark.stop()


def _task_build_silver():
    spark = get_spark("silver")
    try:
        build_silver(spark)
    finally:
        spark.stop()


def _task_validate_silver():
    spark = get_spark("validate-silver")
    try:
        validate_silver(spark)
    finally:
        spark.stop()


def _task_build_gold():
    spark = get_spark("gold")
    try:
        build_gold(spark)
    finally:
        spark.stop()


default_args = {
    "owner": "epiphane",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="paysim_medallion_pipeline",
    description="Pipeline mobile money PaySim (Bronze/Silver/Gold)",
    schedule_interval=None,          # déclenchement manuel
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["data-engineering", "medallion", "paysim"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_bronze", python_callable=_task_ingest_bronze,
    )
    check_bronze = PythonOperator(
        task_id="validate_bronze", python_callable=_task_validate_bronze,
    )
    silver = PythonOperator(
        task_id="build_silver", python_callable=_task_build_silver,
    )
    check_silver = PythonOperator(
        task_id="validate_silver", python_callable=_task_validate_silver,
    )
    gold = PythonOperator(
        task_id="build_gold", python_callable=_task_build_gold,
    )

    ingest >> check_bronze >> silver >> check_silver >> gold
