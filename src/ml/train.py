"""Entraînement et évaluation du scoring de fraude.

Deux modèles complémentaires sur les mêmes features :
  - Logistic Regression : baseline supervisée (class_weight="balanced" pour
    compenser le déséquilibre extrême de classe, ~0.3% de fraude même après
    restriction à TRANSFER/CASH_OUT).
  - Isolation Forest : détection d'anomalie non supervisée (ne voit jamais le
    label à l'entraînement) — pertinent si un futur mode de fraude diffère du
    seul motif vu dans ce dataset.

La table de features tient en mémoire pandas (quelques millions de lignes,
~10 colonnes) : Spark sert à l'ETL à l'échelle (src.ml.features écrit le
Parquet), scikit-learn au modèle final. On lit ce Parquet directement en
pandas via pyarrow plutôt que de rouvrir une SparkSession juste pour un
`toPandas()` — un aller-retour Spark inutile pour une table qui tient déjà
en mémoire.

PR-AUC (average precision) est la métrique de référence, pas l'accuracy :
avec ~0.3% de positifs, un modèle qui prédit toujours "non fraude" atteint
99.7% d'accuracy tout en étant inutile.
"""
import json

import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score, classification_report, confusion_matrix,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from src import config

FEATURE_COLUMNS = [
    "amount", "hour_of_day", "is_transfer",
    "oldbalance_orig", "newbalance_orig",
    "oldbalance_dest", "newbalance_dest",
    "error_balance_orig", "error_balance_dest",
    "balance_inconsistent",
]


def _load_training_frame() -> pd.DataFrame:
    return pd.read_parquet(config.ML_FEATURES_DIR, columns=[*FEATURE_COLUMNS, "label"])


def train_and_evaluate() -> dict:
    """Entraîne les deux modèles, les évalue, sauvegarde artefacts + métriques."""
    pdf = _load_training_frame()
    X = pdf[FEATURE_COLUMNS].to_numpy()
    y = pdf["label"].to_numpy()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y,
    )

    scaler = StandardScaler().fit(X_train)
    X_train_s, X_test_s = scaler.transform(X_train), scaler.transform(X_test)

    # --- Logistic Regression (baseline supervisée) --------------------------
    logreg = LogisticRegression(class_weight="balanced", max_iter=1000)
    logreg.fit(X_train_s, y_train)
    logreg_scores = logreg.predict_proba(X_test_s)[:, 1]
    logreg_preds = logreg.predict(X_test_s)

    # --- Isolation Forest (anomalie non supervisée) -------------------------
    fraud_rate = y_train.mean()
    iso = IsolationForest(
        n_estimators=100, contamination=max(fraud_rate, 1e-4), random_state=42,
    )
    iso.fit(X_train_s)
    # decision_function : plus bas = plus anormal -> on inverse le signe pour
    # que "score élevé = plus suspect", cohérent avec logreg_scores.
    iso_scores = -iso.decision_function(X_test_s)
    iso_preds = (iso.predict(X_test_s) == -1).astype(int)

    metrics = {
        "n_train": int(len(y_train)),
        "n_test": int(len(y_test)),
        "fraud_rate_test": float(y_test.mean()),
        "logistic_regression": {
            "pr_auc": float(average_precision_score(y_test, logreg_scores)),
            "confusion_matrix": confusion_matrix(y_test, logreg_preds).tolist(),
            "report": classification_report(y_test, logreg_preds, output_dict=True),
        },
        "isolation_forest": {
            "pr_auc": float(average_precision_score(y_test, iso_scores)),
            "confusion_matrix": confusion_matrix(y_test, iso_preds).tolist(),
            "report": classification_report(y_test, iso_preds, output_dict=True),
        },
    }

    config.ML_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(scaler, config.ML_MODELS_DIR / "scaler.joblib")
    joblib.dump(logreg, config.ML_MODELS_DIR / "logistic_regression.joblib")
    joblib.dump(iso, config.ML_MODELS_DIR / "isolation_forest.joblib")

    config.ML_METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(config.ML_METRICS_PATH, "w") as f:
        json.dump(metrics, f, indent=2)

    return metrics


if __name__ == "__main__":
    result_metrics = train_and_evaluate()

    print(
        f"Train: {result_metrics['n_train']:,} | Test: {result_metrics['n_test']:,} "
        f"(taux de fraude test: {result_metrics['fraud_rate_test']:.3%})"
    )
    for model_name in ("logistic_regression", "isolation_forest"):
        m = result_metrics[model_name]
        report_1 = m["report"].get("1", {})
        print(f"\n{model_name}")
        print(f"  PR-AUC        : {m['pr_auc']:.4f}")
        print(f"  Precision(1)  : {report_1.get('precision', 0):.3f}")
        print(f"  Recall(1)     : {report_1.get('recall', 0):.3f}")
        print(f"  Confusion     : {m['confusion_matrix']}")
