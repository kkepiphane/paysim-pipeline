# PaySim Mobile Money — Pipeline Data Engineering

Pipeline batch de bout en bout traitant **6,3 millions de transactions mobile money** pour la détection de fraude, construit selon l'**architecture medallion** (Bronze / Silver / Gold), orchestré par Airflow, testé et conteneurisé.

> Cas d'usage pertinent pour le marché ouest-africain : le mobile money est le principal canal de transaction financière au Togo, Ghana et Nigeria.

---

## Architecture

```
CSV source (6.3M lignes)
     │
  BRONZE   ── ingestion brute, immuable, Parquet partitionné par `type`
     │        (+ métadonnées : ingestion_timestamp, source_file)
  [validate]  ── contrats de schéma (Great Expectations logic)
     │
  SILVER   ── nettoyage : typage, dédoublonnage, flag d'incohérence
     │        de solde, dérivation client/marchand
  [validate]  ── GATE qualité : échec du DAG si incohérence > seuil
     │
   GOLD    ── schéma en étoile : fact_transactions + dimensions
     │        + tables agrégées (fraude par type/heure/montant)
     │
  DuckDB   ── requêtes analytiques directes sur Parquet
```

### Schéma en étoile (Gold)

- **`fact_transactions`** — table de faits (montants, flags fraude, FK)
- **`dim_account`** — comptes (client / marchand)
- **`dim_transaction_type`** — 5 types (CASH_IN, CASH_OUT, TRANSFER, PAYMENT, DEBIT)
- **`dim_time`** — dérivée du `step` (1 step = 1 heure)
- **Agrégats** — `agg_fraud_by_type_hour`, `agg_top_accounts`, `agg_fraud_by_amount_band`

---

## Stack

| Composant | Outil | Rôle |
|---|---|---|
| Traitement | **PySpark 3.5** | distribué, adapté à la volumétrie |
| Stockage | **Parquet** | colonnaire, compressé, splittable |
| Orchestration | **Airflow 2.9** | DAG idempotent, dépendances |
| Qualité | **Great Expectations / assertions** | contrats intégrés au pipeline |
| Requêtes | **DuckDB** | analytique rapide sans serveur |
| Reproductibilité | **Docker** | environnement identique |

---

## Données

**PaySim** — Synthetic Financial Datasets For Fraud Detection
- Source : [kaggle.com/datasets/ealaxi/paysim1](https://www.kaggle.com/datasets/ealaxi/paysim1)
- 6,3 M lignes, ~470 Mo, basé sur des logs réels de mobile money africain
- Déposer le CSV dans `data/raw/`

---

## Lancer le projet

### En local (sans Airflow)

```bash
pip install -r requirements.txt
export PYTHONPATH=$(pwd)

make pipeline      # bronze → silver → gold
make analytics     # requêtes DuckDB
make test          # tests unitaires
```

### Avec Airflow (Docker)

```bash
make up            # UI sur http://localhost:8080
# Déclencher le DAG "paysim_medallion_pipeline"
```

---

## Décisions d'architecture

**Pourquoi partitionner le Bronze par `type` et pas par `step` ?**
`type` a 5 valeurs (faible cardinalité) → 5 dossiers avec de gros fichiers, efficaces à lire. `step` a ~743 valeurs → des centaines de petits fichiers (« small files problem »), pénalisant Spark. On partitionne sur ce qui est peu cardinal et souvent filtré.

**Pourquoi le Bronze est-il immuable ?**
Conserver la donnée brute telle quelle garantit la traçabilité et permet de rejouer tout nettoyage ultérieur sans re-télécharger la source. Toute logique métier vit en Silver.

**Schéma explicite plutôt qu'inférence.**
`inferSchema=true` lit deux fois le fichier et peut mal typer une colonne selon l'échantillon. En production, on impose le schéma pour un comportement déterministe.

**Les soldes destinataires marchands à zéro : bug ou métier ?**
Caractéristique du jeu de données : les soldes des comptes marchands (`M`) ne sont pas suivis. On ne flague donc l'incohérence que côté émetteur, sans supprimer d'information — on ajoute une colonne `balance_inconsistent` au lieu de filtrer des lignes.

**Idempotence.**
Chaque tâche écrit en `mode=overwrite`. Rejouer le DAG entier ne duplique aucune donnée.

---

## Structure

```
├── dags/              DAG Airflow
├── src/
│   ├── bronze/        ingestion
│   ├── silver/        nettoyage
│   ├── gold/          modélisation dimensionnelle
│   └── quality/       gates de qualité
├── sql/               requêtes analytiques DuckDB
├── tests/             tests unitaires pytest
└── docs/              architecture détaillée
```

---

## Pistes d'extension

- Couche streaming Kafka pour l'ingestion temps réel
- Dashboard Streamlit branché sur le Gold
- Modèle ML de scoring de fraude (baseline vs Isolation Forest)
- Migration vers Delta Lake (transactions ACID, time travel)
