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
| Metadata DB | **PostgreSQL 16** | backend Airflow (SQLite non recommandé hors tests) |
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

> `make up` exécute d'abord `init-dirs`, qui donne à `./logs` et `./data/{bronze,silver,gold}`
> les droits de l'utilisateur `airflow` (uid 50000) du conteneur. Sans ça, ces dossiers
> appartiennent à ton utilisateur hôte (ou à root) et Spark/le scheduler plantent avec
> `PermissionError` / `Mkdirs failed` en essayant d'y écrire.

**Se connecter à l'UI.** Identifiants par défaut : `admin` / `admin`
(définis dans `.env` via `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`, créés
au démarrage grâce aux variables `_AIRFLOW_WWW_USER_*` de l'image officielle Airflow).
Change ces valeurs dans `.env` avant de lancer `make up` si le port 8080 est exposé
au-delà de ta machine locale — ce ne sont pas des identifiants pensés pour un accès public.

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

**Pourquoi un seuil de gate par `transaction_type` et pas un seuil global ?**
Sur le dataset complet (6,3M lignes), le taux d'incohérence de solde varie énormément selon le type : ~95% sur `TRANSFER`, ~89% sur `CASH_OUT` (le simulateur PaySim ne répercute pas toujours correctement le solde émetteur sur ces deux types — precisément les deux seuls où la fraude existe dans ce dataset), contre ~54% sur `PAYMENT` et ~30% sur `DEBIT`. Un seuil global mélangerait ces populations et serait soit toujours en échec (si calé sous 90%), soit incapable de détecter une vraie dérive sur un type à faible volume comme `DEBIT`, noyé dans la moyenne pondérée. Les seuils par type vivent dans `config.BALANCE_INCONSISTENCY_THRESHOLDS`.

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
- Dashboard Streamlit branché sur le Gold — charger les agrégats dans **Postgres**
  (déjà présent pour Airflow) plutôt que de rescanner du Parquet à chaque requête
  concurrente, DuckDB restant le bon choix pour l'analytique ad hoc en local
- Modèle ML de scoring de fraude (baseline vs Isolation Forest)
- Migration vers Delta Lake (transactions ACID, time travel)
