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

## Scoring de fraude (ML)

```
Silver (TRANSFER + CASH_OUT uniquement)
     │
  FEATURES  ── error_balance_orig/dest, hour_of_day, is_transfer, ...
     │        (Spark, écrit Gold/ml_features)
  TRAIN     ── Logistic Regression (baseline) vs Isolation Forest (anomalie)
     │        (scikit-learn, PR-AUC comme métrique de référence)
  data/ml/  ── models/*.joblib + metrics.json
```

```bash
make ml-features   # feature engineering (Spark -> Gold/ml_features)
make ml-train       # entraînement + évaluation (scikit-learn -> data/ml/)
```

**Pourquoi restreindre à TRANSFER/CASH_OUT ?** Dans PaySim, `isFraud` ne vaut jamais 1 en dehors de ces deux types (vérifié sur les 6,3M lignes). Entraîner sur CASH_IN/PAYMENT/DEBIT n'ajouterait aucun signal et diluerait encore plus un déséquilibre de classe déjà extrême (~0,3% de fraude sur la population éligible).

**Pourquoi PR-AUC plutôt qu'accuracy ?** Avec ~0,3% de positifs, un modèle qui prédit toujours "non fraude" atteint 99,7% d'accuracy en étant inutile. PR-AUC (average precision) et le rappel/précision sur la classe minoritaire sont les métriques qui comptent ici.

**Pourquoi scikit-learn et pas Spark ML pour Isolation Forest ?** Spark ML n'implémente pas Isolation Forest nativement. Spark fait l'ETL à l'échelle (feature engineering sur 6,3M lignes), scikit-learn prend le relais sur la table de features déjà réduite (2,77M lignes, ~10 colonnes) qui tient confortablement en mémoire — découpage standard plutôt que de forcer un algorithme non distribué dans Spark.

**Résultats de référence** (dataset complet) : Logistic Regression atteint un rappel de 97,7% pour une précision de 4% (PR-AUC 0,684) — bon filet de sécurité, beaucoup de faux positifs à trier ensuite. Isolation Forest plafonne à PR-AUC 0,046, à peine mieux qu'aléatoire : la fraude sur ce dataset n'est pas structurellement "isolée" dans l'espace des features au sens non supervisé, ce qui est en soi un résultat instructif — la détection d'anomalie non supervisée n'est pas une solution universelle.

---

## Dashboard (Streamlit + warehouse Postgres)

```
Gold (agrégats)
     │
  LOAD-WAREHOUSE  ── charge agg_*, dim_transaction_type + un résumé KPI
     │               dans un Postgres dédié ("warehouse", séparé de celui d'Airflow)
  DASHBOARD        ── Streamlit lit le warehouse (jamais le Parquet directement)
```

```bash
make load-warehouse   # démarre le Postgres warehouse + y charge le Gold
make dashboard        # UI sur http://localhost:8501
```

**Pourquoi un second Postgres et pas réutiliser celui d'Airflow ?** La metadata DB d'Airflow (`dag_run`, `task_instance`, ...) est une base opérationnelle avec son propre cycle de vie ; y ajouter des tables métier consultées par un dashboard mélangerait deux charges et deux responsabilités différentes. Le warehouse est une instance Postgres à part (`docker-compose.yml`, service `warehouse`, port `5433`).

**Pourquoi ne charger que les agrégats, jamais `fact_transactions` ni `dim_account` ?** Ce sont des tables de plusieurs millions de lignes — les recharger dans Postgres à chaque run serait lent et inutile : le dashboard n'a besoin que des agrégats déjà réduits (quelques centaines de lignes) et d'un résumé KPI, calculé une fois par DuckDB directement sur le Parquet.

**Pourquoi pas `DataFrame.to_sql`/`read_sql_table` ?** pandas ≥ 2.1 exige SQLAlchemy ≥ 2.0 pour ces raccourcis, mais `apache-airflow==2.9.3` impose SQLAlchemy < 2.0 dans ce même `requirements.txt`. Plutôt que de dégrader silencieusement vers un mode SQLite non fonctionnel (le comportement par défaut de pandas quand la version ne convient pas), `src/warehouse/load.py` écrit/lit via SQLAlchemy Core directement — explicite, sans dépendre d'une passerelle interne de pandas.

---

## Streaming (Kafka)

```
CSV source
     │
  PRODUCER   ── rejoue le CSV ligne à ligne vers Kafka (topic paysim-transactions)
     │
  CONSUMER   ── micro-batches -> Parquet, partitionné par `type`
     │           (data/bronze_streaming/, séparé du Bronze batch, mode append)
```

```bash
make produce   # démarre Kafka + rejoue 1000 lignes du CSV (--limit/--rate configurables)
make consume   # consomme le topic par micro-batches (--batch-size/--batch-timeout)
```

**Pourquoi un répertoire séparé du Bronze batch ?** `ingest_bronze` écrit en `mode=overwrite` (idempotent : rejouer tout le DAG ne duplique rien). Un flux streaming est par nature additif — chaque micro-batch est un nouvel arrivage, pas un rejeu complet. Mélanger les deux dans un même répertoire casserait l'idempotence de l'un ou l'autre. Le schéma de sortie reste néanmoins identique (mêmes colonnes, même partitionnement par `type`), pour qu'un même Silver puisse en principe consommer les deux origines.

**Pourquoi `confluent-kafka` et pas `kafka-python` ?** `kafka-python` vendait sa propre copie de `six`, cassée sous Python 3.12 (`ModuleNotFoundError: kafka.vendor.six.moves`) — un problème connu, non corrigé dans la dernière release. `confluent-kafka` (wheel précompilée autour de `librdkafka`) fonctionne sans contournement.

**Pourquoi Confluent (`cp-kafka` + `cp-zookeeper`) et pas l'image `apache/kafka` en mode KRaft (sans Zookeeper) ?** KRaft est plus simple (un seul conteneur) et c'est le sens de l'histoire pour Kafka, mais nécessite ici de tirer une image non testée dans tous les environnements réseau. Le couple Confluent est le standard le plus largement répandu et fonctionne de façon identique côté producer/consumer — seul le `docker-compose.yml` change si tu migres vers KRaft plus tard.

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
| ML | **scikit-learn** | scoring de fraude sur la table de features |
| Dashboard | **Streamlit + Plotly** | BI, branché sur un warehouse Postgres dédié |
| Streaming | **Kafka (Confluent)** | simulation d'ingestion temps réel |
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
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=$(pwd)

make pipeline      # bronze → silver → gold
make analytics     # requêtes DuckDB
make test          # tests unitaires
make ml            # feature engineering + scoring de fraude
make load-warehouse && make dashboard   # dashboard Streamlit
make produce && make consume            # streaming Kafka
```

> Tous les `make` ci-dessus (hors `make up`/`make down`, qui tournent dans Docker)
> exécutent des scripts Python locaux (`python`, `streamlit`, ...) — ils doivent
> être lancés avec le venv **activé** dans le terminal courant (`source .venv/bin/activate`).
> `make` hérite du PATH du shell qui l'appelle ; sans venv activé tu auras
> `python: Aucun fichier ou dossier de ce nom` (Ubuntu ne fournit pas de binaire
> `python` nu) ou `streamlit: commande introuvable`.

### Avec Airflow (Docker)

```bash
make up            # UI sur http://localhost:8080
# Déclencher le DAG "paysim_medallion_pipeline"
```

> `make up` exécute d'abord `init-dirs`, qui rend `./logs` et `./data/{bronze,silver,gold}`
> accessibles en écriture à la fois pour l'utilisateur `airflow` (uid 50000) du conteneur
> et pour ton utilisateur hôte (`chmod 777` — acceptable ici car projet local mono-utilisateur,
> pas un service exposé). Sans ça, ces dossiers appartiennent à un seul des deux mondes et
> soit Airflow/Spark plante avec `PermissionError`/`Mkdirs failed` côté conteneur, soit
> `make bronze`/`make gold`/`make ml-*` en local échoue à écrire dans des dossiers déjà
> possédés par le conteneur suite à un run Docker précédent.

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
│   ├── quality/       gates de qualité
│   ├── ml/            feature engineering + scoring de fraude
│   ├── warehouse/     chargement des agrégats Gold -> Postgres
│   └── streaming/     producer/consumer Kafka (bronze_streaming)
├── dashboard/         dashboard Streamlit (lit le warehouse)
├── sql/               requêtes analytiques DuckDB
├── tests/             tests unitaires pytest
└── docs/              architecture détaillée
```

---

## Pistes d'extension

- Migration vers Delta Lake (transactions ACID, time travel) — nécessite un accès
  à Maven Central pour résoudre le JAR `io.delta:delta-spark` au démarrage de la
  session Spark, indisponible dans certains environnements réseau restreints
