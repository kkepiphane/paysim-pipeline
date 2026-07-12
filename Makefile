.PHONY: up down init-dirs test bronze silver gold pipeline analytics ml-features ml-train ml warehouse-up load-warehouse dashboard kafka-up produce consume

init-dirs:     ## Prépare ./logs et ./data/{bronze,silver,gold} en écriture pour le conteneur (uid 50000) ET pour l'exécution locale
	mkdir -p logs data/bronze data/silver data/gold data/raw data/bronze_streaming
	docker run --rm -u root \
		-v $(CURDIR)/logs:/opt/airflow/logs \
		-v $(CURDIR)/data:/opt/airflow/data \
		--entrypoint bash apache/airflow:2.9.3-python3.11 \
		-c "chown -R 50000:0 /opt/airflow/logs /opt/airflow/data/bronze /opt/airflow/data/silver /opt/airflow/data/gold && \
		    chmod -R 777 /opt/airflow/logs /opt/airflow/data/bronze /opt/airflow/data/silver /opt/airflow/data/gold"

up: init-dirs  ## Démarre Airflow (UI sur http://localhost:8080)
	docker compose up --build

down:          ## Arrête la stack
	docker compose down

test:          ## Lance les tests unitaires
	python -m pytest tests/ -v

bronze:        ## Ingestion Bronze en local
	python -m src.bronze.ingest

silver:        ## Transformation Silver en local
	python -m src.silver.transform

gold:          ## Modélisation Gold en local
	python -m src.gold.model

pipeline: bronze silver gold  ## Pipeline complet en local (hors Airflow)

analytics:     ## Requêtes DuckDB sur le Gold
	duckdb < sql/analytics.sql

ml-features:   ## Feature engineering pour le scoring de fraude (lit Silver, écrit Gold/ml_features)
	python -m src.ml.features

ml-train:      ## Entraîne et évalue Logistic Regression + Isolation Forest sur les features
	python -m src.ml.train

ml: ml-features ml-train  ## Pipeline ML complet (features + entraînement + évaluation)

warehouse-up:  ## Démarre uniquement le Postgres warehouse (dashboard), sans Airflow
	docker compose up -d warehouse

load-warehouse: warehouse-up  ## Charge les agrégats Gold dans le warehouse Postgres
	python -m src.warehouse.load

dashboard:     ## Lance le dashboard Streamlit (UI sur http://localhost:8501)
	streamlit run dashboard/app.py

kafka-up:      ## Démarre uniquement le broker Kafka
	docker compose up -d kafka

produce: kafka-up  ## Rejoue le CSV source vers Kafka (--limit=1000 par défaut, voir src/streaming/producer.py)
	python -m src.streaming.producer

consume: kafka-up  ## Consomme le topic Kafka en micro-batches vers data/bronze_streaming
	python -m src.streaming.consumer
