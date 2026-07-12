.PHONY: up down init-dirs test bronze silver gold pipeline analytics

init-dirs:     ## Prépare ./logs et ./data/{bronze,silver,gold} avec les droits de l'utilisateur airflow (uid 50000) du conteneur
	mkdir -p logs data/bronze data/silver data/gold data/raw
	docker run --rm -u root \
		-v $(CURDIR)/logs:/opt/airflow/logs \
		-v $(CURDIR)/data:/opt/airflow/data \
		--entrypoint bash apache/airflow:2.9.3-python3.11 \
		-c "chown -R 50000:0 /opt/airflow/logs /opt/airflow/data/bronze /opt/airflow/data/silver /opt/airflow/data/gold && \
		    chmod -R 775 /opt/airflow/logs /opt/airflow/data/bronze /opt/airflow/data/silver /opt/airflow/data/gold"

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
