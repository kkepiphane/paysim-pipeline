.PHONY: up down test bronze silver gold pipeline analytics

up:            ## Démarre Airflow (UI sur http://localhost:8080)
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
