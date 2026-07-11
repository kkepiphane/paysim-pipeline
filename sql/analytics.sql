-- Requêtes analytiques sur la couche Gold (via DuckDB).
-- Usage : duckdb < sql/analytics.sql
-- DuckDB lit directement les fichiers Parquet, sans import.

-- 1. Taux de fraude global par type de transaction.
SELECT
    transaction_type,
    SUM(n_transactions)              AS total,
    SUM(n_fraud)                     AS frauds,
    ROUND(SUM(n_fraud) * 100.0 / SUM(n_transactions), 3) AS fraud_pct
FROM read_parquet('data/gold/agg_fraud_by_type_hour/*.parquet')
GROUP BY transaction_type
ORDER BY fraud_pct DESC;

-- 2. Heures de la journée les plus à risque (toutes transactions).
SELECT
    hour_of_day,
    SUM(n_fraud)                     AS frauds,
    ROUND(SUM(n_fraud) * 100.0 / SUM(n_transactions), 3) AS fraud_pct
FROM read_parquet('data/gold/agg_fraud_by_type_hour/*.parquet')
GROUP BY hour_of_day
ORDER BY frauds DESC
LIMIT 10;

-- 3. La fraude se concentre-t-elle sur les gros montants ?
SELECT
    amount_band,
    n_transactions,
    n_fraud,
    ROUND(fraud_rate * 100, 3)       AS fraud_pct
FROM read_parquet('data/gold/agg_fraud_by_amount_band/*.parquet')
ORDER BY
    CASE amount_band
        WHEN '0-1K' THEN 1 WHEN '1K-10K' THEN 2
        WHEN '10K-100K' THEN 3 WHEN '100K-1M' THEN 4 ELSE 5
    END;

-- 4. Top 10 comptes par volume sortant, avec leur exposition à la fraude.
SELECT
    name_orig,
    ROUND(total_out, 2)              AS total_out,
    n_transactions,
    n_fraud
FROM read_parquet('data/gold/agg_top_accounts/*.parquet')
ORDER BY total_out DESC
LIMIT 10;

-- 5. Croisement fraude réelle vs incohérence de solde (via les faits).
SELECT
    is_fraud,
    balance_inconsistent,
    COUNT(*)                         AS n
FROM read_parquet('data/gold/fact_transactions/*.parquet')
GROUP BY is_fraud, balance_inconsistent
ORDER BY is_fraud DESC, balance_inconsistent DESC;
