-- Substitua pelo seu catálogo/esquema
USE CATALOG main;
USE SCHEMA gold;

SELECT ticker, COUNT(*) AS rows, MIN(date) AS min_d, MAX(date) AS max_d
FROM silver.prices_silver
GROUP BY 1
ORDER BY 1;

SELECT * FROM gold.prices_features ORDER BY ticker, date DESC LIMIT 50;
