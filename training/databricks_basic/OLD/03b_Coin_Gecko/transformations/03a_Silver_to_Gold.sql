-- Sample 1
CREATE OR REFRESH MATERIALIZED VIEW gold.dlt_coingecko_union AS
SELECT
  *
FROM
  silver.dlt_coingecko_binancecoin
UNION
SELECT
  *
FROM
  silver.dlt_coingecko_bitcoin
UNION
SELECT
  *
FROM
  silver.dlt_coingecko_ethereum
UNION
SELECT
  *
FROM
  silver.dlt_coingecko_ripple
UNION
SELECT
  *
FROM
  silver.dlt_coingecko_tether