CREATE OR REFRESH MATERIALIZED VIEW silver.dlt_coingecko_binancecoin AS
SELECT
  id,
  symbol,
  name,
  market_data.ath.usd AS ath_usd,
  market_data.ath.chf AS ath_chf,
  market_data.atl.usd AS atl_usd,
  market_data.atl.chf AS atl_chf,
  market_data.current_price.usd AS current_price_usd,
  market_data.current_price.chf AS current_price_chf,
  market_data.market_cap.usd AS market_cap_usd,
  market_data.market_cap.chf AS market_cap_chf,
  market_data.total_volume.usd AS total_volume_usd,
  market_data.total_volume.chf AS total_volume_chf,
  market_data.price_change_24h AS price_change_24h,
  market_data.price_change_24h_in_currency.usd AS price_change_24h_usd,
  market_data.price_change_24h_in_currency.chf AS price_change_24h_chf,
  market_data.price_change_percentage_24h AS price_change_percentage_24h
FROM
  bronze.dlt_coingecko_binancecoin;

CREATE OR REFRESH MATERIALIZED VIEW silver.dlt_coingecko_bitcoin AS
SELECT
  id,
  symbol,
  name,
  market_data.ath.usd AS ath_usd,
  market_data.ath.chf AS ath_chf,
  market_data.atl.usd AS atl_usd,
  market_data.atl.chf AS atl_chf,
  market_data.current_price.usd AS current_price_usd,
  market_data.current_price.chf AS current_price_chf,
  market_data.market_cap.usd AS market_cap_usd,
  market_data.market_cap.chf AS market_cap_chf,
  market_data.total_volume.usd AS total_volume_usd,
  market_data.total_volume.chf AS total_volume_chf,
  market_data.price_change_24h AS price_change_24h,
  market_data.price_change_24h_in_currency.usd AS price_change_24h_usd,
  market_data.price_change_24h_in_currency.chf AS price_change_24h_chf,
  market_data.price_change_percentage_24h AS price_change_percentage_24h
FROM
  bronze.dlt_coingecko_bitcoin;

CREATE OR REFRESH MATERIALIZED VIEW silver.dlt_coingecko_ethereum AS
SELECT
  id,
  symbol,
  name,
  market_data.ath.usd AS ath_usd,
  market_data.ath.chf AS ath_chf,
  market_data.atl.usd AS atl_usd,
  market_data.atl.chf AS atl_chf,
  market_data.current_price.usd AS current_price_usd,
  market_data.current_price.chf AS current_price_chf,
  market_data.market_cap.usd AS market_cap_usd,
  market_data.market_cap.chf AS market_cap_chf,
  market_data.total_volume.usd AS total_volume_usd,
  market_data.total_volume.chf AS total_volume_chf,
  market_data.price_change_24h AS price_change_24h,
  market_data.price_change_24h_in_currency.usd AS price_change_24h_usd,
  market_data.price_change_24h_in_currency.chf AS price_change_24h_chf,
  market_data.price_change_percentage_24h AS price_change_percentage_24h
FROM
  bronze.dlt_coingecko_ethereum;

CREATE OR REFRESH MATERIALIZED VIEW silver.dlt_coingecko_ripple AS
SELECT
  id,
  symbol,
  name,
  market_data.ath.usd AS ath_usd,
  market_data.ath.chf AS ath_chf,
  market_data.atl.usd AS atl_usd,
  market_data.atl.chf AS atl_chf,
  market_data.current_price.usd AS current_price_usd,
  market_data.current_price.chf AS current_price_chf,
  market_data.market_cap.usd AS market_cap_usd,
  market_data.market_cap.chf AS market_cap_chf,
  market_data.total_volume.usd AS total_volume_usd,
  market_data.total_volume.chf AS total_volume_chf,
  market_data.price_change_24h AS price_change_24h,
  market_data.price_change_24h_in_currency.usd AS price_change_24h_usd,
  market_data.price_change_24h_in_currency.chf AS price_change_24h_chf,
  market_data.price_change_percentage_24h AS price_change_percentage_24h
FROM
  bronze.dlt_coingecko_ripple;

CREATE OR REFRESH MATERIALIZED VIEW silver.dlt_coingecko_tether AS
SELECT
  id,
  symbol,
  name,
  market_data.ath.usd AS ath_usd,
  market_data.ath.chf AS ath_chf,
  market_data.atl.usd AS atl_usd,
  market_data.atl.chf AS atl_chf,
  market_data.current_price.usd AS current_price_usd,
  market_data.current_price.chf AS current_price_chf,
  market_data.market_cap.usd AS market_cap_usd,
  market_data.market_cap.chf AS market_cap_chf,
  market_data.total_volume.usd AS total_volume_usd,
  market_data.total_volume.chf AS total_volume_chf,
  market_data.price_change_24h AS price_change_24h,
  market_data.price_change_24h_in_currency.usd AS price_change_24h_usd,
  market_data.price_change_24h_in_currency.chf AS price_change_24h_chf,
  market_data.price_change_percentage_24h AS price_change_percentage_24h
FROM
  bronze.dlt_coingecko_tether;