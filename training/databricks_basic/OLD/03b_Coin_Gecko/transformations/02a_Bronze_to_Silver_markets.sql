CREATE MATERIALIZED VIEW silver.dlt_coingecko_markets AS
SELECT
  id,
  symbol,
  name,
  image,
  current_price,
  market_cap,
  market_cap_rank,
  fully_diluted_valuation,
  total_volume,
  high_24h,
  low_24h,
  price_change_24h,
  price_change_percentage_24h,
  market_cap_change_24h,
  market_cap_change_percentage_24h,
  circulating_supply,
  total_supply,
  max_supply,
  ath,
  ath_change_percentage,
  ath_date,
  atl,
  atl_change_percentage,
  atl_date,
  -- parse roi string into struct
  parsed_roi.times       AS roi_times,
  parsed_roi.currency    AS roi_currency,
  parsed_roi.percentage  AS roi_percentage,
  last_updated,
  year,
  month,
  _rescued_data,
  _metadata,
  load_timestamp,
  file_name,
  filename_timestamp_str,
  filename_timestamp
FROM (
  SELECT *,
    from_json(roi, 'STRUCT<times DOUBLE, currency STRING, percentage DOUBLE>') AS parsed_roi
FROM bronze.dlt_coingecko_markets
)