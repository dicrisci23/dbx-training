# Databricks notebook source
# MAGIC %md
# MAGIC # CoinGecko Silver 2 Gold

# COMMAND ----------

import re
from pyspark.sql.functions import *

# COMMAND ----------

def transform_email(email):
    match = re.match(r'^([^@]+)@', email)
    if match:
        username = match.group(1)
        username = re.sub(r'[._]', '_', username)
        return username
    else:
        return None

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
user_catalog_name = transform_email(user_id)

market_silver_table = f"{user_catalog_name}.silver.coingecko_markets"
coin_silver_table = f"{user_catalog_name}.silver.coingecko_coins"

market_gold_table = f"{user_catalog_name}.gold.coingecko_markets"
coin_gold_table = f"{user_catalog_name}.gold.coingecko_coins"

print(f"Market-Gold-Table: {market_gold_table}")
print(f"Coin-Gold-Table: {coin_gold_table}")

print(f"Market-Silver-Table: {market_silver_table}")
print(f"Coin-Silver-Table: {coin_silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Markets Data

# COMMAND ----------

market_silver_df = spark.table(market_silver_table)
display(market_silver_df)

# COMMAND ----------

# Aggregate the silver table to create the gold table
market_gold_df = market_silver_df.groupBy("year", "month", "date").agg(
    avg("current_price").alias("avg_current_price"),
    sum("market_cap").alias("total_market_cap"),
    avg("market_cap_rank").alias("avg_market_cap_rank"),
    sum("fully_diluted_valuation").alias("total_fully_diluted_valuation"),
    sum("total_volume").alias("total_volume"),
    avg("high_24h").alias("avg_high_24h"),
    avg("low_24h").alias("avg_low_24h"),
    avg("price_change_24h").alias("avg_price_change_24h"),
    avg("price_change_percentage_24h").alias("avg_price_change_percentage_24h"),
    sum("market_cap_change_24h").alias("total_market_cap_change_24h"),
    avg("market_cap_change_percentage_24h").alias("avg_market_cap_change_percentage_24h"),
    avg("circulating_supply").alias("avg_circulating_supply"),
    avg("total_supply").alias("avg_total_supply"),
    avg("max_supply").alias("avg_max_supply"),
    avg("ath").alias("avg_ath"),
    avg("ath_change_percentage").alias("avg_ath_change_percentage"),
    avg("atl").alias("avg_atl"),
    avg("atl_change_percentage").alias("avg_atl_change_percentage")
)

# Display the aggregated gold dataframe
display(market_gold_df)

# COMMAND ----------

# save the dataframe as a delta table
market_gold_df.write.format("delta").mode("overwrite").saveAsTable(market_gold_table)

# COMMAND ----------

spark.sql(f"SELECT * FROM {market_gold_table}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coins

# COMMAND ----------

# MAGIC %md
# MAGIC ## extend the example
# MAGIC What else could we do with the tables?
