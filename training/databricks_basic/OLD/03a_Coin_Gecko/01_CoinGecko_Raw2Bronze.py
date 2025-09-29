# Databricks notebook source
# MAGIC %md
# MAGIC # CoinGecko Raw 2 Bronze
# MAGIC
# MAGIC This Notebook loads Raw Data into Bronze Table

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

raw_location   = f"/Volumes/{user_catalog_name}/raw/files/CoinGecko"
markets_location = f"/Volumes/{user_catalog_name}/raw/files/CoinGecko/markets"
coin_location = f"/Volumes/{user_catalog_name}/raw/files/CoinGecko/coins"

market_checkpoint_path = f"/Volumes/{user_catalog_name}/bronze/checkpoints/coingecko_markets"
market_bronze_table = f"{user_catalog_name}.bronze.coingecko_markets"

coin_checkpoint_path = f"/Volumes/{user_catalog_name}/bronze/checkpoints/coingecko_coins"
coin_bronze_table = f"{user_catalog_name}.bronze.coingecko_coins"

print(f"Markets-Location: {markets_location}")
print(f"Coin-Location: {coin_location}")

print(f"Market-Checkpoint-Path: {market_checkpoint_path}")
print(f"Coin-Checkpoint-Path: {coin_checkpoint_path}")

print(f"Market-Bronze-Table: {market_bronze_table}")
print(f"Coin-Bronze-Table: {coin_bronze_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Markets Data

# COMMAND ----------

# just for explaining!
markets_df = spark.read.format("parquet").load(markets_location).select("*", "_metadata")
display(markets_df)

# COMMAND ----------

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.schemaLocation", market_checkpoint_path)
  .load(markets_location)
  .select("*", "_metadata")





  
  .writeStream
  .option("checkpointLocation", market_checkpoint_path)
  .trigger(availableNow=True)
  .toTable(market_bronze_table))


# COMMAND ----------

sql = f"SELECT * FROM {market_bronze_table}"
spark.sql(sql).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Coins Data

# COMMAND ----------

# get all coin folders and save as list
coin_folders = [f for f in dbutils.fs.ls(coin_location) if f.isDir()]
display(coin_folders)

# COMMAND ----------

# get all coin files and save as list
coin_files = [c.name.rstrip('/') for c in coin_folders]
display(coin_files)

# COMMAND ----------

for coin in coin_files:
  print(f"Load {coin} to {coin_bronze_table}_{coin.replace('-', '_')}")
  table_name = f"{coin_bronze_table}_{coin.replace('-', '_')}"
  (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet").option("mergeSchema", "true")
    .option("cloudFiles.schemaLocation", f"{coin_checkpoint_path}/{coin}")
    .load(f"{coin_location}/{coin}")
    .select("*", "_metadata")
    .writeStream
    .option("checkpointLocation", f"{coin_checkpoint_path}/{coin}")
    .trigger(availableNow=True)
    .toTable(table_name)
    )

# COMMAND ----------


