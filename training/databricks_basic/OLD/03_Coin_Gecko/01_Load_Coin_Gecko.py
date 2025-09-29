# Databricks notebook source
# MAGIC %md
# MAGIC # CoinGecko
# MAGIC
# MAGIC This notebooks downloads data from https://coingecko.com/
# MAGIC
# MAGIC https://docs.coingecko.com/  
# MAGIC https://docs.coingecko.com/v3.0.1/reference/coins-markets  
# MAGIC  
# MAGIC

# COMMAND ----------

from datetime import datetime
import requests
import json
import re
import os
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# COMMAND ----------

now = datetime.now()
year = now.year
month = now.month
load_timestamp = now.strftime("%Y%m%dT%H%M%SZ")
print(f"Load timestamp: {load_timestamp}")

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

folder_location   = f"/Volumes/{user_catalog_name}/raw/files/CoinGecko"

print(f"Your file location: {folder_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## get market data

# COMMAND ----------

url = "https://api.coingecko.com/api/v3/coins/markets"
headers = {"accept": "application/json"}
response = requests.get(url, headers=headers, params={"vs_currency": "usd"})

json_response = response.json()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("image", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", LongType(), True),
    StructField("market_cap_rank", LongType(), True),
    StructField("fully_diluted_valuation", LongType(), True),
    StructField("total_volume", LongType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("price_change_24h", DoubleType(), True),
    StructField("price_change_percentage_24h", DoubleType(), True),
    StructField("market_cap_change_24h", LongType(), True),
    StructField("market_cap_change_percentage_24h", DoubleType(), True),
    StructField("circulating_supply", DoubleType(), True),
    StructField("total_supply", DoubleType(), True),
    StructField("max_supply", DoubleType(), True),
    StructField("ath", DoubleType(), True),
    StructField("ath_change_percentage", DoubleType(), True),
    StructField("ath_date", StringType(), True),
    StructField("atl", DoubleType(), True),
    StructField("atl_change_percentage", DoubleType(), True),
    StructField("atl_date", StringType(), True),
    StructField("roi", StringType(), True),
    StructField("last_updated", StringType(), True)
])

df = spark.createDataFrame(json_response, schema)

coin_id_list = [coin["id"] for coin in json_response]

display(df)

# COMMAND ----------

 # Construct the output file path using partitions
spark_path = os.path.join(
    folder_location,
    "markets", 
    f"year={year}",
    f"month={month}",
    f"coingecko_markets_{load_timestamp}_spark_df"
)

print(f"Save it to: {spark_path}")

df.write.mode("overwrite").parquet(spark_path)

# COMMAND ----------

 # Construct the output file path using partitions
path = os.path.join(
    folder_location,
    "markets", 
    f"year={year}",
    f"month={month}",
    f"coingecko_markets_{load_timestamp}.parquet"
)

pdf = df.toPandas()
pdf.to_parquet(path)

# Create the output directory (if it doesn't exist)
dbutils.fs.mkdirs(os.path.dirname(path))

# Save the Pandas DataFrame as a Parquet file (nested structure preserved)
pdf.to_parquet(f"{path}", index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let's have a look at the downloaded files

# COMMAND ----------

# delete the spark df
dbutils.fs.rm(spark_path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Coin Data

# COMMAND ----------

display(coin_id_list)

# COMMAND ----------

# just take the first 10 coins from the list
coin_id_list = coin_id_list[:5]
display(coin_id_list)

# COMMAND ----------

coin_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("market_data", StructType([
        StructField("current_price", StructType([
            StructField("usd", DoubleType(), True),
            StructField("chf", DoubleType(), True)
        ]), True),
        StructField("ath", StructType([
            StructField("usd", DoubleType(), True),
            StructField("chf", DoubleType(), True)
        ]), True),
        StructField("atl", StructType([
            StructField("usd", DoubleType(), True),
            StructField("chf", DoubleType(), True)
        ]), True),
        StructField("price_change_24h", DoubleType(), True),
        StructField("price_change_24h_in_currency", StructType([
            StructField("usd", DoubleType(), True),
            StructField("chf", DoubleType(), True)
        ]), True),
        StructField("price_change_percentage_24h", DoubleType(), True),
        StructField("market_cap", StructType([
            StructField("usd", DoubleType(), True),
            StructField("chf", DoubleType(), True)
        ]), True),
        StructField("total_volume", StructType([
            StructField("usd", DoubleType(), True),
            StructField("chf", DoubleType(), True)
        ]), True)
    ]), True)
])

# COMMAND ----------

for coin_id in coin_id_list:
  
  print(f"Get data for {coin_id}")

  url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
  headers = {"accept": "application/json"}
  response = requests.get(url, headers=headers)

  json_response = response.json()

  df = spark.createDataFrame([json_response], coin_schema)

  path = f"{folder_location}/coins/{coin_id}/year={year}/month={month}/coingecko_coins_{coin_id}_{load_timestamp}.parquet"

  pdf = df.toPandas()

  # Create the output directory (if it doesn't exist)
  dbutils.fs.mkdirs(os.path.dirname(path))

  # Save the Pandas DataFrame as a Parquet file (nested structure preserved)
  pdf.to_parquet(f"{path}", index=False)

# COMMAND ----------

# have a look at the data
df = spark.read.parquet(f"{folder_location}/coins/bitcoin/*/*/*.parquet")
display(df)

# COMMAND ----------


