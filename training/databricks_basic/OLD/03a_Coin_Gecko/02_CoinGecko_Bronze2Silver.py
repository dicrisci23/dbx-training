# Databricks notebook source
# MAGIC %md
# MAGIC # CoinGecko Bronze 2 Silver

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

market_bronze_table = f"{user_catalog_name}.bronze.coingecko_markets"
coin_bronze_table = f"{user_catalog_name}.bronze.coingecko_coins"

market_silver_table = f"{user_catalog_name}.silver.coingecko_markets"
coin_silver_table = f"{user_catalog_name}.silver.coingecko_coins"

print(f"Market-Bronze-Table: {market_bronze_table}")
print(f"Coin-Bronze-Table: {coin_bronze_table}")

print(f"Market-Silver-Table: {market_silver_table}")
print(f"Coin-Silver-Table: {coin_silver_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Markets Data

# COMMAND ----------

market_bronze_df = spark.table(market_bronze_table)
display(market_bronze_df)

# COMMAND ----------

# Extracting the 'times', 'currency', and 'percentage' from the 'roi' column
market_bronze_df = (market_bronze_df.withColumn("roi_times", get_json_object(col("roi"), "$.times"))
                                   .withColumn("roi_currency", get_json_object(col("roi"), "$.currency")) 
                                   .withColumn("roi_percentage", get_json_object(col("roi"), "$.percentage"))
)

display(market_bronze_df)

# COMMAND ----------

market_bronze_df = (
    market_bronze_df
      .withColumn("file_name", col("_metadata.file_name"))
      .withColumn("file_path", col("_metadata.file_path"))
      .withColumn("file_modification_time", col("_metadata.file_modification_time"))
      )
display(market_bronze_df)

# COMMAND ----------

# Extracting the load_timestamp from the file_path
timestamp_pattern = r'(\d{8}T\d{6}Z)'
market_bronze_df = market_bronze_df.withColumn("load_timestamp", to_timestamp(regexp_extract(col("file_path"), timestamp_pattern, 1), "yyyyMMdd'T'HHmmss'Z'"))

# Extracting the day from the load_timestamp
market_bronze_df = market_bronze_df.withColumn("date", to_date(col("load_timestamp")))

display(market_bronze_df)

# COMMAND ----------

columns_to_drop = ["_rescued_data", "_metadata", "roi"]
market_bronze_df = market_bronze_df.drop(*columns_to_drop)
display(market_bronze_df)

# COMMAND ----------

market_silver_table

# COMMAND ----------

# save the dataframe as a delta table
market_bronze_df.write.format("delta").mode("overwrite").saveAsTable(market_silver_table)

# COMMAND ----------

spark.sql(f"SELECT * FROM {market_silver_table}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coins

# COMMAND ----------

tables_df = spark.sql(f"SHOW TABLES IN {user_catalog_name}.bronze LIKE 'coingecko_coins_*'")
tables_list = [row.tableName for row in tables_df.collect()]
display(tables_list)

# COMMAND ----------

spark.sql(f"SELECT * FROM {user_catalog_name}.bronze.coingecko_coins_bitcoin").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union all the coins

# COMMAND ----------

union_df = None

for table_name in tables_list:
    temp_df = spark.sql(f"SELECT * FROM {user_catalog_name}.bronze.{table_name}")
    if union_df is None:
        union_df = temp_df
    else:
        union_df = union_df.union(temp_df)

display(union_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## extend the example
# MAGIC What else could we do with the tables?

# COMMAND ----------


