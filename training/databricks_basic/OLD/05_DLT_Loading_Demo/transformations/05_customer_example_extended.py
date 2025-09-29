# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Example Extended

# COMMAND ----------

import dlt, re

from pyspark.sql.types import DateType, FloatType, StringType, IntegerType, StructType, StructField, LongType, TimestampType
from pyspark.sql.functions import col, to_date, regexp_replace, current_timestamp, regexp_extract, to_timestamp, date_format, concat, lit, max

from datetime import datetime, timedelta, date

# COMMAND ----------

default_catalog = spark.conf.get("catalog")
parquet_path = f"/Volumes/{default_catalog}/raw/files/Customers/delta_customer/"

# COMMAND ----------

def create_table_per_country(country):
    suffix = (country
              .replace(" ", "_")
              .replace("-", "_")
              .replace("'", "_")
              .replace(".", "_")
              .replace(",", "_")
              .replace(";", "_")
              .replace(":", "_")
              .replace("!", "_")
              .replace("?", "_")
              .replace("(", "_")
              .replace(")", "_")
              .replace("[", "_")
              .replace("]", "_")
              .replace("{", "_")
              .replace("}", "_")
              .replace("/", "_")
              .replace("\\", "_")
              .replace("|", "_")
              .replace("&", "_")
              .replace("@", "_")
              .replace("#", "_")
              .replace("%", "_")
              .replace("+", "_")
              .replace("=", "_")
              .replace("$", "_")
              .replace("^", "_")
              .replace("~", "_")
              .lower()
            )

    @dlt.table(
        name = f"delta_customer_{suffix}",
        comment =f"This is a subset of customer_delta table for country: {country}",
        table_properties = {"quality": "bronze"}
    )

    def build_table():
        df = spark.readStream.table("delta_customer").filter(col("country") == country)
        return df

# COMMAND ----------

@dlt.table
def distinct_customer_countries():
    return (
        spark.read.table("delta_customer")
        .select("country")
        .distinct()
    )

# COMMAND ----------

df = spark.read.parquet(parquet_path)
countries = [row["country"] for row in df.select("country").distinct().collect()]

# COMMAND ----------

for country in countries:
    create_table_per_country(country)
