# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Example Python

# COMMAND ----------

import dlt, re

from pyspark.sql.types import DateType, FloatType, StringType, IntegerType, StructType, StructField, LongType, TimestampType
from pyspark.sql.functions import col, to_date, regexp_replace, current_timestamp, regexp_extract, to_timestamp, date_format, concat, lit, max

from datetime import datetime, timedelta, date

# COMMAND ----------

default_catalog = spark.conf.get("catalog")
parquet_path = f"/Volumes/{default_catalog}/raw/files/Customers/delta_customer/"

# COMMAND ----------

table_name = "delta_customer"

@dlt.table(
    name = table_name,
    comment = 'This is an append only table',
    table_properties = {"quality": "bronze"}
)

def build_table():
    df = (spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "parquet")
                .option("mergeSchema", "true")
                .load(parquet_path)
                .select(
                    "*"
                    , col('_metadata')
                )
    )   

    # Add the filename by extracting it from _metadata.file_path
    df = df.withColumn("file_name", col("_metadata").getField("file_name"))

    # Extract timestamp using regex
    df = df.withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{14})", 1))
    df = df.withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMddHHmmss"))

    df = df.withColumn("dlt_load_timestamp", (current_timestamp()))
    
    # drop unnecessary columns
    df = df.drop("filename_timestamp_str")
    df = df.drop("file_name")
    
    return df
