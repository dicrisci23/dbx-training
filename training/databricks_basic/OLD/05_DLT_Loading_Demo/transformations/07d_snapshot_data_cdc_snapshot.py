# Databricks notebook source
# MAGIC %md
# MAGIC # Process Snapshot Data
# MAGIC ## Change Data Capture from Snapshot

# COMMAND ----------

import dlt, re

from pyspark.sql.types import DateType, FloatType, StringType, IntegerType, StructType, StructField, LongType, TimestampType
from pyspark.sql.functions import col, to_date, regexp_replace, current_timestamp, regexp_extract, to_timestamp, date_format, concat, lit, max

from datetime import datetime, timedelta, date

# COMMAND ----------

default_catalog = spark.conf.get("catalog")
parquet_path = f"/Volumes/{default_catalog}/raw/files/Customers/snapshot_customer"

# COMMAND ----------

# DBTITLE 1,customer_cdc_snapshot_scd1

def exist(file_path):
    try:
        dbutils.fs.ls(file_path)
        return True
    except:
        return False

# Function to determine the next snapshot based on date
def next_snapshot_and_version(latest_snapshot_version):
    # If no previous snapshot exists, start with the first day (replace with start date)
    if latest_snapshot_version is None:
        latest_snapshot_date = date(2025, 3, 1)
    else:
        latest_snapshot_date = datetime.strptime(str(latest_snapshot_version), "%Y%m%d") + timedelta(days=1)

    # Create the path to the next snapshot
    next_version = latest_snapshot_date.strftime("%Y%m%d")

    file_path = f"{parquet_path}/Year={latest_snapshot_date.year}/Month={latest_snapshot_date.month}/Day={latest_snapshot_date.day}/"

    if exist(file_path):
        df = spark.read.option("basePath", file_path).parquet(f"{file_path}*").select("*", col('_metadata'))

        # Add the filename by extracting it from _metadata.file_path
        df = df.withColumn("file_name", col("_metadata").getField("file_name"))

        # Extract timestamp using regex
        df = df.withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{14})", 1))
        df = df.withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMddHHmmss"))

        df = df.withColumn("dlt_load_timestamp", (current_timestamp()))
        
        # drop unnecessary columns
        df = df.drop("filename_timestamp_str")
        df = df.drop("file_name")
        
        return df, int(next_version)
    
    else:
        return None

dlt.create_streaming_live_table("snapshot_scd1")

dlt.apply_changes_from_snapshot(
    target="snapshot_scd1",
    source=next_snapshot_and_version,
    keys=["primary_key"],
    stored_as_scd_type=1
)

# COMMAND ----------

# DBTITLE 1,customer_cdc_snapshot_scd2

def exist(file_path):
    try:
        dbutils.fs.ls(file_path)
        return True
    except:
        return False

# Function to determine the next snapshot based on date
def next_snapshot_and_version(latest_snapshot_version):
    # If no previous snapshot exists, start with the first day (replace with start date)
    if latest_snapshot_version is None:
        latest_snapshot_date = date(2025, 3, 1)
    else:
        latest_snapshot_date = datetime.strptime(str(latest_snapshot_version), "%Y%m%d") + timedelta(days=1)

    # Create the path to the next snapshot
    next_version = latest_snapshot_date.strftime("%Y%m%d")
    
    file_path = f"{parquet_path}/Year={latest_snapshot_date.year}/Month={latest_snapshot_date.month}/Day={latest_snapshot_date.day}/"

    if exist(file_path):
        df = spark.read.option("basePath", file_path).parquet(f"{file_path}*").select("*", col('_metadata'))

        # Add the filename by extracting it from _metadata.file_path
        df = df.withColumn("file_name", col("_metadata").getField("file_name"))

        # Extract timestamp using regex
        df = df.withColumn("filename_timestamp_str", regexp_extract("file_name", r"(\d{14})", 1))
        df = df.withColumn("filename_timestamp", to_timestamp("filename_timestamp_str", "yyyyMMddHHmmss"))

        df = df.withColumn("dlt_load_timestamp", (current_timestamp()))
        
        # drop unnecessary columns
        df = df.drop("filename_timestamp_str")
        df = df.drop("file_name")
        
        return df, int(next_version)
    
    else:
        return None

dlt.create_streaming_live_table("snapshot_scd2")

dlt.apply_changes_from_snapshot(
    target="snapshot_scd2",
    source=next_snapshot_and_version,
    keys=["primary_key"],
    stored_as_scd_type=2,
    track_history_except_column_list = ["_metadata", "filename_timestamp", "dlt_load_timestamp"]
)
