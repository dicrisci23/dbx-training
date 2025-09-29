# Databricks notebook source
# MAGIC %md
# MAGIC # Process Snapshot Data
# MAGIC ## Change Data Capture (CDC) SCD

# COMMAND ----------

import dlt, re

from pyspark.sql.types import DateType, FloatType, StringType, IntegerType, StructType, StructField, LongType, TimestampType
from pyspark.sql.functions import col, to_date, regexp_replace, current_timestamp, regexp_extract, to_timestamp, date_format, concat, lit, max

from datetime import datetime, timedelta, date

# COMMAND ----------

default_catalog = spark.conf.get("catalog")
parquet_path = f"/Volumes/{default_catalog}/raw/files/Customers/snapshot_customer/"

# COMMAND ----------

# DBTITLE 1,customer_raw_files
view_name = "customer_raw_files"

@dlt.view(
    name = view_name,
    comment = 'The purpose of this view is to use Auto-Loader to read the data from the datalake.'
    )
def build_view():
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

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd1
cdc_table_scd1 = 'customer_cdc_scd1'

dlt.create_streaming_table(cdc_table_scd1)

dlt.apply_changes(
        target=cdc_table_scd1,
        source=view_name,
        keys=["primary_key"],
        sequence_by=col("filename_timestamp"),
        stored_as_scd_type = 1
    )

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd2
cdc_table_scd2 = 'customer_cdc_scd2'

dlt.create_streaming_table(cdc_table_scd2)

dlt.apply_changes(
        target=cdc_table_scd2,
        source=view_name,
        keys=["primary_key"],
        sequence_by=col("filename_timestamp"),
        stored_as_scd_type = 2,
        
        track_history_except_column_list = ["_metadata", "filename_timestamp", "dlt_load_timestamp"]
    )

