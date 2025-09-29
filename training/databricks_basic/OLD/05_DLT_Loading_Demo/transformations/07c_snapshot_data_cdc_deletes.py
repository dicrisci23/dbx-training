# Databricks notebook source
# MAGIC %md
# MAGIC # Process Snapshot Data
# MAGIC ## Change Data Capture (CDC) with deletes

# COMMAND ----------

import dlt, re

from pyspark.sql.types import DateType, FloatType, StringType, IntegerType, StructType, StructField, LongType, TimestampType
from pyspark.sql.functions import col, to_date, regexp_replace, current_timestamp, regexp_extract, to_timestamp, date_format, concat, lit, max

from datetime import datetime, timedelta, date

# COMMAND ----------

default_catalog = spark.conf.get("catalog")
parquet_path = f"/Volumes/{default_catalog}/raw/files/Customers/snapshot_customer/"

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd2 deletes
cdc_scd2_deletes = "cdc_scd2_deletes"

@dlt.table(
  name=cdc_scd2_deletes)

def filter_deletes():
    return spark.read.table("bronze.customer_cdc_scd2").alias("c").where(
        f"c.__END_AT IS NULL AND c.__START_AT != (SELECT MAX(__START_AT) FROM {default_catalog}.bronze.customer_cdc_scd2)"
    )

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd2 active
cdc_view_active = "cdc_scd2_active"

@dlt.table(
  name=cdc_view_active
)
def filter_active():
    return spark.read.table("bronze.customer_cdc_scd2").alias("c").where(
        f"c.__END_AT IS NULL AND c.__START_AT = (SELECT MAX(__START_AT) FROM {default_catalog}.bronze.customer_cdc_scd2)"
    )

# COMMAND ----------

# DBTITLE 1,customer_cdc_scd2 final
cdc_view_final = "cdc_scd2_final"

@dlt.table(
  name=cdc_view_final
)


def filter_active_and_deletes():

    df_active = dlt.read(cdc_view_active)

    df_deleted  = dlt.read(cdc_scd2_deletes).select("primary_key")

    df_final = df_active.join(df_deleted, on="primary_key", how="left_anti")

    return df_final
