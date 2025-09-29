# Databricks notebook source
# MAGIC %md
# MAGIC # Data Enrichment
# MAGIC Sample Notebook, how you can enrich ingested source data.

# COMMAND ----------

import re
from datetime import datetime
import pytz
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

schema_name = f"{user_catalog_name}.temp"
file_location   = "/Volumes/deads5/iron_shared/eventhub_capture/iot/*/*/*/*/*/*/*.avro"

print(f"Your Schema: {schema_name}")
print(f"Your file location: {file_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load raw files

# COMMAND ----------

df = (spark
      .read
      .format("avro")
      .load(file_location)
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## add System Metadata

# COMMAND ----------

# add meta data from files
# https://docs.databricks.com/en/ingestion/file-metadata-column.html

df = (spark
      .read
      .format("avro")
      .load(file_location)
      .select("*", "_metadata")
)
display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Loadtimestamp

# COMMAND ----------

# add load timestamp
load_timestamp = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
print(load_timestamp)

# COMMAND ----------

df = (spark
      .read
      .format("avro")
      .load(file_location)
      .select("*", "_metadata")
      .withColumn("load_timestamp", lit(load_timestamp))
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Load Id

# COMMAND ----------

load_id = "dummy_load_id"

# COMMAND ----------

df = (spark
      .read
      .format("avro")
      .load(file_location)
      .select("*", "_metadata")
      .withColumn("load_timestamp", lit(load_timestamp))
      .withColumn("load_id", lit(load_id))
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Source-System Id and Pipeline Name

# COMMAND ----------

source_system = "iot_eventhub_4536"
pipeline_name = "iot_ingestion"

# COMMAND ----------

df = (spark
      .read
      .format("avro")
      .load(file_location)
      .select("*", "_metadata")
      .withColumn("load_timestamp", lit(load_timestamp))
      .withColumn("load_id", lit(load_id))
      .withColumn("source_system", lit(source_system))
      .withColumn("pipeline_name", lit(pipeline_name))
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add User or Service Account which run the pipeline 

# COMMAND ----------

run_as = spark.sql('select current_user() as user').collect()[0]['user']
print(run_as)

# COMMAND ----------

df = (spark
      .read
      .format("avro")
      .load(file_location)
      .select("*", "_metadata")
      .withColumn("load_timestamp", lit(load_timestamp))
      .withColumn("load_id", lit(load_id))
      .withColumn("source_system", lit(source_system))
      .withColumn("pipeline_name", lit(pipeline_name))
      .withColumn("run_as", lit(run_as))
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## flatten Metadata, drop _metadata

# COMMAND ----------

df = (spark
      .read
      .format("avro")
      .load(file_location)
      .select("*", "_metadata")
      .withColumn("load_timestamp", lit(load_timestamp))
      .withColumn("load_id", lit(load_id))
      .withColumn("source_system", lit(source_system))
      .withColumn("pipeline_name", lit(pipeline_name))
      .withColumn("run_as", lit(run_as))
      .withColumn("meta_file_path", col("_metadata.file_path"))
      .withColumn("meta_file_name", col("_metadata.file_name"))
      .withColumn("meta_file_size", col("_metadata.file_size"))
      .withColumn("meta_file_block_start", col("_metadata.file_block_start"))
      .withColumn("meta_file_block_length", col("_metadata.file_block_length"))
      .withColumn("meta_file_modification_time", col("_metadata.file_modification_time"))
      .drop("_metadata")
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## decrypt body from payload

# COMMAND ----------

# Define the schema of the JSON string in the Body
json_schema = (
    StructType()
    .add("deviceid", "integer")
    .add("temp", "double")
    .add("timestamp", "timestamp")
    .add("sender", "string")
)

# COMMAND ----------

df = (spark
      .read
      .format("avro")
      .load(file_location)
      .select("*", "_metadata")
      .withColumn("load_timestamp", lit(load_timestamp))
      .withColumn("load_id", lit(load_id))
      .withColumn("source_system", lit(source_system))
      .withColumn("pipeline_name", lit(pipeline_name))
      .withColumn("run_as", lit(run_as))
      .withColumn("meta_file_path", col("_metadata.file_path"))
      .withColumn("meta_file_name", col("_metadata.file_name"))
      .withColumn("meta_file_size", col("_metadata.file_size"))
      .withColumn("meta_file_block_start", col("_metadata.file_block_start"))
      .withColumn("meta_file_block_length", col("_metadata.file_block_length"))
      .withColumn("meta_file_modification_time", col("_metadata.file_modification_time"))
      .drop("_metadata")
      .withColumn("body_string", col("body").cast("string"))
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add a Hashkey

# COMMAND ----------

df = (spark
      .read
      .format("avro")
      .load(file_location)
      .select("*", "_metadata")
      .withColumn("load_timestamp", lit(load_timestamp))
      .withColumn("load_id", lit(load_id))
      .withColumn("source_system", lit(source_system))
      .withColumn("pipeline_name", lit(pipeline_name))
      .withColumn("run_as", lit(run_as))
      .withColumn("meta_file_path", col("_metadata.file_path"))
      .withColumn("meta_file_name", col("_metadata.file_name"))
      .withColumn("meta_file_size", col("_metadata.file_size"))
      .withColumn("meta_file_block_start", col("_metadata.file_block_start"))
      .withColumn("meta_file_block_length", col("_metadata.file_block_length"))
      .withColumn("meta_file_modification_time", col("_metadata.file_modification_time"))
      .drop("_metadata")
      .withColumn("body_string", col("body").cast("string"))
      .withColumn("hash_key", sha2(concat_ws("||", col("Body"), col("body_string")), 256))
)
display(df)

# COMMAND ----------


