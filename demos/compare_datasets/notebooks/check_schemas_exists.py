# Databricks notebook source
# MAGIC %md
# MAGIC # check if schemas exists
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Create widgets for catalog selection
dbutils.widgets.text("catalog", "main", "Select Catalog")
catalog = dbutils.widgets.get("catalog")
schemas = ['schema1', 'schema2', 'schema3', 'schema4']

# COMMAND ----------

# Check if schemas in the selected catalog exist

df = spark.sql(f"SHOW SCHEMAS IN {catalog}")
existing_schemas = [row['databaseName'] for row in df.collect()]

schemas_exists = all(schema in existing_schemas for schema in schemas)
display(schemas_exists)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="schemas_exists", value=schemas_exists)
