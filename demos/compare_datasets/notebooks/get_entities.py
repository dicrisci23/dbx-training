# Databricks notebook source
# MAGIC %md
# MAGIC # get Entities

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Select Catalog")
catalog = dbutils.widgets.get("catalog")
schema = 'schema1'

# COMMAND ----------

df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")

display(df)

# COMMAND ----------

entities = [row['tableName'] for row in df.select('tableName').collect()]
display(entities)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="entities", value=entities)

# COMMAND ----------


