# Databricks notebook source
# MAGIC %md
# MAGIC # Compare Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ## ToDO
# MAGIC
# MAGIC Add option to exclude columns

# COMMAND ----------

from pyspark.testing import assertDataFrameEqual
from pyspark.errors import PySparkAssertionError

# COMMAND ----------

dbutils.widgets.text("source_schema", "")
dbutils.widgets.text("target_schema", "")
dbutils.widgets.text("entity", "")

# COMMAND ----------

source_dataset = f"{dbutils.widgets.get('source_schema')}.{dbutils.widgets.get('entity')}"
target_dataset = f"{dbutils.widgets.get('target_schema')}.{dbutils.widgets.get('entity')}"

print(f"Comparing:")
print(f"{source_dataset}")
print("to") 
print(f"{target_dataset}")

# COMMAND ----------

df_source = spark.table(source_dataset)

df_source = (df_source
             .drop('_rescued_data')
             .drop('FilePath')
             .drop('FileModificationTime')
             .drop('LoadTimestamp')
             )

df_target = spark.table(target_dataset)
df_target = (df_target
             .drop('_rescued_data')
             .drop('FilePath')
             .drop('FileModificationTime')
             .drop('LoadTimestamp')
             )

# COMMAND ----------

try:
    assertDataFrameEqual(df_source, df_target, includeDiffRows=True)

except PySparkAssertionError as e:
    # `e.data` here looks like:
    # [(Row(name='Alfred', amount=1200), Row(name='Alfred', amount=1500))]
    # spark.createDataFrame(e.data, schema=["Source", "Target"]).show() 
    display(e.data)
    raise e
