# Databricks notebook source
# MAGIC %md
# MAGIC # Import CSV

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

import re

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

file_location_csv   = f"/Volumes/demo/sample_data/files/csv/WineQuality.csv"

print(f"Your CSV file location: {file_location_csv}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2: Read the data
# MAGIC
# MAGIC Now that we have specified our file metadata, we can create a DataFrame. Notice that we use an *option* to specify that we want to infer the schema from the file. We can also explicitly set this to a particular schema if we have one already.
# MAGIC
# MAGIC First, let's create a DataFrame in Python.

# COMMAND ----------

df = spark.read.format("csv").load(file_location_csv)

# COMMAND ----------

display(df)

# COMMAND ----------

df3 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_location_csv)

# COMMAND ----------

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3: Query the data
# MAGIC
# MAGIC Now that we have created our DataFrame, we can query it. For instance, you can identify particular columns to select and display.

# COMMAND ----------

display(df3)

# COMMAND ----------

df3.count()

# COMMAND ----------

# MAGIC %md
# MAGIC May some transformations?

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df3 = (df3
      .withColumnRenamed("od280/od315_of_diluted_wines", "od280_od315_of_diluted_wines")
)

# COMMAND ----------

df3 = (df3
     .withColumn("acloholdoubled", col("alcohol")*2 )
     )

# COMMAND ----------

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 4: (Optional) Create a view
# MAGIC
# MAGIC If you want to query this data as a table, you can simply register it as a *view* or a table.

# COMMAND ----------

df3.createOrReplaceTempView("TempViewCSVSample")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We can query this view using Spark SQL. For instance, we can perform a simple aggregation. Notice how we can use `%sql` to query the view from SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM TempViewCSVSample LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the dataframe as table

# COMMAND ----------

df3.write.saveAsTable(f"{user_catalog_name}.temp.wine_quality_v2")

# COMMAND ----------

wine_v2 = spark.sql(f"SELECT * FROM {user_catalog_name}.temp.wine_quality_v2")
display(wine_v2)
