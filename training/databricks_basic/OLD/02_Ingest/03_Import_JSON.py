# Databricks notebook source
# MAGIC %md
# MAGIC # Import JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation
# MAGIC Upload the json-File countries.json

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1: Set the data location and type

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

schema_name = f"{user_catalog_name}.temp"
file_location_json   = "/Volumes/demo/sample_data/files/json/countries.json"

print(f"Your Schema: {schema_name}")
print(f"Your CSV file location: {file_location_json}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2: Read the data

# COMMAND ----------

df = spark.read.option("multiline", "true").json(file_location_json)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What about the nested columns?
# MAGIC

# COMMAND ----------

df.createOrReplaceTempView("vwCountries")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vwCountries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC name,  
# MAGIC timezones[0]['abbreviation'] AS timezones_abbreviation,
# MAGIC timezones[0]['gmtOffset'] AS timezones_gmtOffset,
# MAGIC timezones[0]['gmtOffsetName'] AS timezones_gmtOffsetName,
# MAGIC timezones[0]['tzName'] AS timezones_tzName,
# MAGIC timezones[0]['zoneName'] AS timezones_zoneName,
# MAGIC timezones[0],
# MAGIC timezones
# MAGIC FROM vwCountries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why not automate it?

# COMMAND ----------

from pyspark.sql import types as T
import pyspark.sql.functions as F

def flatten(df):
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])
    
    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode(col_name))
    
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])
        
        
    for df_col_name in df.columns:
        df = df.withColumnRenamed(df_col_name, df_col_name.replace(qualify, ""))

    return df

# COMMAND ----------

df_flat = flatten(df)

# COMMAND ----------

display(df_flat)

# COMMAND ----------


