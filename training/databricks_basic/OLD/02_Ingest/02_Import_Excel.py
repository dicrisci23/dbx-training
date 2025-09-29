# Databricks notebook source
# MAGIC %md
# MAGIC # Import Excel
# MAGIC
# MAGIC ## Links
# MAGIC Pandas Docu for Excel: https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparation

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

import re
import pandas as pd

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

file_location_xls   = "/Volumes/demo/sample_data/files/xlsx/FinancialsSampleData.xlsx"

print(f"Your Schema: {schema_name}")
print(f"Your Excel file location: {file_location_xls}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data into Pandas Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC If you want to read only a specific sheet of an Excel file, you can do this by naming the sheet explicitly in the options

# COMMAND ----------

pdf_sheet1 = pd.read_excel(file_location_xls, sheet_name="Financials1")

# COMMAND ----------

display(pdf_sheet1)

# COMMAND ----------

pdf_sheet2 = pd.read_excel(file_location_xls, sheet_name="Financials2")

# COMMAND ----------

display(pdf_sheet2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform the Pandas Dataframe to a Pyspark Dataframe

# COMMAND ----------

df_sheet1 = spark.createDataFrame(pdf_sheet1)
df_sheet2 = spark.createDataFrame(pdf_sheet2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert Columnames, remove spaces

# COMMAND ----------

# Get the current column names
current_columns1 = df_sheet1.columns

# Generate a new list of column names with spaces replaced by underscores
new_columns1 = [col.replace(" ", "_") for col in current_columns1]

# Rename the columns in the DataFrame
df_sheet1 = df_sheet1.select([df_sheet1[col].alias(new_col) for col, new_col in zip(df_sheet1.columns, new_columns1)])

# COMMAND ----------

# Get the current column names
current_columns2 = df_sheet2.columns

# Generate a new list of column names with spaces replaced by underscores
new_columns2 = [col.replace(" ", "_") for col in current_columns2]

# Rename the columns in the DataFrame
df_sheet2 = df_sheet2.select([df_sheet2[col].alias(new_col) for col, new_col in zip(df_sheet2.columns, new_columns2)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Dataframe as Unity Catalog table
# MAGIC Save sheet1 and sheet2 as table in the temp-schema, like in the example with CSV

# COMMAND ----------

df_sheet1.write.saveAsTable(f"{schema_name}.df_sheet1")
df_sheet2.write.saveAsTable(f"{schema_name}.df_sheet2")

# COMMAND ----------

sh1 = spark.sql(f"SELECT * FROM {schema_name}.df_sheet1")
display(sh1)

# COMMAND ----------

sh2 = spark.sql(f"SELECT * FROM {schema_name}.df_sheet2")
display(sh2)
