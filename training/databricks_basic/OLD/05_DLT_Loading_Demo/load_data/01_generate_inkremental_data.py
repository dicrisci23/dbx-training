# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Loading Demo: 
# MAGIC ## Generate Inkremental Data with Faker

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

import re
import uuid
import os
import sys
import pandas as pd
from pyspark.sql.functions import current_timestamp, sha2, concat, lit, col, udf
from pyspark.sql.types import StringType
from datetime import datetime, timedelta
from faker import Faker
import logging
fake = Faker()

# COMMAND ----------

# DBTITLE 1,set up logger
for h in logging.root.handlers[:]:
    logging.root.removeHandler(h)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

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

dbutils.widgets.text("first_snapshot_date", "2025-03-01", "Date, when the first snapshot was taken.") 

# COMMAND ----------

user_id = spark.sql('select current_user() as user').collect()[0]['user']
catalog_name = transform_email(user_id)

first_snapshot_date = datetime.strptime(dbutils.widgets.get("first_snapshot_date"), "%Y-%m-%d")

base_path = f"/Volumes/{catalog_name}/raw/files/Customers"
date_csv_path = f"{base_path}/delta_snapshot_dates.csv"

logger.info(f"catalog_name: {catalog_name}")
logger.info(f"first_snapshot_date: {first_snapshot_date}")
logger.info(f"base_path: {base_path}")
logger.info(f"date_csv_path: {date_csv_path}")

# COMMAND ----------

def update_snapshot_date(date_csv_path: str, first_snapshot_date: datetime) -> tuple:
    """
    Updates the snapshot date in the given CSV file.

    - If the file exists, it reads the latest snapshot date, increments it by one day, and appends it.
    - If the file does not exist, it initializes it with the first snapshot date.

    Parameters:
        date_csv_path (str): Path to the snapshot CSV file.
        first_snapshot_date (datetime): The initial snapshot date to use if the file does not exist.

    Returns:
        tuple: (datetime, int) - The newly added snapshot date and the row count (iteration).
    """

    # Check if the CSV file exists
    if os.path.exists(date_csv_path):
        # Load the existing CSV file
        existing_df = pd.read_csv(date_csv_path, dtype=str)  # Read as string to prevent automatic type conversion

        # Convert the column to datetime format
        existing_df["snapshot_date"] = pd.to_datetime(existing_df["snapshot_date"]).dt.date  # Keep only the date part

        # Get the latest snapshot date
        latest_snapshot_date = existing_df["snapshot_date"].max()

        # Increment the date by one day
        snapshot_timestamp = latest_snapshot_date + timedelta(days=1)

        # Append the new timestamp (ensure it's a string)
        new_entry = pd.DataFrame({"snapshot_date": [snapshot_timestamp.isoformat()]})
        updated_df = pd.concat([existing_df, new_entry], ignore_index=True)

        # Save the updated CSV file (force writing only date format)
        updated_df.to_csv(date_csv_path, index=False, date_format='%Y-%m-%d')

        # Get the row count (iteration)
        iteration = len(updated_df)

        print(f"New snapshot timestamp {snapshot_timestamp} saved. Iteration: {iteration}")
    else:
        # If the file does not exist, create it with the first snapshot date
        snapshot_timestamp = first_snapshot_date
        df = pd.DataFrame({"snapshot_date": [snapshot_timestamp.isoformat()]})  # Store as string
        df.to_csv(date_csv_path, index=False)

        # Since it's the first row, iteration = 1
        iteration = 1
        print(f"Snapshot timestamp {snapshot_timestamp} saved. Iteration: {iteration}")

    return snapshot_timestamp, iteration  # Return both values

# COMMAND ----------

dbutils.fs.mkdirs(base_path)

# COMMAND ----------

snapshot_timestamp, iteration = update_snapshot_date(date_csv_path, first_snapshot_date)

# COMMAND ----------

# DBTITLE 1,Create Customer Data
# Create a list of fake person data
data = []
for i in range(5):
    primary_key = str(uuid.uuid4()) # Generate a unique primary key
    name = fake.first_name()
    surname = fake.last_name()
    dob = fake.date_of_birth()
    country = fake.country()
    age = fake.random_int(min=18, max=99)

    data.append((primary_key, name, surname, dob, country, age))

# Create a DataFrame
df_customer_delta = spark.createDataFrame(data, ['primary_key', 'name', 'surname', 'date_of_birth', 'country', 'age'])

# Display the DataFrame
display(df_customer_delta)

# COMMAND ----------

# DBTITLE 1,save snapshot
# MAGIC %md
# MAGIC ## Save as parquet

# COMMAND ----------

# DBTITLE 1,save snapshot parquet
timestamp_string = snapshot_timestamp.strftime("%Y%m%d%H%M%S")
year    = snapshot_timestamp.year
month   = snapshot_timestamp.month
day     = snapshot_timestamp.day

output_path = os.path.join(
                            base_path, 
                            "delta_customer/",
                            f"Year={year}/Month={month}/Day={day}/"
                            )
file_name = f"customer_delta_{timestamp_string}.parquet"

os.makedirs(output_path, exist_ok=True)

pdf_customer_delta = df_customer_delta.toPandas()
pdf_customer_delta.to_parquet(f"{output_path}{file_name}")

# COMMAND ----------

# check the data
display(spark.read.parquet(output_path))

# COMMAND ----------


