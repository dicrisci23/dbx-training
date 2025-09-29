# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Loading Demo: 
# MAGIC ## Generate Snapshot Data with Faker

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
date_csv_path = f"{base_path}/customer_snapshot_dates.csv"

customer_source_table = f"{catalog_name}.source.customer"

logger.info(f"catalog_name: {catalog_name}")
logger.info(f"first_snapshot_date: {first_snapshot_date}")
logger.info(f"base_path: {base_path}")
logger.info(f"date_csv_path: {date_csv_path}")
logger.info(f"customer_source_table: {customer_source_table}")


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

        logger.info(f"New snapshot timestamp {snapshot_timestamp} saved. Iteration: {iteration}")
    else:
        # If the file does not exist, create it with the first snapshot date
        snapshot_timestamp = first_snapshot_date
        df = pd.DataFrame({"snapshot_date": [snapshot_timestamp.isoformat()]})  # Store as string
        df.to_csv(date_csv_path, index=False)

        # Since it's the first row, iteration = 1
        iteration = 1
        logger.info(f"Snapshot timestamp {snapshot_timestamp} saved. Iteration: {iteration}")

        # also delete the source_customer table, if it already exists
        sql = f"DROP TABLE IF EXISTS {customer_source_table}"
        print(sql)
        spark.sql(sql).display()

    return snapshot_timestamp, iteration  # Return both values

# COMMAND ----------

snapshot_timestamp, iteration = update_snapshot_date(date_csv_path, first_snapshot_date)
logger.info(f"Snapshot timestamp: {snapshot_timestamp}")
logger.info(f"Iteration: {iteration}")

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

# DBTITLE 1,append to delta source table
# Append the DataFrame to the Delta Lake table
df_customer_delta.write.mode("append").saveAsTable(f"{customer_source_table}")


# COMMAND ----------

# Display the table
display(spark.sql(f"SELECT * FROM {customer_source_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add sample Records, for the delete & updates

# COMMAND ----------

# DBTITLE 1,insert 5 values, only if row-count is 5
if iteration == 1:
    logger.info("Insert additional data")
    spark.sql(f"""INSERT INTO {customer_source_table} VALUES
        ('PK1', 'Update1', 'Update1', '1990-05-15', 'Switzerland', 34),
        ('PK2', 'Delete1', 'Delete1', '1985-08-20', 'Switzerland', 38),
        ('PK3', 'Update2', 'Update2', '1992-11-30', 'Switzerland', 31),
        ('PK4', 'Delete2', 'Delete2', '1995-07-25', 'Switzerland', 28),
        ('PK5', 'Update3', 'Update3', '1980-03-10', 'Switzerland', 44);
        """)
else: 
    logger.info("No additional data to insert.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update #1

# COMMAND ----------

if iteration == 2:
    logger.info("Will do Update #1")
    spark.sql(f"""
    UPDATE {customer_source_table} 
    SET name = 'UPDATED', surname = 'UPDATED'
    WHERE primary_key = 'PK1';
    """)
else:
    logger.info("Nothing to do")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete #1

# COMMAND ----------

if iteration == 3:
    logger.info("Will do Delete #1")
    spark.sql(f"""
    DELETE FROM {customer_source_table} 
    WHERE primary_key = 'PK2';
    """)
else:
    logger.info("Nothing to do")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update #2

# COMMAND ----------

if iteration == 4:
    logger.info("Will do Update #2")
    spark.sql(f"""
    UPDATE {customer_source_table} 
    SET name = 'UPDATED', surname = 'UPDATED'
    WHERE primary_key = 'PK3';
    """)
else:
    logger.info("Nothing to do")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delete #2

# COMMAND ----------

if iteration == 5:
    logger.info("Will do Delete #2")
    spark.sql(f"""
    DELETE FROM {customer_source_table} 
    WHERE primary_key = 'PK4';
    """)
else:
    logger.info("Nothing to do")

# COMMAND ----------

# DBTITLE 1,save snapshot
# MAGIC %md
# MAGIC ## Save snapshot

# COMMAND ----------

df_customer_snapshot = spark.read.table(f"{customer_source_table}")
display(df_customer_snapshot)

# COMMAND ----------

# DBTITLE 1,save snapshot parquet
timestamp_string = snapshot_timestamp.strftime("%Y%m%d%H%M%S")
year    = snapshot_timestamp.year
month   = snapshot_timestamp.month
day     = snapshot_timestamp.day

output_path = os.path.join(
                            base_path, 
                            "snapshot_customer/",
                            f"Year={year}/Month={month}/Day={day}/"
                            )
file_name = f"customer_snapshot_{timestamp_string}.parquet"

os.makedirs(output_path, exist_ok=True)

pdf_customer_snapshot = df_customer_snapshot.toPandas()
pdf_customer_snapshot.to_parquet(f"{output_path}{file_name}")

# COMMAND ----------

# check the data
display(spark.read.parquet(output_path))
