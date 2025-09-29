# Databricks notebook source
# MAGIC %md
# MAGIC # Create test data

# COMMAND ----------

from pyspark.sql import Row
from datetime import date, datetime

# COMMAND ----------

# Create widgets for catalog selection
dbutils.widgets.text("catalog", "main", "Select Catalog")
dbutils.widgets.text("clean_up", "false", "Delete Schemas?")
catalog = dbutils.widgets.get("catalog")
clean_up = dbutils.widgets.get("clean_up")


# COMMAND ----------

if clean_up == "true":
    # Drop tables in schema1
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema1.customers").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema1.orders").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema1.products").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema1.order_items").display()

    # Drop tables in schema2
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema2.customers").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema2.orders").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema2.products").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema2.order_items").display()

    # Drop tables in schema3
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema3.customers").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema3.orders").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema3.products").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema3.order_items").display()

    # Drop tables in schema4
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema4.customers").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema4.orders").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema4.products").display()
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema4.order_items").display()

    # Drop schemas
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.schema1 CASCADE").display()
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.schema2 CASCADE").display()
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.schema3 CASCADE").display()
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.schema4 CASCADE").display()

    dbutils.notebook.exit("Deleted test schemas and tables.")

# COMMAND ----------

# Create schemas
for schema in ["schema1", "schema2", "schema3", "schema4"]:
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,create 4 dataframes

# Generate sample data (10 rows)
customers_data = [Row(customer_id=i, customer_name=f"Customer {i}", customer_email=f"customer{i}@example.com", signup_date=date(2023, 1, i % 28 + 1)) for i in range(1, 11)]
orders_data = [Row(order_id=i, customer_id=i, order_date=date(2023, 2, i % 28 + 1), total_amount=100.00 + i) for i in range(1, 11)]
products_data = [Row(product_id=i, product_name=f"Product {i}", category="Category A", price=10.00 + i) for i in range(1, 11)]
order_items_data = [Row(order_item_id=i, order_id=i, product_id=i, quantity=1, item_price=10.00 + i) for i in range(1, 11)]

customers_df = spark.createDataFrame(customers_data)
orders_df = spark.createDataFrame(orders_data)
products_df = spark.createDataFrame(products_data)
order_items_df = spark.createDataFrame(order_items_data)

display(customers_df)
display(orders_df)
display(products_df)
display(order_items_df)

# COMMAND ----------

# Insert data into schema1 and schema2 (identical)
for schema in ["schema1", "schema2"]:
  customers_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.customers")
  orders_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.orders")
  products_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.products")
  order_items_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.order_items")

# COMMAND ----------

# Modify customers_df: remove 1 row
customers_df_schema3 = customers_df.limit(customers_df.count() - 1)

# Modify products_df: add an additional row
additional_product = [Row(product_id=products_df.count() + 1, product_name=f"Product {products_df.count() + 1}", category="Category A", price=10.00 + products_df.count() + 1)]
products_df_schema3 = products_df.union(spark.createDataFrame(additional_product))

# Insert modified data into schema3
customers_df_schema3.write.mode("overwrite").saveAsTable(f"{catalog}.schema3.customers")
orders_df.write.mode("overwrite").saveAsTable(f"{catalog}.schema3.orders")
products_df_schema3.write.mode("overwrite").saveAsTable(f"{catalog}.schema3.products")
order_items_df.write.mode("overwrite").saveAsTable(f"{catalog}.schema3.order_items")

# COMMAND ----------

from pyspark.sql.functions import col, lit

# Read original dataframes from schema1
customers_df_schema1 = spark.table(f"{catalog}.schema1.customers")
orders_df_schema1 = spark.table(f"{catalog}.schema1.orders")
products_df_schema1 = spark.table(f"{catalog}.schema1.products")
order_items_df_schema1 = spark.table(f"{catalog}.schema1.order_items")

# Change the data type of product_id to BigINT and signup_date to TIMESTAMP
customers_df_modified = customers_df_schema1.withColumn("signup_date", col("signup_date").cast("timestamp"))
products_df_modified = products_df_schema1.withColumn("product_id", col("product_id").cast("bigint"))
order_items_df_modified = order_items_df_schema1.withColumn("product_id", col("product_id").cast("bigint"))

# Insert modified data into schema1
customers_df_modified.write.mode("overwrite").saveAsTable(f"{catalog}.schema4.customers")
orders_df_schema1.write.mode("overwrite").saveAsTable(f"{catalog}.schema4.orders")
products_df_modified.write.mode("overwrite").saveAsTable(f"{catalog}.schema4.products")
order_items_df_modified.write.mode("overwrite").saveAsTable(f"{catalog}.schema4.order_items")

# COMMAND ----------

# Exit point to stop execution of subsequent cells
dbutils.notebook.exit("Created test schemas and tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup the setup

# COMMAND ----------

# Drop tables in schema1
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema1.customers").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema1.orders").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema1.products").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema1.order_items").display()

# Drop tables in schema2
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema2.customers").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema2.orders").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema2.products").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema2.order_items").display()

# Drop tables in schema3
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema3.customers").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema3.orders").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema3.products").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema3.order_items").display()

# Drop tables in schema4
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema4.customers").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema4.orders").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema4.products").display()
spark.sql(f"DROP TABLE IF EXISTS {catalog}.schema4.order_items").display()

# Drop schemas
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.schema1 CASCADE").display()
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.schema2 CASCADE").display()
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.schema3 CASCADE").display()
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.schema4 CASCADE").display()

