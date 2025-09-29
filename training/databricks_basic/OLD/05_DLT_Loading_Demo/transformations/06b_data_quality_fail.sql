-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Quality with expectations #2 FAIL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Quality 
-- MAGIC Manage data quality with pipeline expectations
-- MAGIC https://docs.databricks.com/aws/en/dlt/expectations
-- MAGIC
-- MAGIC ![](https://docs.databricks.com/aws/en/assets/images/expectations-flow-graph-02ab5dd2011b18ad791c67c0e8449af6.png)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver.customers_dq4
(
  CONSTRAINT valid_customer_age EXPECT (age BETWEEN 200 AND 300) ON VIOLATION FAIL UPDATE
)
AS
SELECT
  primary_key,
  name,
  surname,
  date_of_birth,
  country
  age
FROM
  STREAM (bronze.delta_customer)
