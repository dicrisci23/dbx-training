-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Quality with expectations #3 Extended

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## create a silver table for customers

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver.customers_dq AS SELECT * FROM bronze.delta_customer

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Data Quality 
-- MAGIC Manage data quality with pipeline expectations
-- MAGIC https://docs.databricks.com/aws/en/dlt/expectations
-- MAGIC
-- MAGIC ![](https://docs.databricks.com/aws/en/assets/images/expectations-flow-graph-02ab5dd2011b18ad791c67c0e8449af6.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Row count validation
-- MAGIC https://docs.databricks.com/aws/en/dlt/expectation-patterns?language=SQL#row-count-validation
-- MAGIC
-- MAGIC ![](https://docs.databricks.com/aws/en/assets/images/count-validation-graph-fb977edf9d864a1d497bf039df88823a.png)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver.customer_count_verification(
  CONSTRAINT no_rows_dropped EXPECT (source_count == silver_count)
) AS SELECT * FROM
  (SELECT COUNT(*) AS source_count FROM source.customer),
  (SELECT COUNT(*) AS silver_count FROM silver.customers_dq)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Missing record detection
-- MAGIC https://docs.databricks.com/aws/en/dlt/expectation-patterns?language=SQL#missing-record-detection
-- MAGIC
-- MAGIC ![](https://docs.databricks.com/aws/en/assets/images/left-join-data-completeness-graph-c035bb25e35a3429015781e8be1d381b.png)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW customer_compare_tests(
  CONSTRAINT no_missing_records EXPECT (silver_key IS NOT NULL)
)
AS SELECT source.*, silver.primary_key as silver_key FROM source.customer AS source
  LEFT OUTER JOIN silver.customers_dq AS silver ON silver.primary_key = source.primary_key

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Primary key uniqueness
-- MAGIC https://docs.databricks.com/aws/en/dlt/expectation-patterns?language=SQL#primary-key-uniqueness
-- MAGIC
-- MAGIC ![](https://docs.databricks.com/aws/en/assets/images/pk-validation-graph-abadaefb70a4c36ac36f557bd145f37a.png)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW customer_pk_tests(
  CONSTRAINT unique_pk EXPECT (num_entries = 1)
)
AS SELECT primary_key, count(*) as num_entries
  FROM silver.customers_dq
  GROUP BY primary_key

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Quarantine invalid records
-- MAGIC
-- MAGIC ![](https://docs.databricks.com/aws/en/assets/images/quarantine-flow-graph-76ab9bb57b6c26c3c5334b4193051d0f.png)

-- COMMAND ----------

CREATE TEMPORARY STREAMING LIVE VIEW raw_trips_data AS
  SELECT * FROM STREAM(samples.nyctaxi.trips);

CREATE OR REFRESH TEMPORARY STREAMING TABLE trips_data_quarantine(
  -- Option 1 - merge all expectations to have a single name in the pipeline event log
  CONSTRAINT quarantined_row EXPECT (pickup_zip IS NOT NULL OR dropoff_zip IS NOT NULL),
  -- Option 2 - Keep the expectations separate, resulting in multiple entries under different names
  CONSTRAINT invalid_pickup_zip EXPECT (pickup_zip IS NOT NULL),
  CONSTRAINT invalid_dropoff_zip EXPECT (dropoff_zip IS NOT NULL)
)
PARTITIONED BY (is_quarantined)
AS
  SELECT
    *,
    NOT ((pickup_zip IS NOT NULL) and (dropoff_zip IS NOT NULL)) as is_quarantined
  FROM STREAM(raw_trips_data);

CREATE TEMPORARY LIVE VIEW valid_trips_data AS
SELECT * FROM trips_data_quarantine WHERE is_quarantined=FALSE;

CREATE TEMPORARY LIVE VIEW invalid_trips_data AS
SELECT * FROM trips_data_quarantine WHERE is_quarantined=TRUE;
