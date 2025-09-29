-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DLT Loading Demo Queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SELECT *

-- COMMAND ----------

SELECT * FROM demo.source.customer ORDER BY primary_key DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## get deleted records
-- MAGIC If we do a full-refresh, we loose the load-timestamp, which means, we loose the ability to detect deletes, but....
-- MAGIC
-- MAGIC we could do another extraction and load new files, then we can do a compare, which record got deleted.

-- COMMAND ----------

SELECT * FROM stefan_koch.bronze.customer_scd2 
WHERE __END_AT IS NULL

-- where load_timestamp is not the oldest
AND load_timestamp != (SELECT MAX(load_timestamp) FROM sandbox.stefan_bronze.customer_scd2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## get only actual valid records

-- COMMAND ----------

SELECT primary_key, name, surname, `__START_AT`, `__END_AT`, * FROM stefan_koch.bronze.customer_cdc_scd2
WHERE 1=1
-- AND primary_key IN ('PK2', 'PK4')

-- COMMAND ----------

SELECT primary_key, name, surname, `__START_AT`, `__END_AT`, *  FROM demo.bronze.customer_cdc_scd2
WHERE 1=1
AND __END_AT IS NULL
AND __START_AT != (SELECT MAX(__START_AT) FROM demo.bronze.customer_cdc_scd2)

-- COMMAND ----------

SELECT primary_key, name, surname, `__START_AT`, `__END_AT`, *  FROM demo.bronze.customer_cdc_scd2

-- COMMAND ----------

SELECT * FROM demo.bronze.cdc_scd2_deletes

-- COMMAND ----------

SELECT primary_key, name, surname, `__START_AT`, `__END_AT`, * FROM demo.bronze.cdc_scd2_active

-- COMMAND ----------

SELECT primary_key, name, surname, `__START_AT`, `__END_AT`, * FROM demo.bronze.cdc_scd2_final
WHERE primary_key IN ('PK2', 'PK4')

-- COMMAND ----------

SELECT * FROM demo.bronze.snapshot_scd1

-- COMMAND ----------


