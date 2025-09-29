-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 01 DLT 1. Simple SQL

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze.nyctaxi_trips1 AS
SELECT
  *
FROM
  STREAM (samples.nyctaxi.trips)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW bronze.nyctaxi_trips2
AS
SELECT
*
FROM samples.nyctaxi.trips
