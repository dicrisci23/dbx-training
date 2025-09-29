-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW altyca_demo.monitoring.column_lineage AS
WITH latest_events AS (
  SELECT
    target_table_full_name,
    MAX(event_date) AS max_event_date
  FROM system.access.column_lineage
  GROUP BY target_table_full_name
),

latest_lineage AS (
  SELECT DISTINCT
    l.source_table_full_name,
    l.source_column_name,
    l.target_table_full_name,
    l.target_column_name
  FROM system.access.column_lineage l
  JOIN latest_events e
    ON l.target_table_full_name = e.target_table_full_name
   AND l.event_date = e.max_event_date
  WHERE l.source_column_name IS NOT NULL
    AND l.target_column_name IS NOT NULL
    AND l.source_table_full_name != l.target_table_full_name
    AND NOT split(l.source_table_full_name, '\\.')[2] LIKE '__materialization_%'
),

stage_0 AS (
  SELECT DISTINCT 
    target_table_full_name AS stage_0_table,
    target_column_name     AS stage_0_column
  FROM latest_lineage
),

stage_1 AS (
  SELECT 
    l.source_table_full_name AS stage_1_table,
    l.source_column_name     AS stage_1_column,
    s0.stage_0_table,
    s0.stage_0_column
  FROM latest_lineage l
  JOIN stage_0 s0
    ON l.target_table_full_name = s0.stage_0_table
   AND l.target_column_name     = s0.stage_0_column
),

stage_2 AS (
  SELECT 
    l.source_table_full_name AS stage_2_table,
    l.source_column_name     AS stage_2_column,
    s1.stage_1_table,
    s1.stage_1_column,
    s1.stage_0_table,
    s1.stage_0_column
  FROM latest_lineage l
  JOIN stage_1 s1
    ON l.target_table_full_name = s1.stage_1_table
   AND l.target_column_name     = s1.stage_1_column
)

SELECT 
  -- Columns per Stage
  stage_2_table,
  stage_2_column,
  stage_1_table,
  stage_1_column,
  stage_0_table,
  stage_0_column,

  -- Catalog, Schema, Table from stages
  split(stage_0_table, '\\.')[0] AS 0_catalog,
  split(stage_0_table, '\\.')[1] AS 0_schema,
  split(stage_0_table, '\\.')[2] AS 0_table,

  split(stage_1_table, '\\.')[0] AS 1_catalog,
  split(stage_1_table, '\\.')[1] AS 1_schema,
  split(stage_1_table, '\\.')[2] AS 1_table,

  split(stage_2_table, '\\.')[0] AS 2_catalog,
  split(stage_2_table, '\\.')[1] AS 2_schema,
  split(stage_2_table, '\\.')[2] AS 2_table

FROM stage_2