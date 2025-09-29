-- This file defines a sample transformation.
-- Edit the sample below or add new transformations
-- using "+ Add" in the file browser.

CREATE MATERIALIZED VIEW altyca_demo.monitoring.table_lineage AS
WITH latest_events AS (
  SELECT
    target_table_full_name,
    MAX(event_date) AS max_event_date
  FROM system.access.table_lineage
  -- WHERE target_table_full_name LIKE 'pilatus_hub_dev.%'
  GROUP BY target_table_full_name
),

latest_lineage AS (
  SELECT DISTINCT
    l.source_table_full_name,
    l.target_table_full_name
  FROM system.access.table_lineage l
  JOIN latest_events e
    ON l.target_table_full_name = e.target_table_full_name
   AND l.event_date = e.max_event_date
  WHERE l.source_table_full_name IS NOT NULL
    AND l.source_table_full_name != l.target_table_full_name
    AND NOT split(l.source_table_full_name, '\\.')[2] LIKE '__materialization_%'
),

stage_0 AS (
  SELECT DISTINCT target_table_full_name AS stage_0 FROM latest_lineage
),

stage_1 AS (
  SELECT l.source_table_full_name AS stage_1, s0.stage_0
  FROM latest_lineage l
  JOIN stage_0 s0 ON l.target_table_full_name = s0.stage_0
),

stage_2 AS (
  SELECT l.source_table_full_name AS stage_2, s1.stage_1, s1.stage_0
  FROM latest_lineage l
  JOIN stage_1 s1 ON l.target_table_full_name = s1.stage_1
)

SELECT 
  stage_2,
  stage_1,
  stage_0,

  -- Ziel-Infos aus stage_0
  split(stage_0, '\\.')[0] AS 0_catalog,
  split(stage_0, '\\.')[1] AS 0_schema,
  split(stage_0, '\\.')[2] AS 0_table,

  split(stage_1, '\\.')[0] AS 1_catalog,
  split(stage_1, '\\.')[1] AS 1_schema,
  split(stage_1, '\\.')[2] AS 1_table,

  split(stage_2, '\\.')[0] AS 2_catalog,
  split(stage_2, '\\.')[1] AS 2_schema,
  split(stage_2, '\\.')[2] AS 2_table

FROM stage_2
