-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks System Tables Queries

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This Notebooks contains Sample Queries for Databricks System Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Is my team using Databricks foundation models?
-- MAGIC Tracking usage by SKU is essential for understanding which product features are being utilized. While many customers may be less concerned about traditional products like jobs, all-purpose, or SQL DBUs, there is significant interest in monitoring the usage of new products. For instance, to monitor the usage of foundation models in Databricks, an admin can run the following query:

-- COMMAND ----------

select *
from system.billing.usage
where contains(lower(sku_name), 'inference') 
      and product_features.serving_type = 'FOUNDATION_MODEL'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Who deleted my job or job run?
-- MAGIC Auditing who deleted an object in Databricks is crucial, especially since the UI does not display this information, leaving you often at a loss. If you find yourself wondering what happened to your job, run the following query:

-- COMMAND ----------

select a.user_identity.email as deleter
, a.request_params.run_id
, a.event_time
, a.*

from system.access.audit a

where lower(a.service_name) = 'jobs'
  and a.action_name = 'deleteRun'
  and a.request_params.run_id in (
    select run_id
    from system.lakeflow.job_run_timeline
    where job_id = '<job_id>'
  )

order by a.event_time desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## How can I see the schedule of all my jobs?
-- MAGIC This can be a little tricky, as all jobs are defined using CRON and can be changed at any time. To do so, we will use the audit table to see all changes or updates to the cron schedule and parse the schedule into a human readable format. This requires some python. Check out the following.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC
-- MAGIC df = spark.sql(
-- MAGIC     """
-- MAGIC   with jobs_w_schedules as (
-- MAGIC     select request_params.job_id
-- MAGIC       , workspace_id
-- MAGIC       , event_time
-- MAGIC       , user_identity.email
-- MAGIC       , get_json_object(request_params.new_settings, '$.schedule') as cron_schedule
-- MAGIC       , get_json_object(request_params.new_settings, '$.schedule.quartz_cron_expression') as quartz_cron_expression
-- MAGIC       , get_json_object(request_params.new_settings, '$.schedule.timezone_id') as timezone_id
-- MAGIC       , get_json_object(request_params.new_settings, '$.schedule.pause_status') as pause_status
-- MAGIC       , get_json_object(request_params.new_settings, "$.name") as job_name
-- MAGIC       , ROW_NUMBER() OVER (PARTITION BY request_params.job_id, workspace_id ORDER BY event_time DESC) AS rn
-- MAGIC
-- MAGIC
-- MAGIC     from system.access.audit
-- MAGIC
-- MAGIC     where service_name = 'jobs' and action_name in ('create', 'update')
-- MAGIC   ) 
-- MAGIC
-- MAGIC   select job_id
-- MAGIC       , workspace_id
-- MAGIC       , event_time
-- MAGIC       , email
-- MAGIC       , cron_schedule
-- MAGIC       , quartz_cron_expression
-- MAGIC       , timezone_id
-- MAGIC       , pause_status
-- MAGIC       , job_name
-- MAGIC   from jobs_w_schedules
-- MAGIC   where cron_schedule is not null
-- MAGIC   and rn = 1
-- MAGIC """
-- MAGIC )
-- MAGIC
-- MAGIC # UDF to convert cron expression to human-readable format
-- MAGIC def cron_to_human_readable(cron_expr):
-- MAGIC     parts = cron_expr.split()
-- MAGIC     
-- MAGIC     second_part = parts[0]
-- MAGIC     minute_part = parts[1]
-- MAGIC     hour_part = parts[2]
-- MAGIC     day_of_month_part = parts[3]
-- MAGIC     month_part = parts[4]
-- MAGIC     day_of_week_part = parts[5]
-- MAGIC     
-- MAGIC     human_readable = f"At {hour_part}:{minute_part}:{second_part}"
-- MAGIC
-- MAGIC     if day_of_month_part == "*" and day_of_week_part == "?":
-- MAGIC         human_readable += " every day"
-- MAGIC     elif day_of_month_part == "*":
-- MAGIC         human_readable += f" on every {day_of_week_part}"
-- MAGIC     elif day_of_week_part == "?":
-- MAGIC         human_readable += f" on day {day_of_month_part} of the month"
-- MAGIC
-- MAGIC     return human_readable.strip()
-- MAGIC
-- MAGIC # Register the UDF
-- MAGIC cron_to_human_readable_udf = udf(cron_to_human_readable, StringType())
-- MAGIC df_transformed = df.withColumn("human_readable", cron_to_human_readable_udf(df.quartz_cron_expression))
-- MAGIC display(df_transformed)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Can I get the URLs of all my jobs in a subset of workspaces?
-- MAGIC Here is some python code that does it for you and returns a Dataframe.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC
-- MAGIC # set a list of intersted workspaces
-- MAGIC workspace_list = ['https://adb-12345678901234.12.azuredatabricks.net/']
-- MAGIC
-- MAGIC # create a df of the interested workspace URLs
-- MAGIC workspace_df = (spark.createDataFrame([(url,) for url in workspace_list], ["workspace_url"])
-- MAGIC                 .withColumn("workspace_id", regexp_extract("workspace_url", r"https://adb-(\d+)\.11\.azuredatabricks\.net/", 1))
-- MAGIC                 )
-- MAGIC display(workspace_df)
-- MAGIC
-- MAGIC # load the jobs
-- MAGIC jobs_df = spark.read.table("system.lakeflow.jobs")
-- MAGIC display(jobs_df)
-- MAGIC
-- MAGIC # Inner join to filter and combine cols to format URL
-- MAGIC joined_df = jobs_df.join(workspace_df, on="workspace_id", how="inner")
-- MAGIC joined_df = joined_df.withColumn("job_url", concat(col("workspace_url"), lit("jobs/"), col("job_id")))
-- MAGIC
-- MAGIC display(joined_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Who are all admins in my workspace?
-- MAGIC Fine. This one is technically not using system tables in Databricks and is actually using the Databricks Python SDK, but it is a working example of getting all admin users in a workspace programmatically!

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from databricks.sdk import WorkspaceClient
-- MAGIC
-- MAGIC w = WorkspaceClient()
-- MAGIC
-- MAGIC all_users = w.users.list()
-- MAGIC
-- MAGIC for u in all_users:
-- MAGIC   user_groups = u.groups
-- MAGIC   for g in user_groups:
-- MAGIC     if g.display=="admins":
-- MAGIC       print(f"-------> {u.display_name} is a Workspace Admin") 

-- COMMAND ----------


