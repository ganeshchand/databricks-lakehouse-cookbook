-- Databricks notebook source
-- MAGIC %sql
-- MAGIC --find DLT pipeline ID from the pipeline event table
-- MAGIC select distinct origin.pipeline_id from delta.`dbfs:/pipelines/10b7a9fc-9e96-46aa-98f7-dbe613cb1592/system/events/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from delta.`dbfs:/pipelines/10b7a9fc-9e96-46aa-98f7-dbe613cb1592/system/events/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe extended delta.`dbfs:/pipelines/10b7a9fc-9e96-46aa-98f7-dbe613cb1592/system/events/`

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/pipelines/10b7a9fc-9e96-46aa-98f7-dbe613cb1592/tables/

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC describe extended delta.`dbfs:/pipelines/10b7a9fc-9e96-46aa-98f7-dbe613cb1592/tables/dlt_rate_limit_bronze/`
