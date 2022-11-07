// Databricks notebook source
// MAGIC %fs ls dbfs:/pipelines/d5bc7315-b5d2-46aa-a47e-142cadf5f1bc

// COMMAND ----------

// MAGIC %fs ls dbfs:/pipelines/d5bc7315-b5d2-46aa-a47e-142cadf5f1bc/checkpoints/

// COMMAND ----------

one checkpoint per table

// COMMAND ----------

// MAGIC %fs ls dbfs:/pipelines/d5bc7315-b5d2-46aa-a47e-142cadf5f1bc/autoloader/

// COMMAND ----------

// MAGIC %fs ls dbfs:/pipelines/d5bc7315-b5d2-46aa-a47e-142cadf5f1bc/autoloader/533568772/_schemas

// COMMAND ----------

// MAGIC %fs head dbfs:/pipelines/d5bc7315-b5d2-46aa-a47e-142cadf5f1bc/autoloader/533568772/_schemas/0

// COMMAND ----------

// MAGIC %fs head dbfs:/pipelines/d5bc7315-b5d2-46aa-a47e-142cadf5f1bc/autoloader/-1616844952/_schemas/0

// COMMAND ----------

// MAGIC %fs head dbfs:/pipelines/d5bc7315-b5d2-46aa-a47e-142cadf5f1bc/autoloader/-1515587845/_schemas/0

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`dbfs:/pipelines/d5bc7315-b5d2-46aa-a47e-142cadf5f1bc/system/events/`
// MAGIC order by timestamp desc

// COMMAND ----------


