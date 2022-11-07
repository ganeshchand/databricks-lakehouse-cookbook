// Databricks notebook source
dbutils.notebook.getContext.tags

// COMMAND ----------

dbutils.notebook.getContext.extraContext

// COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")

// COMMAND ----------

io.delta.VERSION
