// Databricks notebook source
// MAGIC %sql
// MAGIC select version, timestamp, 
// MAGIC  operationMetrics.numAddedFiles,
// MAGIC  operationMetrics.numOutputRows, 
// MAGIC  operationParameters.outputMode, 
// MAGIC  operationParameters.queryId
// MAGIC from (
// MAGIC describe history hive_metastore.databricks_lakehouse_cookbook.dlt_rate_limit_bronze 
// MAGIC ) h
// MAGIC where h.operation = 'STREAMING UPDATE' and h.timestamp > '2022-11-07T08:07:28.000+0000'

// COMMAND ----------

// MAGIC %sql
// MAGIC select version, timestamp, 
// MAGIC  operationMetrics.numAddedFiles,
// MAGIC  operationMetrics.numOutputRows, 
// MAGIC  operationParameters.outputMode, 
// MAGIC  operationParameters.queryId
// MAGIC from (
// MAGIC describe history hive_metastore.databricks_lakehouse_cookbook.dlt_rate_limit_silver 
// MAGIC ) h
// MAGIC where h.operation = 'STREAMING UPDATE' and h.timestamp > '2022-11-07T08:07:28.000+0000'
