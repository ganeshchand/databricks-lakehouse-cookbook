// Databricks notebook source
// MAGIC %sql
// MAGIC show catalogs;

// COMMAND ----------

spark.catalog.listCatalogs.show()

// COMMAND ----------

spark.catalog.listDatabases().show(false)

// COMMAND ----------


