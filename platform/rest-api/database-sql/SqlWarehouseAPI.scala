// Databricks notebook source
// MAGIC %sql
// MAGIC select hive_metastore.gc_functions_test.hello()

// COMMAND ----------

spark.sql("select hive_metastore.gc_functions_test.hello()").queryExecution.analyzed.resolved

// COMMAND ----------

spark.sessionState.sqlParser.parsePlan("select hive_metastore.gc_functions_test.hello()").resolved

// COMMAND ----------

spark.sql("CREATE TEMPORARY FUNCTION hello() RETURNS STRING RETURN 'Hello World!';")

// COMMAND ----------

// MAGIC %sql
// MAGIC select hello()
