// Databricks notebook source
// spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")
// spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true")

// COMMAND ----------

// spark.conf.get("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite") //not set by default

// COMMAND ----------

def processNewData(numRows: Int, numPartitions: Int, path: String): Unit = {
   spark
     .range(numRows)
     .repartition(numPartitions)
     .write
     .mode("append")
     .save(tablePath)
}

// COMMAND ----------

val tablePath = "/tmp/gc/delta_optimization/writer/table1"

// COMMAND ----------

processNewData(1000,8,tablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history delta.`/tmp/gc/delta_optimization/writer/table1`

// COMMAND ----------

// with Optimized writer

// COMMAND ----------

spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")

// COMMAND ----------

processNewData(1000,8,tablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history delta.`/tmp/gc/delta_optimization/writer/table1`

// COMMAND ----------

spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")
spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true")

// COMMAND ----------

processNewData(1000,8,tablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history delta.`/tmp/gc/delta_optimization/writer/table1`

// COMMAND ----------

spark.sql("set spark.databricks.delta.autoCompact.enabled = true")

// COMMAND ----------

processNewData(1000,8,tablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history delta.`/tmp/gc/delta_optimization/writer/table1`

// COMMAND ----------

spark.conf.set("spark.databricks.delta.autoCompact.minNumFiles",5) // default: 50

// COMMAND ----------

processNewData(1000,8,tablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history delta.`/tmp/gc/delta_optimization/writer/table1`

// COMMAND ----------

spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false")

// COMMAND ----------

processNewData(1000,8,tablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history delta.`/tmp/gc/delta_optimization/writer/table1`

// COMMAND ----------


