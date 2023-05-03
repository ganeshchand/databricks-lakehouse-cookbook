// Databricks notebook source
// dbutils.fs.rm("/tmp/gc/stream_read_upstream_optimize/dlt/source_table/", true)
// dbutils.fs.rm("/tmp/gc/stream_read_upstream_optimize/dlt/target_table/", true)

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

val sourceStreamTablePath = "/tmp/gc/stream_read_upstream_optimize/dlt/source_table"
val targetStreamTablePath = "/tmp/gc/stream_read_upstream_optimize/dlt/target_table"

def enableAutoOptimizeConfig(): Unit = {
  spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")
  spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true")
}

def disableAutoOptimizeConfig(): Unit = {
  spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false")
  spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false")
}
def ingestIntoSourceTables(batchId: Int, enableAutoOptimize: Boolean, tablePath: String): Unit = {
  if(enableAutoOptimize) {
    enableAutoOptimizeConfig()
  }
  spark
    .readStream
    .format("rate")
    .option("rowsPerSecond", 10).load()
    .withColumn("etl_date", current_date)
    .withColumn("batch_id", lit(batchId))
    .writeStream
    .partitionBy("etl_date")
    .format("delta")
    .option("checkpointLocation", s"${tablePath}/_checkpoint")
    .trigger(Trigger.Once)
    .outputMode("append")
    .start(s"$tablePath")
  
  disableAutoOptimizeConfig()
}

def streamDeltaToDelta(batchId: Int, sourcePath: String, targetPath: String): Unit = {
  spark
  .readStream
  .format("delta")
  .load(sourcePath)
  .withColumn("processing_batch_id", lit(batchId))
  .writeStream
  .format("delta")
  .option("checkpointLocation", s"${targetPath}/_checkpoint")
  .trigger(Trigger.Once)
  .outputMode("append")
  .start(targetPath)
}

def showHistory(tablePath: String): Unit = {
  display(spark.sql(s"describe history delta.`$tablePath`").select("version", "operation", "operationMetrics"))
}

def showRecords(tablePath: String): Unit = {
  display(spark.sql(s"""
  select batch_id, count(*) as num_rows from delta.`$tablePath`
  group by batch_id
  order by batch_id asc
  """))
}

def optimizeTable(path: String): Unit = {
  spark.sql(s"optimize delta.`$path`")
}

// COMMAND ----------

ingestIntoSourceTables(1, false, sourceStreamTablePath)

// COMMAND ----------

showHistory(sourceStreamTablePath)

// COMMAND ----------

showRecords(sourceStreamTablePath)

// COMMAND ----------

// RUN DLT Pipeline: https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#joblist/pipelines/3c51ab9d-6097-4ec9-994f-8182a4b0ab5e/updates/7f907725-7801-4646-b6ca-66e30e3df27a

// COMMAND ----------

optimizeTable(sourceStreamTablePath)

// COMMAND ----------

showHistory(sourceStreamTablePath)

// COMMAND ----------

// RUN DLT Pipeline: https://e2-demo-west.cloud.databricks.com/?o=2556758628403379#joblist/pipelines/3c51ab9d-6097-4ec9-994f-8182a4b0ab5e/updates/7f907725-7801-4646-b6ca-66e30e3df27a

// COMMAND ----------

showRecords(targetStreamTablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC select 'source', count(*) as count from delta.`/tmp/gc/stream_read_upstream_optimize/dlt/source_table/`
// MAGIC union 
// MAGIC select 'target', count(*) as count from delta.`/tmp/gc/stream_read_upstream_optimize/dlt/target_table/`

// COMMAND ----------

ingestIntoSourceTables(2, true, sourceStreamTablePath)

// COMMAND ----------

showHistory(sourceStreamTablePath)

// COMMAND ----------

ingestIntoSourceTables(2, true, sourceStreamTablePath)

// COMMAND ----------

showHistory(sourceStreamTablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC select 'source', count(*) as count from delta.`/tmp/gc/stream_read_upstream_optimize/dlt/source_table/`
// MAGIC union 
// MAGIC select 'target', count(*) as count from delta.`/tmp/gc/stream_read_upstream_optimize/dlt/target_table/`

// COMMAND ----------

// MAGIC %sql
// MAGIC optimize delta.`/tmp/gc/stream_read_upstream_optimize/dlt/source_table/`
// MAGIC zorder by batch_id

// COMMAND ----------

showHistory(sourceStreamTablePath)

// COMMAND ----------

// run DLT pipeline

// COMMAND ----------

// MAGIC %sql
// MAGIC select 'source', count(*) as count from delta.`/tmp/gc/stream_read_upstream_optimize/dlt/source_table/`
// MAGIC union 
// MAGIC select 'target', count(*) as count from delta.`/tmp/gc/stream_read_upstream_optimize/dlt/target_table/`

// COMMAND ----------

showHistory("/tmp/gc/stream_read_upstream_optimize/dlt/source_table/")

// COMMAND ----------

showHistory("/tmp/gc/stream_read_upstream_optimize/dlt/target_table/")
