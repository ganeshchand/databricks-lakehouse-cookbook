// Databricks notebook source
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._

def reset(paths: String*): Unit = {  
  paths.foreach { path =>
    println(s"Reseting path $path")
    dbutils.fs.rm(path,true)
  }
  println("Done!")
}

def enableAutoOptimizeConfig(): Unit = {
  spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true")
  spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true")
}

def disableAutoOptimizeConfig(): Unit = {
  spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = false")
  spark.sql("set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = false")
}
def ingestIntoSourceTables(batchId: Int, numRows: Int, enableAutoOptimize: Boolean, tablePath: String): Unit = {
  if(enableAutoOptimize) {
    enableAutoOptimizeConfig()
  }
  spark
    .readStream
//     .format("rate")
//     .option("rowsPerSecond", 10)
    .format("rate-micro-batch")
    .option("rowsPerBatch", numRows)
    .load()
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

val sourceStreamTablePath = "/tmp/gc/stream_read_upstream_optimize/ss/source_table"
val targetStreamTablePath = "/tmp/gc/stream_read_upstream_optimize/ss/target_table"

// COMMAND ----------

ingestIntoSourceTables(1, 10,false,sourceStreamTablePath)

// COMMAND ----------

showHistory(sourceStreamTablePath)

// COMMAND ----------

showRecords(sourceStreamTablePath)

// COMMAND ----------

// spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

// COMMAND ----------

streamDeltaToDelta(1,sourceStreamTablePath, targetStreamTablePath)

// COMMAND ----------

showRecords(targetStreamTablePath)

// COMMAND ----------

optimizeTable(sourceStreamTablePath)

// COMMAND ----------

showHistory(sourceStreamTablePath)

// COMMAND ----------

streamDeltaToDelta(2,sourceStreamTablePath, targetStreamTablePath)

// COMMAND ----------

showRecords(targetStreamTablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC select 'source', count(*) as count from delta.`/tmp/gc/stream_read_upstream_optimize/ss/source_table/`
// MAGIC union 
// MAGIC select 'target', count(*) as count from delta.`/tmp/gc/stream_read_upstream_optimize/ss/target_table/`

// COMMAND ----------

ingestIntoSourceTables(3, 10000,true, sourceStreamTablePath)

// COMMAND ----------

showHistory(sourceStreamTablePath)
