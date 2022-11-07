// Databricks notebook source
import scala.util._

val rootDir = "/databricks_lakehouse_cookbook/"
Try(dbutils.fs.ls(rootDir)).getOrElse(dbutils.fs.mkdirs(rootDir))
require(dbutils.fs.ls(rootDir).size >= 0)

// COMMAND ----------

// DBTITLE 1,Generate IOT Parquet Sample data
val deviceJsonData = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
val numberOfFiles = 100
val format = "parquet"
val outputPath = rootDir.stripSuffix("/") + s"/data/iot/iot_devices/format=$format/"
deviceJsonData.repartition(numberOfFiles.toInt).orderBy("timestamp").write.format(format).mode("overwrite").save(outputPath)

// remove control files
dbutils.fs.ls(outputPath).foreach { fileInfo => 
  if(fileInfo.name.startsWith("_")) dbutils.fs.rm(fileInfo.path)
}

// COMMAND ----------

// DBTITLE 1,Generate IOT Delta Sample data
val deviceJsonData = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
val numberOfFiles = 100
val format = "delta"
val outputPath = rootDir.stripSuffix("/") + s"/data/iot/iot_devices/format=$format/"
deviceJsonData.repartition(numberOfFiles.toInt).orderBy("timestamp").write.format(format).mode("overwrite").save(outputPath)

// COMMAND ----------

// MAGIC %sql
// MAGIC select h.operationMetrics.numFiles, h.operationMetrics.numOutputRows
// MAGIC from (describe history delta.`/databricks_lakehouse_cookbook/data/iot/iot_devices/format=delta/`) h

// COMMAND ----------

import org.apache.spark.sql.functions._
val numberOddEvenData = spark.range(100000)
  .withColumn("is_even", expr("id % 2 ==0"))
  .withColumn("is_odd", expr("is_even == false"))
val numberOfFiles = 100
val format = "parquet"
val outputPath = rootDir.stripSuffix("/") + s"/data/numeric/number_odd_even/format=$format/"
numberOddEvenData.repartition(numberOfFiles.toInt).write.format(format).mode("overwrite").save(outputPath)

// remove control files
dbutils.fs.ls(outputPath).foreach { fileInfo => 
  if(fileInfo.name.startsWith("_")) dbutils.fs.rm(fileInfo.path)
}
