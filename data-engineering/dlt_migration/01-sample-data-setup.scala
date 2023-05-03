// Databricks notebook source
import scala.util._
val ver = 0
val rootPath = s"s3://db-gc-lakehouse/dlt_migration/ver=$ver"
Try(dbutils.fs.ls(rootPath)).getOrElse(dbutils.fs.mkdirs(rootPath))
require(dbutils.fs.ls(rootPath).size >= 0)

// COMMAND ----------

// DBTITLE 1,Generate IOT Parquet Sample data
val deviceJsonData = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
val numberOfFiles = 4
val format = "parquet"
val outputPath = rootPath.stripSuffix("/") + s"/data/iot/iot_devices/format=$format/"
deviceJsonData.repartition(numberOfFiles.toInt).orderBy("timestamp").write.format(format).mode("overwrite").save(outputPath)

// remove control files
dbutils.fs.ls(outputPath).foreach { fileInfo => 
  if(fileInfo.name.startsWith("_")) dbutils.fs.rm(fileInfo.path)
}

// COMMAND ----------

// DBTITLE 1,Generate IOT JSON Sample data
val deviceJsonData = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
val numberOfFiles = 4
val format = "json"
val outputPath = rootPath.stripSuffix("/") + s"/data/iot/iot_devices/format=$format/"
deviceJsonData.repartition(numberOfFiles.toInt).orderBy("timestamp").write.format(format).mode("overwrite").save(outputPath)

// remove control files
dbutils.fs.ls(outputPath).foreach { fileInfo => 
  if(fileInfo.name.startsWith("_")) dbutils.fs.rm(fileInfo.path)
}

// COMMAND ----------

val deviceJsonData = spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
val numberOfFiles = 4
val format = "delta"
val outputPath = rootPath.stripSuffix("/") + s"/data/iot/iot_devices/format=$format/"
deviceJsonData.repartition(numberOfFiles.toInt).orderBy("timestamp").write.format(format).mode("overwrite").save(outputPath)

// COMMAND ----------


