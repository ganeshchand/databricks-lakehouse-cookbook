// Databricks notebook source
val version = "1"
val iotDevicesRawDeltaTablePath = s"s3://db-gc-lakehouse/workflows/simple_workflow/iot_devices/ver=$version/raw"
val outputPath =                  s"s3://db-gc-lakehouse/workflows/simple_workflow/iot_devices/ver=$version/aggregation_cca3"

// COMMAND ----------

import org.apache.spark.sql.functions._
spark.table(s"delta.`$iotDevicesRawDeltaTablePath`")
  .groupBy("cca3")
  .agg(
    count("device_id").alias("device_count"),
    avg("humidity").alias("avg_humidity")
  )
.write
.format("delta")
.mode("overwrite")
.save(outputPath)
