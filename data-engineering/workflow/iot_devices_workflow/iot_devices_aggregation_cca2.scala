// Databricks notebook source
val version = "1"
val iotDevicesRawDeltaTablePath = s"s3://db-gc-lakehouse/workflows/simple_workflow/iot_devices/ver=$version/raw"
val outputPath =                  s"s3://db-gc-lakehouse/workflows/simple_workflow/iot_devices/ver=$version/aggregation_cca2"

// COMMAND ----------

import org.apache.spark.sql.functions._

Thread.sleep(1000 * 60 * 2) // wait for 2 min

spark.table(s"delta.`$iotDevicesRawDeltaTablePath`")
  .groupBy("cca2")
  .agg(
    count("device_id").alias("device_count"),
    avg("humidity").alias("avg_humidity")
  )
.write
.format("delta")
.mode("overwrite")
.save(outputPath)

// COMMAND ----------

println("done!")
Thread.sleep(1000 * 60 * 10) // wait for 10 min
