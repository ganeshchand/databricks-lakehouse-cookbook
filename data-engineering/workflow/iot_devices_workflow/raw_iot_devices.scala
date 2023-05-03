// Databricks notebook source
val version = 1
val iot_data_path = "s3://db-gc-lakehouse/dlt_migration/ver=0/data/iot/iot_devices/format=parquet/"
val rootOutputPath = s"s3://db-gc-lakehouse/workflows/simple_workflow/iot_devices/ver=$version"

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger._

// COMMAND ----------

val rawIotDevices = spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.maxFilesPerTrigger", 1)
            .option("cloudFiles.schemaLocation", s"$rootOutputPath/raw/_schemaLocation")
            .load(iot_data_path)
            .select("*", "_metadata")

// COMMAND ----------

if(!io.delta.tables.DeltaTable.isDeltaTable(s"$rootOutputPath/raw/")) spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

// COMMAND ----------


rawIotDevices
  .writeStream
  .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
  .option("checkpointLocation", s"$rootOutputPath/raw/_checkpointLocation")
  .foreachBatch((df: org.apache.spark.sql.DataFrame, id: Long) => {
    println("Processing microbatch $id")
//     Thread.sleep(1000 * 60 * 5)
   df.write.mode("append").save(s"$rootOutputPath/raw/")
 }).start()

// COMMAND ----------


