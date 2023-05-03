# Databricks notebook source
iot_data_path = "s3://db-gc-lakehouse/dlt_migration/ver=0/data/iot/iot_devices/format=parquet/"

# COMMAND ----------

from pyspark.sql.functions import *

(
  spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.maxFilesPerTrigger", 1)
  .option("ignoreCorruptFiles", "true")
  .option("ignoreMissingFiles", "true")
  .option("cloudFiles.schemaLocation", "s3://db-gc-lakehouse/demo/streaming/auto_loader/iot/iot_raw/ver=1/_schema_location")
  .load(iot_data_path)
  .select("*", "_metadata")
  .withColumn("event_timestamp", expr("from_unixtime(timestamp/1000)").cast("timestamp"))
  .withColumn("event_date", expr("to_date(event_timestamp)"))
  .writeStream
  .partitionBy("event_date")
  .format("delta")
  .outputMode("append")
  .trigger(once=True)
  .option("checkpointLocation","s3://db-gc-lakehouse/demo/streaming/auto_loader/iot/iot_raw/ver=1/_checkpoint_location")
  .queryName("iot_raw")
  .start("s3://db-gc-lakehouse/demo/streaming/auto_loader/iot/iot_raw/ver=1/")
)  
