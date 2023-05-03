# Databricks notebook source
iot_data_path = "s3://db-gc-lakehouse/dlt_migration/ver=0/data/iot/iot_devices/format=parquet/"

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

@dlt.table
def iot_devices_raw():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.maxFilesPerTrigger", 1)
            .load(iot_data_path)
            .select("*", "_metadata")
    )
    
@dlt.table
def iot_devices_aggregation_cca3():
  return(
    dlt.read("iot_devices_raw")
       .groupBy("cca3")
      .agg(count("device_id").alias("device_count"),
           avg("humidity").alias("avg_humidity"))
  )
@dlt.table
def iot_devices_aggregation_cca2():
  return(
    dlt.read("iot_devices_raw")
       .groupBy("cca2")
      .agg(count("device_ids").alias("device_count"),
           avg("humidity").alias("avg_humidity"))
  )  
