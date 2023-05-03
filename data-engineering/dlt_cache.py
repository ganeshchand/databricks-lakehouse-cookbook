# Databricks notebook source
iot_data_path = "/databricks_lakehouse_cookbook/data/iot/iot_devices/format=parquet/"

# COMMAND ----------

import dlt
from pyspark.sql.functions import *


@dlt.table
def dlt_cache_test_raw_iot():
  df1 = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(iot_data_path)
            .select("*", "_metadata"))
  df2 = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(iot_data_path)
            .select("*", "_metadata"))
  df3 = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(iot_data_path)
            .select("*", "_metadata"))
    
  df4 = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(iot_data_path)
            .select("*", "_metadata"))
  return df1.union(df2).union(df3).union(df4)

@dlt.table
def dlt_cache_test_even():
  return dlt.read_stream("dlt_cache_test_raw_iot").where("device_id % 2 == 0")
        
@dlt.table
def dlt_cache_test_odd():
  return dlt.read_stream("dlt_cache_test_raw_iot").where("device_id % 2 <> 0")

@dlt.table
def dlt_cache_test_agg():
  return dlt.read("dlt_cache_test_raw_iot").groupBy("cca2").count()

# COMMAND ----------

# MAGIC %md
# MAGIC pyspark.sql.utils.AnalysisException: Queries with streaming sources must be executed with writeStream.start();
# MAGIC cloudFiles
