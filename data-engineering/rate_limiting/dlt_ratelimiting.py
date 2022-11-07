# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC --source table: 8 files, 198,164 rows
# MAGIC select h.operationMetrics.numFiles, h.operationMetrics.numOutputRows
# MAGIC from (describe history delta.`/databricks_lakehouse_cookbook/data/iot/iot_devices/format=delta/`) h

# COMMAND ----------

@dlt.table
def dlt_rate_limit_bronze():
    return (
        spark.readStream
            .format("delta")           
            .option("maxFilesPerTrigger", 1)
            .load("/databricks_lakehouse_cookbook/data/iot/iot_devices/format=delta/")

    )

# @dlt.table
# def dlt_rate_limit_silver():
#     return (
#         dlt.read_stream("dlt_rate_limit_bronze")       
#             .option("maxFilesPerTrigger", 1) # you cannot do this because dlt.read_stream() returns a dataframe
#     )

@dlt.table
def dlt_rate_limit_silver():
    return (
        spark.readStream
             .option("maxFilesPerTrigger", 2) 
             .table("LIVE.dlt_rate_limit_bronze")                   
    ) 
