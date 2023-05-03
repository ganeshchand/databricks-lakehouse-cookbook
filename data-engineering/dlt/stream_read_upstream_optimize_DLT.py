# Databricks notebook source
import dlt
from pyspark.sql.functions import expr

# COMMAND ----------

@dlt.create_table(
  comment="This is a DLT managed streaming table with Delta table as source",
  path="/tmp/gc/stream_read_upstream_optimize/dlt/target_table",  
  table_properties={
    "quality": "silver"
  }
)
def process():
  return (
   spark.readStream
    .format("delta")
    .option("rowsPerSecond", 1)  
    .load("/tmp/gc/stream_read_upstream_optimize/dlt/source_table/")
  )
