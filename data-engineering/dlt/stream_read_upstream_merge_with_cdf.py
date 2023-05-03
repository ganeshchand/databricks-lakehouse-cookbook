# Databricks notebook source
import dlt
from pyspark.sql.functions import expr

# COMMAND ----------

upstream_merge_table_path = "dbfs:/tmp/gc/de/dlt/cdf/source_table"

# COMMAND ----------

@dlt.create_table(
  comment="This is a DLT managed streaming table with Delta table Merge AS source",
  path="dbfs:/tmp/gc/de/dlt/cdf/stream_read_upstream_merge_with_cdf",
  name = "cdf_target_table"
)
def process():
  return (
   spark.readStream
    .format("delta")
    .option("readChangeFeed", True)
    .option("startingVersion", 1) 
    .load(upstream_merge_table_path)
  )
