# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.create_table(
  comment="This is a rate based streaming DLT table",
  path="s3://db-gc-lakehouse/features/dlt/skipChangeCommits/bronze/rate_source/",  
  table_properties={
    "quality": "bronze",
    "pipelines.metastore.tableName": "gc_features_dlt.rate_source"
  },
  partition_cols=["etl_date"]
)
def rate_source():
  return (
   spark.readStream
    .format("rate-micro-batch")
    .option("rowsPerBatch", 10)
    .option("maxBytesPerTrigger", "1024")  
    .load()
    .withColumn("etl_date", current_date())
  )
  
@dlt.create_table(
  comment="table containing even numbers",
  path="s3://db-gc-lakehouse/features/dlt/skipChangeCommits/silver/even/",  
  table_properties={
    "quality": "silver",
    "pipelines.metastore.tableName": "gc_features_dlt.rate_even_numbers"
  },
)
def even():
  return (
    spark.readStream
    .option("maxBytesPerTrigger", "100")
    .table("Live.rate_source")
  #  dlt
  #  .read_stream("rate_source") 
  #  .option("maxBytesPerTrigger", "1024") # DataFrame' object has no attribute 'option'
   .where('value % 2 == 0')
  ) 
