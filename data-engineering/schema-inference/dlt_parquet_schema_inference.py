# Databricks notebook source
iot_data_path = "/databricks_lakehouse_cookbook/data/iot/iot_devices/format=parquet/"
numeric_odd_even_data_path = "/databricks_lakehouse_cookbook/data/numeric/number_odd_even/format=parquet/"


# COMMAND ----------

import dlt
from pyspark.sql.functions import *


@dlt.table
def dlt_parquet_schema_inference_iot(
table_properties={"delta.minReaderVersion" : "2", "delta.minWriterVersion" : "5","delta.columnMapping.mode": "name"}
):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(iot_data_path)
            .select("*", "_metadata")
    )

@dlt.table
def dlt_parquet_schema_inference_odd_even(
table_properties={"delta.minReaderVersion" : "2", "delta.minWriterVersion" : "5","delta.columnMapping.mode": "name"}
):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .load(numeric_odd_even_data_path)
            .select("*", "_metadata")
    )    
