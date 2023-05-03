# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr

@dlt.view
def events_source():
  return spark.readStream.format("delta").load("/tmp/gc_lakehouse/ft/dlt/dedup/bronze")

dlt.create_streaming_table(
  name = "events"
  )

dlt.apply_changes(
  target = "events",
  source = "events_source",
  keys = ["device_id"],
  sequence_by = col("event_timestamp")
)
