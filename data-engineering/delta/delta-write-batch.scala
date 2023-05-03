// Databricks notebook source
spark.range(10).write.mode("append").save("/tmp/gc/delta/test")

// COMMAND ----------

spark.readStream.format("rate").load().writeStream.format("delta").outputMode("append").option("checkpointLocation", "/tmp/gc/delta/streaming/test/_checkpoint").start("/tmp/gc/delta/streaming/test/")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`/tmp/gc/delta/streaming/test/`
