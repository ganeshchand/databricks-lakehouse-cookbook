// Databricks notebook source
import org.apache.spark.sql.functions._
val source1 = spark
  .readStream
  .format("rate")
  .option("rowsPerSeconds", 10)
  .load()
  .withColumn("even", expr("value % 2 ==0"))
  .withWatermark("timestamp", "5 minutes")
//   .groupBy("even", window($"timestamp", "5 minutes"))
//   .agg(count("even").alias("even_counts"))
  .withColumnRenamed("timestamp", "event_timestamp")

// COMMAND ----------

display(source1)
