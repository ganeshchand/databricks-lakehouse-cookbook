// Databricks notebook source
import org.apache.spark.sql.execution.streaming.MemoryStream
implicit val ctx = spark.sqlContext
val intsStreams = MemoryStream[Int]

println(intsStreams.id)
println(intsStreams.initialOffset)
println(intsStreams.latestOffset)

// COMMAND ----------

import org.apache.spark.sql.functions.{timestamp_seconds, expr, count}

val test1 = intsStreams.toDF()
        .withColumn("eventTime", timestamp_seconds($"value"))
        .withColumn("even_odd_type", expr("case when value % 2 == 0 THEN 'even' else 'odd' end" ))
        .withWatermark("eventTime", "1 seconds")
        .groupBy("even_odd_type")
        .agg(count("*").as("count"))

// COMMAND ----------

display(test1)

// COMMAND ----------

// intsStreams.addData((1 to 5): _*)
intsStreams.addData((6 to 8): _*)

// COMMAND ----------

import org.apache.spark.sql.functions.{timestamp_seconds}

display(spark.range(10).withColumn("ts", timestamp_seconds($"id")))

// COMMAND ----------

display(df)
