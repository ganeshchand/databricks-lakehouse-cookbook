# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/iot/

# COMMAND ----------

from pyspark.sql.functions import *
df = (
  spark.read.json("dbfs:/databricks-datasets/iot/iot_devices.json")
  .withColumn("event_timestamp", from_unixtime(col('timestamp') / 1000))
  .withColumn("event_date", to_date(col('event_timestamp')))
  .withColumn("event_day", dayofmonth(col('event_timestamp')))
  .withColumn("event_hour", hour(col('event_timestamp')))
)

# COMMAND ----------

display(df)

# COMMAND ----------

display(
  df.groupBy("event_date", "event_hour").count()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select from_unixtime(1458444054093/1000), to_date(from_unixtime(1458444054093/1000)), dayofmonth(from_unixtime(1458444054093/1000))

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/iot-stream/data-device/

# COMMAND ----------

display(spark.read.json("dbfs:/databricks-datasets/iot-stream/data-device/"))

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets/iot-stream/data-user/

# COMMAND ----------

display(spark.read.option("header", True).csv("dbfs:/databricks-datasets/iot-stream/data-user/userData.csv"))

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.rm("/tmp/gc/ss/333/", true)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC spark
# MAGIC   .readStream
# MAGIC   .format("rate-micro-batch")
# MAGIC   .option("rowsPerBatch", 1000)
# MAGIC   .load()
# MAGIC   .repartition(10)
# MAGIC   .writeStream
# MAGIC   .format("delta")
# MAGIC   .outputMode("append")
# MAGIC   .trigger(Trigger.Once)
# MAGIC   .option("checkpointLocation", "/tmp/gc/ss/111/_checkpoint")
# MAGIC   .start("/tmp/gc/ss/111/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (
# MAGIC describe history delta.`/tmp/gc/ss/111/`)

# COMMAND ----------

# MAGIC %scala
# MAGIC val dt = io.delta.tables.DeltaTable.forPath("/tmp/gc/ss/111/")
# MAGIC display(dt.detail)

# COMMAND ----------

# MAGIC %sql
# MAGIC select version, timestamp, operationMetrics from (
# MAGIC describe history delta.`/tmp/gc/ss/111/`)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC spark
# MAGIC   .readStream
# MAGIC   .format("delta")
# MAGIC   .option("maxFilesPerTrigger", 1)
# MAGIC   .load("/tmp/gc/ss/111/")
# MAGIC   .writeStream
# MAGIC   .format("delta")
# MAGIC   .outputMode("append")
# MAGIC   .option("checkpointLocation", "/tmp/gc/ss/222/_checkpoint")
# MAGIC   .trigger(Trigger.AvailableNow)
# MAGIC   .start("/tmp/gc/ss/222/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select version, timestamp, operationMetrics from (
# MAGIC describe history delta.`/tmp/gc/ss/222/`)

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC spark
# MAGIC   .readStream
# MAGIC   .format("delta")
# MAGIC   .option("maxFilesPerTrigger", 1)
# MAGIC   .load("/tmp/gc/ss/111/")
# MAGIC   .writeStream
# MAGIC   .format("delta")
# MAGIC   .outputMode("append")
# MAGIC   .option("checkpointLocation", "/tmp/gc/ss/333/_checkpoint")
# MAGIC   .trigger(Trigger.Once)
# MAGIC   .start("/tmp/gc/ss/333/")

# COMMAND ----------

# MAGIC %fs ls /tmp/gc/ss/333/_checkpoint

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/gc/ss/333/_checkpoint/commits/

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/gc/ss/333/_checkpoint/commits/0

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/gc/ss/333/_checkpoint/commits/0

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/gc/ss/333/_checkpoint/metadata

# COMMAND ----------

# MAGIC %fs ls dbfs:/tmp/gc/ss/333/_checkpoint/offsets/

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/gc/ss/333/_checkpoint/offsets/0

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/gc/ss/333/_checkpoint/offsets/1

# COMMAND ----------

# MAGIC %fs head dbfs:/tmp/gc/ss/333/_checkpoint/offsets/2

# COMMAND ----------

# MAGIC %sql
# MAGIC select version, timestamp, operationMetrics from (
# MAGIC describe history delta.`/tmp/gc/ss/333/`)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view test_json as SELECT '{"modeSessionId":"00000000-0000-0000-0000-000000000000",
# MAGIC    "transactionInstanceId":"81c56536-1210-4938-8906-59d1420c5419",
# MAGIC    "subModeId":"cd883a33-4026-4881-8694-391c427aa3f4",
# MAGIC    "hardwareGPU":"ps5-Gnmp/Agc, native gfx jobs",
# MAGIC    "clubPassSeasonId":"SEASON3-clubpassseason",
# MAGIC    "buildEnvironment":"RELEASE",
# MAGIC    "hardwareCPU":"PS5 x64",
# MAGIC    "transactionSourceLocation":"Match",
# MAGIC    "applicationSessionId":"4068fa42-9bfa-4bf3-98bb-0635341fa04e",
# MAGIC    "earnedItems":[
# MAGIC       {
# MAGIC          "reason":"GolferLevelReward",
# MAGIC          "amount":250,
# MAGIC          "type":"VC"
# MAGIC       }
# MAGIC    ],
# MAGIC    "buildChangelist":508678,
# MAGIC    "roundType":"None"
# MAGIC   }' as data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select 
# MAGIC from_json(data:earnedItems, 'array<string>') as arr,
# MAGIC
# MAGIC filter(from_json(data:earnedItems, 'array<struct<reason string, amount int, type string>>'), x -> x.amount = '250')[0]["amount"]
# MAGIC -- array_position(from_json(data:earnedItems, 'array<string>'), '{\"reason\":\"GolferLevelReward\",\"amount\":250,\"type\":\"VC\"}'),
# MAGIC -- array_position(from_json(data:earnedItems, 'array<struct<reason string, amount int, type string>>').amount, '250')
# MAGIC -- case when (data:earnedItems[*].`type` = 'VC') THEN data:earnedItems[*].`amount` ELSE NULL END,
# MAGIC -- array_contains(from_json(data:earnedItems, 'array<string>'), '\amount') as test
# MAGIC from test_json

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select data:earnedItems[0]  from test_json

# COMMAND ----------


