// Databricks notebook source


// COMMAND ----------


import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame


def getIotEvents(location: String = "dbfs:/databricks-datasets/iot/iot_devices.json"): DataFrame = {
  spark.read.json(location)
  .selectExpr(
    "device_id", "device_name", "battery_level", "ip", 
    "cast(timestamp / 1000 as timestamp) as event_timestamp")
  .where("device_id IN (1,2,3,4,5)")  
}

def getIotEventsById(deviceId: Int, location: String = "dbfs:/databricks-datasets/iot/iot_devices.json"): DataFrame = {
  spark.read.json(location)
  .selectExpr(
    "device_id", "device_name", "battery_level", "ip", 
    "cast(timestamp / 1000 as timestamp) as event_timestamp")
    .where(s"device_id = $deviceId")
}

// COMMAND ----------


val bronzeTablePath = "/tmp/gc_lakehouse/ft/dlt/dedup/bronze"

getIotEvents()
.write
.format("delta")
.mode("append")
.save(bronzeTablePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Run 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`/tmp/gc_lakehouse/ft/dlt/dedup/bronze`

// COMMAND ----------

// MAGIC 	%sql
// MAGIC   select * from gc_ft_dlt.events

// COMMAND ----------

// MAGIC %md
// MAGIC ## Introduce duplicates

// COMMAND ----------

getIotEvents().where("device_id = 1")
.write
.format("delta")
.mode("append")
.save(bronzeTablePath)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Run 2

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`/tmp/gc_lakehouse/ft/dlt/dedup/bronze`

// COMMAND ----------

// MAGIC 	%sql
// MAGIC   select * from gc_ft_dlt.events

// COMMAND ----------

// MAGIC %sql
// MAGIC describe extended gc_ft_dlt.events

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history gc_ft_dlt.`__apply_changes_storage_events`

// COMMAND ----------

// MAGIC %md
// MAGIC ## Run3

// COMMAND ----------

val duplicateEvents = getIotEventsById(6).union(getIotEventsById(6)).union(getIotEventsById(6))

// COMMAND ----------

display(duplicateEvents)

// COMMAND ----------

duplicateEvents
.write
.format("delta")
.mode("append")
.save(bronzeTablePath)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`/tmp/gc_lakehouse/ft/dlt/dedup/bronze`

// COMMAND ----------

// MAGIC 	%sql
// MAGIC   select * from gc_ft_dlt.events

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history gc_ft_dlt.`__apply_changes_storage_events`

// COMMAND ----------

// MAGIC %fs ls dbfs:/pipelines/9d29cb27-9cbd-488e-8e6d-5bccf353bf62

// COMMAND ----------


