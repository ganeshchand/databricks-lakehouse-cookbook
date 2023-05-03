// Databricks notebook source
// MAGIC %md
// MAGIC ## Data De-duplication

// COMMAND ----------

val ids = List(1,2,1)
val idsDF = ids.toDF("id")
idsDF.createOrReplaceTempView("ids")

// COMMAND ----------

 val distinctIds = ids.distinct // or ids.toSet // Set(1,2)
 // ids.distinctBy(_) // from Scala 2.13: def distinctBy[B](f: (A) => B): List[A]

// COMMAND ----------

// idsDF.distinct.show
idsDF.dropDuplicates.show

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct id from ids

// COMMAND ----------

// MAGIC %sql
// MAGIC select id, count(1) as num_occurances 
// MAGIC from ids
// MAGIC group by id
// MAGIC having num_occurances > 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select id from (
// MAGIC   select id, count(1) as num_occurances 
// MAGIC   from ids
// MAGIC   group by id)
// MAGIC   

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC SELECT id, dense_rank() OVER (PARTITION BY ID ORDER BY ID) as DENSE_RANK,
// MAGIC   rank() OVER (PARTITION BY ID ORDER BY ID) as RANK
// MAGIC FROM ids

// COMMAND ----------

Array([1,meter-gauge-1xbYRYcj,8,68.161.225.1,2016-03-20 03:20:54.093], [2,sensor-pad-2n2Pea,7,213.161.254.1,2016-03-20 03:20:54.119], [3,device-mac-36TWSKiT,2,88.36.5.1,2016-03-20 03:20:54.12], [4,sensor-pad-4mzWkz,6,66.39.173.154,2016-03-20 03:20:54.121], [5,therm-stick-5gimpUrBB,4,203.82.41.9,2016-03-20 03:20:54.122])

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select *, dense_rank() OVER (PARTITION BY device_id ORDER BY event_timestamp) as DENSE_RANK from (
// MAGIC select 1 as device_id,'meter-gauge-1xbYRYcj' as device_name,8 as battery_level,'68.161.225.1' as ip_address,cast('2016-03-20 03:20:54.000' as TIMESTAMP) as event_timestamp
// MAGIC UNION ALL
// MAGIC select 2 as device_id,'meter-gauge-2xbYRYcj' as device_name,3 as battery_level,'68.161.225.2' as ip_address,cast('2016-03-20 03:20:55.000' as TIMESTAMP) as event_timestamp
// MAGIC UNION ALL
// MAGIC select 1 as device_id,'meter-gauge-1xbYRYcj' as device_name,8 as battery_level,'68.161.225.1' as ip_address,cast('2016-03-20 03:20:55.000' as TIMESTAMP) as event_timestamp
// MAGIC )

// COMMAND ----------

// MAGIC %md
// MAGIC ## Structured Streaming

// COMMAND ----------

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._
import org.apache.spark.sql.functions._


implicit val sqlContext: SQLContext = spark.sqlContext
val source = MemoryStream[(Int, String,Int, String, java.sql.Timestamp)]

// COMMAND ----------


val deviceEvents = source.toDS
  .toDF("device_id", "device_name", "battery_level", "ip_address", "event_timestamp")
  .withColumn("processed_timestamp", current_timestamp)

println(deviceEvents.isStreaming)

val dedupedDeviceEvents = deviceEvents.dropDuplicates("device_id")

val dedupedDeviceEventsWithWaterMark = deviceEvents
.withWatermark("event_timestamp", "2 minutes")
.dropDuplicates("device_id")

// COMMAND ----------

println(deviceEvents.queryExecution.analyzed.numberedTreeString)
println(dedupedDeviceEvents.queryExecution.analyzed.numberedTreeString)
println(dedupedDeviceEventsWithWaterMark.queryExecution.analyzed.numberedTreeString)

// COMMAND ----------

// dbutils.fs.rm("/tmp/gc/ft/ss/deduplication/", true)

// COMMAND ----------


val deviceEventsQuery = deviceEvents.
  writeStream.
  format("delta").
  queryName("device_events").
  outputMode(OutputMode.Append).  
  trigger(Trigger.ProcessingTime(10.seconds)).
  option("checkpointLocation", "/tmp/gc/ft/ss/deduplication/device_events/_checkpoint").
  start("/tmp/gc/ft/ss/deduplication/device_events/")

val dedupedDeviceEventsQuery = dedupedDeviceEvents.
  writeStream.
  format("delta").
  queryName("device_events_deduped").
  outputMode(OutputMode.Append). 
  trigger(Trigger.ProcessingTime(30.seconds)).
  option("checkpointLocation", "/tmp/gc/ft/ss/deduplication/device_events_deduped/_checkpoint").
  start("/tmp/gc/ft/ss/deduplication/device_events_deduped/")

val dedupedDeviceEventsWithWaterMarkQuery = dedupedDeviceEventsWithWaterMark.
  writeStream.
  format("delta").
  queryName("device_events_deduped_watermark").
  outputMode(OutputMode.Append). 
  trigger(Trigger.ProcessingTime(30.seconds)).
  option("checkpointLocation", "/tmp/gc/ft/ss/deduplication/device_events_deduped_watermark/_checkpoint").
  start("/tmp/gc/ft/ss/deduplication/device_events_deduped_watermark/")

// COMMAND ----------

// MAGIC %fs ls /tmp/gc/ft/ss/deduplication/

// COMMAND ----------

source.addData((1, "test-1", 8, "1.1.1.1", java.sql.Timestamp.from(java.time.Instant.now())))
source.addData((2, "test-2", 8, "1.1.1.2", java.sql.Timestamp.from(java.time.Instant.now())))

// COMMAND ----------

deviceEventsQuery.processAllAvailable
dedupedDeviceEventsQuery.processAllAvailable
dedupedDeviceEventsWithWaterMarkQuery.processAllAvailable

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`dbfs:/tmp/gc/ft/ss/deduplication/device_events/`

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`dbfs:/tmp/gc/ft/ss/deduplication/device_events_deduped/`

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from delta.`dbfs:/tmp/gc/ft/ss/deduplication/device_events_deduped_watermark/`

// COMMAND ----------

source.addData((1, "test-1", 8, "1.1.1.1", java.sql.Timestamp.from(java.time.Instant.now())))

// COMMAND ----------

(3 to 10000).foreach { i  => source.addData((i, s"test-$i", scala.util.Random.nextInt(100), s"1.1.1.$i", java.sql.Timestamp.from(java.time.Instant.now())))}
