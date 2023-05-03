// Databricks notebook source
// MAGIC %md
// MAGIC ## Delta To Delta w/ Structured Streaming

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import scala.util.Try

val parentTopic = "de"
val subTopic = "streaming"
val topic = "checkpoint_monitoring"

// create Source Delta Table
val ver=1
val databaseName = s"gc_${parentTopic}_${subTopic}_${topic}"
val tableName = "delta_rate_microbatch_source"
val format = "delta"
val tablePath = s"s3://db-gc-lakehouse/$databaseName/source=$format/$tableName/ver=$ver"

val sampleData = 
    spark
      .readStream
      .format("rate-micro-batch")
      .option("rowsPerBatch", 10)
      .load()
      .withColumn("etl_date", current_date)
      .withColumn("etl_datetime", current_timestamp)

def deltaTableExistsAtPath(path: String): Boolean = Try {
  dbutils.fs.ls(tablePath).filter(_.name.contains("_delta_log")).nonEmpty
}.getOrElse(false)

val isInitialWrite = !deltaTableExistsAtPath(tablePath)
if(isInitialWrite) spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

// COMMAND ----------

object StreamPrgressTrackerPayload {
  import org.json4s._
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization.{read, write}
  implicit val formats = DefaultFormats
  
  case class QueryTerminatedPayload(id: String, runId: String, exception: String, timestamp: Long = java.time.Instant.now().toEpochMilli) {
    def toJson() = write(this)
  }
  
    case class QueryProgressPayload(id: String, runId: String, name: String, timestamp: Long = java.time.Instant.now().toEpochMilli) {
    def toJson() = write(this)
  }
   
}

// COMMAND ----------

java.time.Instant.now.atZone(java.time.ZoneId.of("UTC")).toLocalDate

// COMMAND ----------

import StreamPrgressTrackerPayload._
QueryTerminatedPayload("asdfasd", "asdfasdf", "exception").toJson

// COMMAND ----------

// sealed trait QueryProgressEventParser {
//   case class QueryProgressEventMetadata(streamId: String, runId: String, streamQueryName: String, batchId: Long)
//   def getProgressMedata(queryProgressJson: String): QueryProgressEventMetadata = {
//     import org.json4s._
//     import org.json4s.jackson.JsonMethods._
//     val parsedJson = parse(queryProgressJson)
//     implicit val formats = DefaultFormats
//     val streamId = (parsedJson \ "id").extract[String]
//     val runId = (parsedJson \ "runId").extract[String]
//     val streamName = (parsedJson \ "name").extract[String]
//     val batchId = (parsedJson \ "batchId").extract[Long]
//     QueryProgressEventMetadata(streamId, runId, streamName, batchId)
//   }
// }

object StreamPrgressTrackerPayload {
  import org.json4s._
  import org.json4s.DefaultFormats
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization.{read, write}
  implicit val formats = DefaultFormats
  
  case class QueryTerminatedPayload(id: String, runId: String, exception: String, timestamp: Long = java.time.Instant.now().toEpochMilli) {
    def toJson() = write(this)
  }   
}
    
object StreamProgressTracker {
  import StreamPrgressTrackerPayload._
  import org.apache.spark.sql.SparkSession
  def run(spark: SparkSession, rootPath: String): Unit = {
    import org.apache.spark.sql.streaming.StreamingQueryListener
    import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryStartedEvent, QueryTerminatedEvent, QueryProgressEvent}  
    
    spark.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = {
          println(s"Stream Query ${queryStarted.id} started")
          // do not persist started events
      }
      override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = {
          val streamId = queryTerminated.id.toString
          val runId = queryTerminated.runId.toString
          val exceptionMsg = queryTerminated.exception.getOrElse("")
          val now =  java.time.Instant.now()
          val timestamp =  now.toEpochMilli
          val utcDate = now.atZone(java.time.ZoneId.of("UTC")).toLocalDate
//           println(s"Stream Query $streamId:$runId terminated with exception ${exception}")
          // persist termination events
          val fileName = s"${streamId}_${runId}_${timestamp}.json"
          val filePath = rootPath + "/stream_progress_tracker/events=terminated" + s"/streamId=${streamId}/date=$utcDate/$fileName"
          val payload = QueryTerminatedPayload(streamId, runId, exceptionMsg,timestamp).toJson
          dbutils.fs.put(filePath, payload, false)
      }
      override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
          val progress = queryProgress.progress
          val streamId = progress.id.toString
          val runId = progress.runId.toString
          val streamName = progress.name
          val batchId = progress.batchId
          val now =  java.time.Instant.now()
          val timestamp =  now.toEpochMilli
          val utcDate = now.atZone(java.time.ZoneId.of("UTC")).toLocalDate
//           println(s"Stream Query $streamName made progress with batch $batchId")
          // persist progress events
          val fileName = s"${streamId}_${runId}_${timestamp}.json"
          val filePath = rootPath + "/stream_progress_tracker/events=progress" + s"/streamId=${streamId}/date=$utcDate/$fileName"
          val payload = queryProgress.progress.json
          dbutils.fs.put(filePath, payload, false)        
      }
   })  
  }
}   

// COMMAND ----------

StreamProgressTracker.run(spark, "dbfs:/tmp/gc/streaming")
display(spark.readStream.format("rate").option("rowsPerSecond", 1).load())

// COMMAND ----------

display(spark.readStream.format("rate").option("rowsPerSecond", 1).load())

// COMMAND ----------

// dbutils.fs.rm("dbfs:/tmp/gc/streaming/checkpoint_tracker", true)

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/gc/streaming/stream_progress_tracker/"))

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/gc/streaming/stream_progress_tracker/events=terminated/"))

// COMMAND ----------

display(spark.read.json("dbfs:/tmp/gc/streaming/stream_progress_tracker/events=terminated/"))

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/gc/streaming/stream_progress_tracker/events=progress/"))

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/tmp/gc/streaming/stream_progress_tracker/events=progress/streamId=290553e1-d77c-4cd4-9fe3-48d0d3d0b66f/date=2023-01-18"))

// COMMAND ----------

display(
  spark.read.json("dbfs:/tmp/gc/streaming/stream_progress_tracker/events=progress/streamId=*/date=*")
 ) 

// COMMAND ----------

val sampleData = 
    spark
      .readStream
      .format("rate-micro-batch")
      .option("rowsPerBatch", 10)
      .load()
      .withColumn("etl_date", current_date)
      .withColumn("etl_datetime", current_timestamp)

// COMMAND ----------

import org.json4s._
import org.json4s.jackson.JsonMethods._

lazy val parsedJson = parse(""" {
  "id" : "40005f19-1d75-4c5b-8f9c-06c7596a2b88",
  "runId" : "da53a406-5bc6-47db-ab86-e7810f7c266c",
  "name" : "display_query_1",
  "timestamp" : "2023-01-11T07:02:40.000Z",
  "batchId" : 72,
  "numInputRows" : 1,
  "inputRowsPerSecond" : 1.001001001001001,
  "processedRowsPerSecond" : 3.003003003003003,
  "durationMs" : {
    "addBatch" : 182,
    "commitOffsets" : 64,
    "getBatch" : 0,
    "latestOffset" : 0,
    "queryPlanning" : 6,
    "triggerExecution" : 333,
    "walCommit" : 80
  },
  "stateOperators" : [ {
    "operatorName" : "globalLimit",
    "numRowsTotal" : 1,
    "numRowsUpdated" : 1,
    "allUpdatesTimeMs" : 37,
    "numRowsRemoved" : 0,
    "allRemovalsTimeMs" : 0,
    "commitTimeMs" : 26,
    "memoryUsedBytes" : 712,
    "numRowsDroppedByWatermark" : 0,
    "numShufflePartitions" : 1,
    "numStateStoreInstances" : 1,
    "customMetrics" : {
      "loadedMapCacheHitCount" : 72,
      "loadedMapCacheMissCount" : 0,
      "stateOnCurrentVersionSizeBytes" : 280
    }
  } ],
  "sources" : [ {
    "description" : "RateStreamV2[rowsPerSecond=1, rampUpTimeSeconds=0, numPartitions=default",
    "startOffset" : 88,
    "endOffset" : 89,
    "latestOffset" : 89,
    "numInputRows" : 1,
    "inputRowsPerSecond" : 1.001001001001001,
    "processedRowsPerSecond" : 3.003003003003003
  } ],
  "sink" : {
    "description" : "MemorySink",
    "numOutputRows" : 1
  }
}""")

implicit val formats = DefaultFormats
val streamId = (parsedJson \ "id").extract[String]
val runId = (parsedJson \ "runId").extract[String]
val streamName = (parsedJson \ "name").extract[String]
val batchId = (parsedJson \ "batchId").extract[Long]

// COMMAND ----------

display(spark.readStream.format("rate").option("rowsPerSecond", 1).load())

// COMMAND ----------

// MAGIC %md
// MAGIC ## AutoLoader
