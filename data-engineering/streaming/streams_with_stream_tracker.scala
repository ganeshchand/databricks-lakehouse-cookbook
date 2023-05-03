// Databricks notebook source
import com.gcdaii.streamtracker.StreamProgressTracker

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// COMMAND ----------

// MAGIC %md
// MAGIC ## Parameters

// COMMAND ----------

val category = "data_engineering"
val topic = "streaming"
val subTopic = "monitoring"
val feature = "streams_tracker"


val rootPath = s"s3://db-gc-lakehouse/tmp"
val rootMetricksTrackerPath = s"$rootPath/$category/$topic/$subTopic/$feature"

// COMMAND ----------

import scala.util.Try
def deltaTableExistsAtPath(tablePath: String): Boolean = Try {
  dbutils.fs.ls(tablePath).filter(_.name.contains("_delta_log")).nonEmpty
}.getOrElse(false)


def handleInitialWrite(tablePath: String): Unit = {
  val isInitialWrite = !deltaTableExistsAtPath(tablePath)
  if(isInitialWrite) spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")  
}

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "4")

// COMMAND ----------

// stream 1

val tableName1 = "stream1"
val tablePath1 = s"$rootPath/tables/${category}/${topic}/${subTopic}/$feature/$tableName1"
val checkpointPath1 = s"$tablePath1/_checkpoint"

val stream1  = spark
      .readStream
      .format("rate-micro-batch")
      .option("rowsPerBatch", 10)
      .load()
      .withColumn("etl_date", current_date)
      .withColumn("etl_datetime", current_timestamp)
      .writeStream
      .format("delta")
      .outputMode("append")
      .queryName(tableName1)
      .trigger(Trigger.ProcessingTime("10 minutes"))
      .option("checkpointLocation", checkpointPath1)
      .start(tablePath1)

// COMMAND ----------

// stream 2
val tableName2 = "stream2"
val tablePath2 = s"$rootPath/tables/${category}/${topic}/${subTopic}/$feature/$tableName2"
val checkpointPath2 = s"$tablePath2/_checkpoint"

val stream2  = spark
      .readStream
      .format("delta")
      .option("maxFilesPerTrigger", 4)
      .load(tablePath1) // upstream dependency
      .writeStream
      .format("delta")
      .outputMode("append")
      .queryName(tableName2)
      .trigger(Trigger.ProcessingTime("15 minutes"))
      .option("checkpointLocation", checkpointPath2)
      .start(tablePath2)

// COMMAND ----------

// 


