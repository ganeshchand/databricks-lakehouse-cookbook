// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import scala.util.Try

sealed trait IoTDataProcessingMode
object IoTDataProcessingMode {
  case object BatchProcessing extends IoTDataProcessingMode
  case class StreamProcessing(trigger: Trigger) extends IoTDataProcessingMode
}

final case class IoTDevicesIngestion(
  ver: Int,
  processingMode: IoTDataProcessingMode,
  sparkConfigs: Map[String,String] = Map.empty[String,String]
  ) {
  import IoTDataProcessingMode._
  lazy val numberOfInputFiles = dbutils.fs.ls(IoTDevicesIngestion.iotDataPath).size
  val processingType = processingMode match {
    case BatchProcessing => "batch"
    case StreamProcessing(_) => "streaming"
  }
  val outputPath= s"${IoTDevicesIngestion.rootOutputPathRawIotData}/$processingType/ver=$ver"
  val schemaLocation = s"$outputPath/_schemaLocation"
  val checkpointLocation = s"$outputPath/_checkpointLocation"
  IoTDevicesIngestion.setSparkConfigs(sparkConfigs) 
  private def streamIotData(trigger: Trigger): StreamingQuery = {
    IoTDevicesIngestion.disableDeltaCheckIfRequired(outputPath)
    Try(spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("maxFilesPerTrigger", 100)
            .option("cloudFiles.schemaLocation", "/tmp/1/_schema")
            .load(IoTDevicesIngestion.iotDataPath)
            .select("*", "_metadata")
    ).getOrElse {
     println("Unable to use autoloader schema inference")
     val schema = spark.read.parquet(IoTDevicesIngestion.iotDataPath).schema
     spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("maxFilesPerTrigger", 100)
            .schema(schema)
            .load(IoTDevicesIngestion.iotDataPath)
      }            
            .withColumn("input_file", org.apache.spark.sql.functions.input_file_name)
            .writeStream
            .queryName(s"streaming_handling_empty_batches_$ver")
            .outputMode("append")
            .option("checkpointLocation", checkpointLocation)
            .trigger(trigger)
            .foreachBatch((df: DataFrame, id: Long) => {            
              val numRows = df.count
              println(s"Processing batch $id with $numRows rows")
//               IoTDevicesIngestion.disableDeltaCheckIfRequired(outputPath)
              df
                .write
                .format("delta")
                .mode("append")
                .save(outputPath)
            }).start
  }
  private def batchIotData(predicate: Option[String]=None): Unit = {
    spark.read
            .format("parquet")            
            .load(IoTDevicesIngestion.iotDataPath)
            .withColumn("input_file", org.apache.spark.sql.functions.input_file_name)
            .where(if(predicate.isDefined) predicate.get else "1==1")
            .write
            .mode("append")
            .save(outputPath)            
  }
  def run(emptyBatch: Boolean = false): Unit = {
    println(s"Running IOT Ingestion version $ver in $processingType mode")
    processingMode match {
    case BatchProcessing => if(emptyBatch) batchIotData(Some("1 < 1")) else batchIotData()
    case StreamProcessing(t: Trigger) => streamIotData(t)
    }
  }
  
  lazy val deltaTableHistory: DataFrame = IoTDevicesIngestion.getDeltaTableHistory(outputPath).getOrElse(throw new Exception("unable to get table history"))  
}
object IoTDevicesIngestion {
  val iotDataPath = "/databricks_lakehouse_cookbook/data/iot/iot_devices/format=parquet/"
  val rootOutputPathRawIotData = "/databricks_lakehouse_cookbook/deltalake/iot/iot_devices/raw"
  
  def resetAllVersions(rootOutputPath: String = rootOutputPathRawIotData) = {   
    dbutils.fs.rm(rootOutputPath, true)
  }
  
  def setSparkConfigs(configs: Map[String,String] = Map.empty[String,String]): Unit = {
    if(!configs.isEmpty) {
      configs.foreach  { case (k,v) => {
        println(s"Setting $k to $v")
        spark.conf.set(k,v)
      }}
    }
  }
  def getDeltaTableHistory(tablepath: String): Option[DataFrame] = {
    Try {
      io.delta.tables.DeltaTable.forPath(tablepath).history.toDF
    }.toOption
  }
  def disableDeltaCheckIfRequired(deltaTablePath: String) = {
    if(Try(dbutils.fs.ls(deltaTablePath)).isFailure)
      println(s"Existing delta table not found at $deltaTablePath. Disabling delta format check for first append")
      spark.conf.set("spark.databricks.delta.formatCheck.enabled", false)
  }
}

// COMMAND ----------

IoTDevicesIngestion.resetAllVersions()

// COMMAND ----------

// val predicate: Option[String] = Some("1 < 1")
val predicate: Option[String] = None
val test = if(predicate.isDefined) predicate.get else "1==1"
spark.read
            .format("parquet")            
            .load(IoTDevicesIngestion.iotDataPath)
            .withColumn("input_file", org.apache.spark.sql.functions.input_file_name)
            .where(test).count

// COMMAND ----------

println(spark.conf.get("spark.sql.streaming.noDataMicroBatches.enabled")) // default

// COMMAND ----------

import IoTDataProcessingMode._
val batchIngestionV1 = IoTDevicesIngestion(ver=1, BatchProcessing)
batchIngestionV1.run()

// COMMAND ----------

display(batchIngestionV1.deltaTableHistory)

// COMMAND ----------

batchIngestionV1.run(emptyBatch=true)

// COMMAND ----------

display(batchIngestionV1.deltaTableHistory.selectExpr("version", "operationMetrics.numOutputRows"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Streaming

// COMMAND ----------

import IoTDataProcessingMode._
val streamIngestionV1 = IoTDevicesIngestion(ver=1, StreamProcessing(Trigger.Once))
streamIngestionV1.run()

// COMMAND ----------

display(streamIngestionV1.deltaTableHistory.selectExpr("version", "operationMetrics.numOutputRows"))

// COMMAND ----------

streamIngestionV1.run()

// COMMAND ----------

display(streamIngestionV1.deltaTableHistory.selectExpr("version", "operationMetrics.numOutputRows"))

// COMMAND ----------

val streamIngestionV2 = IoTDevicesIngestion(ver=2, StreamProcessing(Trigger.ProcessingTime("2 minutes")))
streamIngestionV2.run()

// COMMAND ----------

display(streamIngestionV2.deltaTableHistory.selectExpr("version", "operationMetrics.numOutputRows"))

// COMMAND ----------

val streamIngestionV3 = IoTDevicesIngestion(ver=3, 
                                            StreamProcessing(Trigger.ProcessingTime("2 minutes")),
                                            Map("spark.sql.streaming.noDataMicroBatches.enabled" -> "false")
                                                            )
streamIngestionV3.run()

// COMMAND ----------

display(streamIngestionV2.deltaTableHistory.selectExpr("version", "operationMetrics.numOutputRows"))

// COMMAND ----------

// %sql
// SET spark.databricks.delta.formatCheck.enabled=false
// --You are trying to write to `/databricks_lakehouse_cookbook/deltalake/iot/iot_devices/raw/ver=0` using Delta, but there is no
// --transaction log present. Check the upstream job to make sure that it is writing
// --using format("delta") and that you are trying to write to the table base path.
// --	at org.apache.spark.sql.execution.streaming.MicroBatchExecution.$anonfun$runBatch$16(MicroBatchExecution.scala:860)

// --To disable this check, SET spark.databricks.delta.formatCheck.enabled=false
