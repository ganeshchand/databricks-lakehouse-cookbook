// Databricks notebook source
import org.apache.spark.sql.{ DataFrame, Row, ForeachWriter }

spark
  .readStream
  .format("rate-micro-batch")
  .option("rowsPerBatch", 10)
  .option("numPartitions",1)
  .load
  .writeStream
  .foreach(new ForeachWriter[Row] {
            override def open(partitionId: Long, epochId: Long): Boolean = {
          println(s"partitionId: $partitionId, epochId: $epochId")
          true
        }

        override def process(value: Row): Unit = value match {
          case Row(timestamp: Long, value: Long) =>
            println(s"timestamp: $timestamp, value: $value")
        }

        override def close(errorOrNull: Throwable): Unit = {
          println(s"errorOrNull: $errorOrNull")
        }
  })
  .queryName("foreach-writer-demo")
  .format("console")
  .start()
  .awaitTermination()
