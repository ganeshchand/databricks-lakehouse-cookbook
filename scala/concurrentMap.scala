// Databricks notebook source
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

val events = new ConcurrentHashMap[String,String]()

// COMMAND ----------

// concurrent code
import scala.concurrent.Future
implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
val futures = Seq(
  Future {
    val thName = Thread.currentThread.getName
    val message = s"Running on Thread ${thName}"
    println(message)
    events.put(Thread.currentThread.getId.toString, message)
    Thread.sleep(1000 * 10)
  }
)

// COMMAND ----------

events.asScala.values.toList

// COMMAND ----------

futures.map(_.isCompleted)
