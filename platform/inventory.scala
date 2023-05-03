// Databricks notebook source
display(dbutils.fs.ls("/"))

// COMMAND ----------

display(dbutils.fs.mounts())

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

import java.sql.Timestamp
import java.time._
import java.time.zone._

val zdtmUTC = ZonedDateTime.of(java.time.LocalDateTime.now(), ZoneId.of("UTC"))
val zdtmUTC1 = ZonedDateTime.of
val timestampFromZDT = Timestamp.valueOf(zdtmUTC.toLocalDateTime())

// COMMAND ----------

// val instantUTC = Instant.now().atZone(ZoneId.of("UTC"))
val instantUTC = Instant.now()
val zonedInstantUTC = Instant.now.atZone(ZoneId.of("UTC"))
println(Timestamp.from(instantUTC))
println(Timestamp.valueOf(zonedInstantUTC.toLocalDateTime))

// COMMAND ----------

import org.apache.spark.sql.catalyst._
val tableIdentifier = TableIdentifier("ade_events", None)
val tableMetadata = spark.sessionState.catalog.getTableMetadata(tableIdentifier)

// COMMAND ----------

tableMetadata.tableType.name

// COMMAND ----------

tableMetadata.provider

// COMMAND ----------

// MAGIC %sql
// MAGIC show tables

// COMMAND ----------

// MAGIC %sql
// MAGIC create table testbrokentable(id long)

// COMMAND ----------

// MAGIC %sql
// MAGIC desc extended testbrokentable

// COMMAND ----------

  import java.sql.Timestamp
  import java.time.{Instant, ZoneOffset, ZonedDateTime, ZoneId}
  import java.time.temporal.ChronoUnit

val millis = System.currentTimeMillis
val lastModifiedTimestampUTC: ZonedDateTime = Instant.ofEpochMilli(millis).atZone(ZoneId.of("UTC"))
val todayUTC: ZonedDateTime = Instant.now().atZone(ZoneId.of("UTC"))
ChronoUnit.DAYS.between(lastModifiedTimestampUTC, todayUTC)

// COMMAND ----------

val millis = -1
val lastModifiedTimestampUTC: ZonedDateTime = Instant.ofEpochMilli(millis).atZone(ZoneId.of("UTC"))
val todayUTC: ZonedDateTime = Instant.now().atZone(ZoneId.of("UTC"))
ChronoUnit.DAYS.between(lastModifiedTimestampUTC, todayUTC)

// COMMAND ----------

def currentEpochMilli: Long = Instant.now().toEpochMilli  
def daysBetweenMillis(begin: Long, end: Long = currentEpochMilli): Long = {
    val lastModifiedDateUTC: ZonedDateTime = Instant.ofEpochMilli(begin).atZone(ZoneId.of("UTC"))
    val todayDateUTC: ZonedDateTime = Instant.ofEpochMilli(end).atZone(ZoneId.of("UTC"))
   scala.math.abs(ChronoUnit.DAYS.between(lastModifiedDateUTC, todayDateUTC))
  }
def daysBetweenMillis1(begin: Long, end: Long = currentEpochMilli): Long = {
   scala.math.abs(ChronoUnit.DAYS.between(Instant.ofEpochMilli(begin), Instant.ofEpochMilli(end)))
  } 

// COMMAND ----------

daysBetweenMillis

// COMMAND ----------

val tenDaysAgo = ZonedDateTime.now.minusDays(10)
val today = ZonedDateTime.now

println(daysBetweenMillis(tenDaysAgo.toInstant.toEpochMilli, today.toInstant.toEpochMilli))
println(daysBetweenMillis1(tenDaysAgo.toInstant.toEpochMilli, today.toInstant.toEpochMilli))

println(daysBetweenMillis(today.toInstant.toEpochMilli, tenDaysAgo.toInstant.toEpochMilli))
println(daysBetweenMillis1(today.toInstant.toEpochMilli, tenDaysAgo.toInstant.toEpochMilli))
