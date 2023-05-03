// Databricks notebook source
// MAGIC %md
// MAGIC ## Merge Map column types

// COMMAND ----------

import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

def do_upsert(updatesDF: DataFrame, tablePath: String): Unit = {
  val deltaTable = DeltaTable.forPath(spark, tablePath)
  deltaTable
    .as("target")
    .merge(
      updatesDF.as("updates"),
      "target.id = updates.id")
    .whenMatched
    .updateExpr(
      Map(
        "id" -> "updates.id",
        "reader_options" -> "updates.reader_options"        
      ))
    .whenNotMatched
    .insertExpr(
      Map(
        "id" -> "updates.id",
        "reader_options" -> "updates.reader_options"
      ))
    .execute()
}

// COMMAND ----------

val initialData = Seq(
  (1, Map(("k1" -> "v1"), ("k2" -> "v2")))
  ).toDF("id", "reader_options")

// COMMAND ----------

display(initialData)

// COMMAND ----------

val tablePath = "/gc/tmp/delta/features/merge_map_col"
initialData.write.format("delta").save(tablePath)

// COMMAND ----------

val updates = Seq(
  (2, Map(("k4" -> "v1"))))
.toDF("id", "reader_options")

do_upsert(updates, tablePath)

// COMMAND ----------

display(io.delta.tables.DeltaTable.forPath(tablePath).toDF)

// COMMAND ----------

val updates = Seq(
  (2, Map(("k4" -> "v4"),("k2" -> "V2")))) // add new key and update value for existing key
.toDF("id", "reader_options")

do_upsert(updates, tablePath)

// COMMAND ----------

display(io.delta.tables.DeltaTable.forPath(tablePath).toDF)

// COMMAND ----------

val updates = Seq(
  (2, Map(("k4" -> "v4"),("k2" -> "V2")))) // add new key and update value for existing key
.toDF("id", "reader_options")

do_upsert(updates, tablePath)
