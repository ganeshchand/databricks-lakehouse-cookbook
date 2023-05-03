// Databricks notebook source
Seq((1, "ganesh", "SF")).toDF("id", "name", "location").write.format("delta").save("dbfs:/tmp/gc/de/dlt/cdf/source_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE delta.`dbfs:/tmp/gc/de/dlt/cdf/source_table` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history delta.`dbfs:/tmp/gc/de/dlt/cdf/source_table`

// COMMAND ----------

Seq((2, "ravi", "SF")).toDF("id", "name", "location")
.write.format("delta").mode("append").save("dbfs:/tmp/gc/de/dlt/cdf/source_table")

// COMMAND ----------

Seq((1, "ganesh", "SF")).toDF("id", "name", "location")
.write.format("delta").mode("append").save("dbfs:/tmp/gc/de/dlt/cdf/source_table")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from gc_lakehouse.cdf_target_table

// COMMAND ----------

// MAGIC %sql
// MAGIC update  delta.`dbfs:/tmp/gc/de/dlt/cdf/source_table`
// MAGIC SET location = 'PINOLE'
// MAGIC where id=1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from gc_lakehouse.cdf_target_table

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM delta.`dbfs:/tmp/gc/de/dlt/cdf/source_table`
// MAGIC where id=2

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from gc_lakehouse.cdf_target_table

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW cdf_target_table_latest_version as
// MAGIC SELECT * 
// MAGIC     FROM 
// MAGIC          (SELECT *, rank() over (partition by id order by _commit_version desc) as rank
// MAGIC           FROM table_changes('gc_lakehouse.cdf_target_table', 2, 5)
// MAGIC           WHERE _change_type !='update_preimage')
// MAGIC     WHERE rank=1
