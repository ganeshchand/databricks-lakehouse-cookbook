# Databricks notebook source
topic = "de"
sub_topic = "dlt"
feature = "stream-static-join"

# COMMAND ----------

# MAGIC %scala
# MAGIC Seq((1, "USA"), (2, "Nepal")).toDF("id", "country")
# MAGIC .write
# MAGIC .save("/tmp/ganesh/delta/merge/example1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`/tmp/ganesh/delta/merge/example1`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gc_delta_merge_example
# MAGIC using delta
# MAGIC location '/tmp/ganesh/delta/merge/example1'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gc_delta_merge_example

# COMMAND ----------

# MAGIC %scala
# MAGIC Seq((1, "US"), (2, "Nepal")).toDF("id", "country").createOrReplaceTempView("updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC   MERGE INTO gc_delta_merge_example t
# MAGIC   USING updates s
# MAGIC   ON s.id = t.id
# MAGIC   WHEN MATCHED THEN UPDATE SET *

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gc_delta_merge_example

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gc_delta_merge_example
