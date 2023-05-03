# Databricks notebook source
import dlt
from pyspark.sql.functions import expr

# COMMAND ----------

@dlt.create_table(
  comment="This is a rate based streaming DLT table",
  path="s3://db-gc-lakehouse/dlt_features/multiplexing/bronze/pattern1/rate_source/",  
  table_properties={
    "quality": "bronze",
    "pipelines.metastore.tableName": "gc_dlt_multiplexing.rate_source"
  },
  partition_cols=["table"]
)
def rate_source():
  return (
   spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)  
    .load()
    .withColumn("table", expr("case when value % 2 == 0 then 'even' else 'odd' end"))
  )
  
@dlt.create_table(
  comment="table containing even numbers",
  path="s3://db-gc-lakehouse/dlt_features/multiplexing/silver/even/",  
  table_properties={
    "quality": "silver",
    "pipelines.metastore.tableName": "gc_dlt_multiplexing.even"
  },
)
def even():
  return (
   dlt.read_stream("rate_source").where("table = 'even'")
  )
  
@dlt.create_table(
  comment="table containing odd numbers",
  path="s3://db-gc-lakehouse/dlt_features/multiplexing/silver/odd/",  
  table_properties={
    "quality": "silver",
    "pipelines.metastore.tableName": "gc_dlt_multiplexing.odd"
  },
)
def odd():
  return (
   dlt.read_stream("rate_source").where("table = 'odd'")
  )   

# COMMAND ----------

@dlt.create_view(
  comment="View representing streaming rate source"
)
def rate_source_v():
  return (
   spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)  
    .load()
    .withColumn("table", expr("case when value % 2 == 0 then 'even' else 'odd' end"))
  )
  
@dlt.create_table(
  comment="table containing even numbers",
  path="s3://db-gc-lakehouse/dlt_features/multiplexing/silver/even1/",  
  table_properties={
    "quality": "silver",
    "pipelines.metastore.tableName": "gc_dlt_multiplexing.even1"
  },
)
def even1():
  return (
   dlt.read_stream("rate_source_v").where("table = 'even'")
  )
  
@dlt.create_table(
  comment="table containing odd numbers",
  path="s3://db-gc-lakehouse/dlt_features/multiplexing/silver/odd1/",  
  table_properties={
    "quality": "silver",
    "pipelines.metastore.tableName": "gc_dlt_multiplexing.odd1"
  },
)
def odd1():
  return (
   dlt.read_stream("rate_source_v").where("table = 'odd'")
  )   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes:
# MAGIC
# MAGIC * The table property pipelines.metastore.tableName (set on 'rate1') cannot be set if the pipeline contains the 'target' schema 'multi_target_schema_pipeline_xyz'. Please remove the table property or the target database.

# COMMAND ----------

# MAGIC %md
# MAGIC ```json
# MAGIC {
# MAGIC     "clusters": [
# MAGIC         {
# MAGIC             "label": "default",
# MAGIC             "aws_attributes": {
# MAGIC               "instance_profile_arn": "arn:aws:iam::997819012307:instance-profile/shard-demo-s3-access"
# MAGIC        
# MAGIC             },
# MAGIC             "policy_id": "E06216CAA0000360",
# MAGIC             "num_workers": 1
# MAGIC         }
# MAGIC     ],
# MAGIC     "development": true,
# MAGIC     "continuous": true,
# MAGIC     "channel": "PREVIEW",
# MAGIC     "edition": "ADVANCED",
# MAGIC     "photon": false,
# MAGIC     "libraries": [
# MAGIC         {
# MAGIC             "notebook": {
# MAGIC                 "path": "/Repos/ganesh@databricks.com/databricks-lakehouse-cookbook/data-engineering/dlt/multiplexing_pipeline1"
# MAGIC               
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "name": "gc_dlt_multiplexing",
# MAGIC     "storage": "s3://db-gc-lakehouse/dlt_features/multiplexing_pipeline1",
# MAGIC }```

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from gc_dlt_multiplexing

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gc_dlt_multiplexing.even1

# COMMAND ----------


