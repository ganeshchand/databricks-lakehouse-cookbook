# Databricks notebook source
import dlt

# COMMAND ----------

@dlt.create_table(
  comment="This is a rate based streaming DLT table",
  path="s3://db-gc-lakehouse/dlt_features/multi_target_schema/bronze/rate_1/",  
  table_properties={
    "quality": "bronze",
    "pipelines.metastore.tableName": "gc_dlt_multi_target_schema.rate1"
  },
)
def rate1():
  return (
   spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)  
    .load()
  )
  
@dlt.create_table(
  comment="This is a rate based streaming DLT table",
  path="s3://db-gc-lakehouse/dlt_features/multi_target_schema/bronze/rate_2/",  
  table_properties={
    "quality": "bronze",
    "pipelines.metastore.tableName": "gc_dlt_multi_target_schema.rate2"
  },
)
def rate2():
  return (
   spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1)  
    .load()
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
# MAGIC                 "path": "/Repos/ganesh@databricks.com/databricks-lakehouse-cookbook/data-engineering/dlt/multi_target_schema_pipeline1"
# MAGIC               
# MAGIC             }
# MAGIC         }
# MAGIC     ],
# MAGIC     "name": "gc_dlt_migration_source",
# MAGIC     "storage": "s3://db-gc-lakehouse/dlt_features/multi_target_schema_pipeline1",
# MAGIC }```

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from gc_dlt_multi_target_schema
