# Databricks notebook source
iot_data_path = "/databricks_lakehouse_cookbook/data/iot/iot_devices/format=parquet/"

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "4")

# COMMAND ----------

from pyspark.sql.functions import *

dlt_cache_test_raw_iot = (
          spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", "/databricks_lakehouse_cookbook/bronze/iot/iot_devices/jobs/ss/cache_test/ver=1/_schemaLocation/")
            .load(iot_data_path)
            .select("*", "_metadata")
)

# COMMAND ----------

(
  dlt_cache_test_raw_iot
  .where("device_id % 2 == 0")
  .writeStream.format("delta")
  .option("checkpointLocation", "/databricks_lakehouse_cookbook/bronze/iot/iot_devices/jobs/ss/cache_test/ver=1/dlt_cache_test_even/_checkpoint")
  .trigger(once=True)
  .start("/databricks_lakehouse_cookbook/bronze/iot/iot_devices/jobs/ss/cache_test/ver=1/dlt_cache_test_even/")
)

# COMMAND ----------

(
  dlt_cache_test_raw_iot
  .where("device_id % 2 == 0")
  .writeStream.format("delta")
  .option("checkpointLocation", "/databricks_lakehouse_cookbook/bronze/iot/iot_devices/jobs/ss/cache_test/ver=1/dlt_cache_test_odd/_checkpoint")
  .trigger(once=True)
  .start("/databricks_lakehouse_cookbook/bronze/iot/iot_devices/jobs/ss/cache_test/ver=1/dlt_cache_test_odd/")
)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *

def saveToDelta(df, epochId):
  cached_df = df.cache()
  
  (
    cached_df
    .where("device_id % 2 == 0")
    .write
    .format("delta")
    .mode('append')
    .save("/databricks_lakehouse_cookbook/bronze/iot/iot_devices/jobs/ss/cache_test/ver=2/dlt_cache_test_even/")
  )
  
  (
    cached_df
    .where("device_id % 2 <> 0")
    .write
    .format("delta")
    .mode('append')
    .save("/databricks_lakehouse_cookbook/bronze/iot/iot_devices/jobs/ss/cache_test/ver=2/dlt_cache_test_odd/")
  )

# COMMAND ----------

(
  dlt_cache_test_raw_iot
  .writeStream  
  .option("checkpointLocation", "/databricks_lakehouse_cookbook/bronze/iot/iot_devices/jobs/ss/cache_test/ver=2/dlt_cache_test_raw_iot/_checkpoint")
  .trigger(once=True)
  .foreachBatch(saveToDelta)
  .queryName("cache_test_v2")
  .start()
)
