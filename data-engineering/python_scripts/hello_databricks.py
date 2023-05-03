from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()
print("Databricks loves VS Code")

schema = StructType([
  StructField('CustomerId', IntegerType(), False),
  StructField('FirstName', StringType(), False),
  StructField('Lastname', StringType(), False)
])

data = [
  [1000, 'Ali', 'Ghodsi'],
  [1001, 'Matei', 'Zaharia']
]

rockstars = spark.createDataFrame(data, schema)
rockstars.show()