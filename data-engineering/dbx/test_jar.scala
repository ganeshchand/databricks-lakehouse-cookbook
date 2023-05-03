// Databricks notebook source
import com.gc.demo.databricks.dbx._

App.main(Array(""))

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC == Physical Plan ==
// MAGIC * Project (4)
// MAGIC +- * ColumnarToRow (3)
// MAGIC    +- PhotonResultStage (2)
// MAGIC       +- PhotonRange (1)
// MAGIC
// MAGIC
// MAGIC (1) PhotonRange
// MAGIC Output [1]: [id#40L]
// MAGIC Arguments: Range (0, 1, step=1, splits=4)
// MAGIC
// MAGIC (2) PhotonResultStage
// MAGIC Input [1]: [id#40L]
// MAGIC
// MAGIC (3) ColumnarToRow [codegen id : 1]
// MAGIC Input [1]: [id#40L]
// MAGIC
// MAGIC (4) Project [codegen id : 1]
// MAGIC Output [1]: [UDF() AS UDF()#46]
// MAGIC Input [1]: [id#40L]
// MAGIC
// MAGIC
// MAGIC == Photon Explanation ==
// MAGIC Photon does not fully support the query because:
// MAGIC 	Unsupported expression(s): UDF()
// MAGIC reference node:
// MAGIC 	Project [UDF() AS UDF()#46]
// MAGIC ```

// COMMAND ----------

spark.sql("CREATE TEMPORARY FUNCTION hello() RETURNS STRING RETURN 'Hello World!';")

// COMMAND ----------

 spark.range(1).selectExpr("hello()").show()

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC == Physical Plan ==
// MAGIC * ColumnarToRow (4)
// MAGIC +- PhotonResultStage (3)
// MAGIC    +- PhotonProject (2)
// MAGIC       +- PhotonRange (1)
// MAGIC
// MAGIC
// MAGIC (1) PhotonRange
// MAGIC Output [1]: [id#63L]
// MAGIC Arguments: Range (0, 1, step=1, splits=4)
// MAGIC
// MAGIC (2) PhotonProject
// MAGIC Input [1]: [id#63L]
// MAGIC Arguments: [Hello World! AS hello()#69]
// MAGIC
// MAGIC (3) PhotonResultStage
// MAGIC Input [1]: [hello()#69]
// MAGIC
// MAGIC (4) ColumnarToRow [codegen id : 1]
// MAGIC Input [1]: [hello()#69]
// MAGIC
// MAGIC
// MAGIC == Photon Explanation ==
// MAGIC The query is fully supported by Photon.
// MAGIC ```

// COMMAND ----------

import com.gc.demo.databricks.dbx._

App.main(Array(""))

// COMMAND ----------

// MAGIC %md
// MAGIC ```
// MAGIC == Physical Plan ==
// MAGIC * ColumnarToRow (4)
// MAGIC +- PhotonResultStage (3)
// MAGIC    +- PhotonProject (2)
// MAGIC       +- PhotonRange (1)
// MAGIC
// MAGIC
// MAGIC (1) PhotonRange
// MAGIC Output [1]: [id#2L]
// MAGIC Arguments: Range (0, 1, step=1, splits=4)
// MAGIC
// MAGIC (2) PhotonProject
// MAGIC Input [1]: [id#2L]
// MAGIC Arguments: [Hello World AS spark_catalog.default.hello()#8]
// MAGIC
// MAGIC (3) PhotonResultStage
// MAGIC Input [1]: [spark_catalog.default.hello()#8]
// MAGIC
// MAGIC (4) ColumnarToRow [codegen id : 1]
// MAGIC Input [1]: [spark_catalog.default.hello()#8]
// MAGIC
// MAGIC
// MAGIC == Photon Explanation ==
// MAGIC The query is fully supported by Photon.
// MAGIC ```
