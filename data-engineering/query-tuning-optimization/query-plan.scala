// Databricks notebook source
// MAGIC %md
// MAGIC ## Accessing query plan

// COMMAND ----------

val df = spark.range(10).union(spark.range(20)).join(spark.range(10), Seq("id"))

// COMMAND ----------

df.explain(extended=true)

// COMMAND ----------

df.queryExecution.toString // the full plan (parsed logical plan, analyzed logical plan, optimized logical plan and the physical plan). Same as df.explain(extended=true)

// COMMAND ----------

df.queryExecution.logical.toString
df.queryExecution.analyzed.toString
df.queryExecution.optimizedPlan.toString
df.queryExecution.executedPlan.toString


// COMMAND ----------

df.queryExecution.debug.toFile("/tmp/myqueryplan.txt")

// COMMAND ----------

dbutils.fs.head("/tmp/myqueryplan.txt")

// COMMAND ----------

spark.read.text("dbfs:/tmp/myqueryplan.txt").queryExecution.executedPlan.collect { case scan: org.apache.spark.sql.execution.DataSourceScanExec => 
  println(scan.metadata)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ## Codegen

// COMMAND ----------

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

// build catalyst expressions
val a = BoundReference(0, IntegerType, false)
val b = BoundReference(1, IntegerType, false)
val c = BoundReference(2, IntegerType, false)

val expr = Add(a, Multiply(b,c))

// generate code
val ctx = new CodegenContext
val exprCode = expr.genCode(ctx)

// COMMAND ----------

val df1 = Seq(1,2,3,1).toDF("id")
val df2 = Seq((1, "o"), (2, "t"), (3, "th")).toDF("id", "name")

// COMMAND ----------

df1.join(df2, Seq("id"), "left_outer").show(false)

// COMMAND ----------

df1.join(df2, Seq("id"), "inner").show(false)

// COMMAND ----------

// 2023-04-19T13:10:59.010Z
import java.time._
 val timestamp = java.sql.Timestamp.from(Instant.now.atZone(ZoneId.of("UTC")).toInstant)
 val test = ZonedDateTime.now(ZoneId.of("UTC"))
 java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(test)
