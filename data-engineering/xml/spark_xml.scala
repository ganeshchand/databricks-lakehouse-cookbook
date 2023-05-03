// Databricks notebook source
// MAGIC %sh wget https://github.com/databricks/spark-xml/raw/master/src/test/resources/books.xml

// COMMAND ----------

// MAGIC %sh cp books.xml /dbfs/databricks_lakehouse_cookbook/data/xml/books.xml

// COMMAND ----------

import com.databricks.spark.xml._ 

val df = spark.read
  .option("rowTag", "book")
  .xml("dbfs:/databricks_lakehouse_cookbook/data/xml/books.xml")

// COMMAND ----------

display(df)

// COMMAND ----------

dbutils.fs.put("dbfs:/databricks_lakehouse_cookbook/data/xml/basket.xsd","""<?xml version="1.0" encoding="UTF-8" ?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="basket">
    <xs:complexType>
      <xs:sequence>
        <xs:element name="entry" minOccurs="0" maxOccurs="unbounded">
          <xs:complexType>
            <xs:sequence>
              <xs:element name="key" minOccurs="0" type="xs:anyType"/>
              <xs:element name="value" minOccurs="0" type="xs:anyType"/>
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:sequence>
    </xs:complexType>
  </xs:element>
</xs:schema>""")

// COMMAND ----------

import com.databricks.spark.xml.util.XSDToSchema
import java.nio.file.Paths

val schema = XSDToSchema.read(Paths.get("dbfs:/databricks_lakehouse_cookbook/data/xml/basket.xsd"))
// val df = spark.read.schema(schema)....xml(...)

// COMMAND ----------



// COMMAND ----------

import java.nio.file.Paths
import com.databricks.spark.xml.util.XSDToSchema
import org.apache.ws.commons.schema.XmlSchemaCollection
XSDToSchema.read(Paths.get("/dbfs/databricks_lakehouse_cookbook/data/xml/basket.xsd"))

// COMMAND ----------

def executeAndPrintTime[T](f: => T): T = {
  val startTime = Instant.now()
  val result = f
  val endTime = Instant.now()
  println(s"Executed in ${endTime.toEpochMilli - startTime.toEpochMilli} milliseconds")
  result
}

// COMMAND ----------

def myFun() = Thread.sleep(1000)

// COMMAND ----------

executeAndPrintTime(myFun)

// COMMAND ----------

import java.time._
case class ExecuteAndPrintTime[R](f: () => R) {
  val startTime = Instant.now()
  val result = f
  val endTime = Instant.now()
  override def toString() = s"Executed in ${endTime.toEpochMilli - startTime.toEpochMilli} milliseconds"
}
implicit def blockToThunk[R](bl: => R) = () => bl //helps to call Timed without the thunk syntax

// COMMAND ----------

val t1 = ExecuteAndPrintTime(myFun())

// COMMAND ----------

t1.f

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC spark \
// MAGIC   .readStream \
// MAGIC   .format("rate") \
// MAGIC   .load() \
// MAGIC   .writeStream \
// MAGIC   .outputMode("append") \
// MAGIC   .format("console") \
// MAGIC   .start()

// COMMAND ----------

import scala.util.Try
val workspaceId = "2556758628403379"
val result = dbutils.fs.mounts.map { m =>
      val isDatabricksInternal: Option[Boolean] = Try {
        m.mountPoint.startsWith("/databricks-datasets") ||
          m.mountPoint.startsWith("/databricks-results") ||
          m.mountPoint.startsWith("/databricks/mlflow-tracking") ||
          m.mountPoint.startsWith("/databricks/mlflow-registry")
      }.toOption
      val toBeIgnored: Option[Boolean] = isDatabricksInternal.map(_ == true)
      val encryptionType = if (Option(m.encryptionType).getOrElse("").isEmpty) None else Some(m.encryptionType)
      val id = s"${workspaceId}_${m.mountPoint}"
      val isBroken = Try(dbutils.fs.ls(m.mountPoint).take(1)).isSuccess 
    (m, isBroken, isDatabricksInternal, toBeIgnored, encryptionType, id)
    }
display(result.toDF)

// COMMAND ----------

dbutils.fs.ls("dbfs:/").take(1)

// COMMAND ----------

case class Customer(customerId: Int, firstName: String, lastName: String, email: String, phone: Option[String])
object Customer {
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.Encoders
  val SCHEMA: StructType = Encoders.product[Customer].schema
}

// COMMAND ----------

Customer.SCHEMA

// COMMAND ----------

val englishCustomers = Seq(
  Customer(1, "wayne", "rooney", "wayne.rooney@zmail.com", Some("111-111-1234")),
  Customer(2, "harry", "kane", "harry.kane@zmail.com", Some("111-111-2345"))
)

val argentineCustomers = Seq(
  Customer(3, "leo", "messi", "leo.messi@zmail.com", Some("111-222-1234")),
  Customer(4, "diego", "maradona", "diego.maradona@zmail.com", None),
  Customer(4, "diego", "maradona", "diego.maradona@zmail.com", None)
)

val englishCustomersDS = englishCustomers.toDS
val argentineCustomersDS = argentineCustomers.toDS

// COMMAND ----------

display(englishCustomersDS)

// COMMAND ----------

display(argentineCustomersDS)

// COMMAND ----------

/*

dataset.union(anotherDataset) Returns a new Dataset containing union of rows in this Dataset and another Dataset.
This is equivalent to UNION ALL in SQL. To do a SQL-style set union (that does deduplication of elements), use this function followed by a distinct.
Also as standard in SQL, this function resolves columns by position (not by name):

*/
val engUnionArg = englishCustomersDS.union(argentineCustomersDS)
display(engUnionArg)

// COMMAND ----------

// UnionAll is just an alias for union
val engUnionAllArg = englishCustomersDS.unionAll(argentineCustomersDS)
display(engUnionAllArg)

// COMMAND ----------

englishCustomersDS.createOrReplaceTempView("english_customers")
argentineCustomersDS.createOrReplaceTempView("argentine_customers")

// COMMAND ----------

// SQL UNION  - union by column position and does the de-duplication
display(
  spark.sql("select * from english_customers UNION select * from argentine_customers")
  )

// COMMAND ----------

// SQL UNION ALL  - union by column position and does not de-duplicate
display(
  spark.sql("select * from english_customers UNION ALL select * from argentine_customers")
  )

// COMMAND ----------

// MAGIC %sql
// MAGIC select 1 as idc
// MAGIC UNION
// MAGIC select 1 as id, current_date as today
// MAGIC UNION
// MAGIC select 2 as id, current_date as today
// MAGIC UNION
// MAGIC select null as id, null as today

// COMMAND ----------

// MAGIC %sql
// MAGIC select cast(1 as int) as id, current_date as today
// MAGIC UNION
// MAGIC select cast(2 as long) as id, current_date as today
// MAGIC UNION
// MAGIC select cast(3 as string) as id, current_date as today

// COMMAND ----------

spark.sql("""select cast(1 as int) as id, current_date as today
UNION
select cast(2 as long) as id, current_date as today
UNION
select cast(3 as string) as id, current_date as today""").printSchema

// COMMAND ----------


