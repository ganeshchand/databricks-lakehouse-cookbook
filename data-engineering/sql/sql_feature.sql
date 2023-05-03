-- Databricks notebook source
select 1

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC
-- MAGIC Seq(
-- MAGIC   ("abc", false, 10),
-- MAGIC   ("def", true, 20),
-- MAGIC   ("abc", true, 30),
-- MAGIC   ("def", true, 50),
-- MAGIC   ("abc", true, 20)
-- MAGIC ).toDF("name", "completed", "distance")
-- MAGIC .createOrReplaceTempView("person")
-- MAGIC
-- MAGIC /* expected output
-- MAGIC
-- MAGIC   name      false_distance      true_distance
-- MAGIC   abc       10                  50 
-- MAGIC   def       0                   70
-- MAGIC */

-- COMMAND ----------

select name,
       sum(case when(completed='true') then distance else 0 end) as true_distance,
       sum(case when(completed='false') then distance else 0 end) as false_distance
from person
group by name

-- COMMAND ----------

select name, completed, distance from person
