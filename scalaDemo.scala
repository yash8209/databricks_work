// Databricks notebook source
// MAGIC %md
// MAGIC **creating a list and dataframe in scala**

// COMMAND ----------

//this was magic command
val nums = List(1,2,3)
println(nums)

val data = Seq((1,"yash","male"),(2,"akshay","trans"))
val df = spark.createDataFrame(data).toDF("id","name","gender")
df.show()

// COMMAND ----------

val salesData = Seq(
  ("2024-01-01", "North", "Product A", 10, 200.0),
  ("2024-01-01", "South", "Product B", 5, 300.0),
  ("2024-01-02", "North", "Product A", 20, 400.0),
  ("2024-01-02", "South", "Product B", 10, 600.0),
  ("2024-01-03", "East",  "Product C", 15, 375.0)
)
val df = spark.createDataFrame(salesData).toDF("date", "region", "product", "quantity", "revenue")
df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC total revenue for each region 
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.functions._

df.groupBy("region")
  .agg(sum("revenue").alias("total_revenue"))
  .show()

