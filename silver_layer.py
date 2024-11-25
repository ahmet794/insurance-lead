# Databricks notebook source
df = spark.read.table("hive_metastore.my_database.orders")
df.show()

# COMMAND ----------

df.summary().show()

# COMMAND ----------

row_count = df.count()
row_count

# COMMAND ----------

from pyspark.sql.functions import col, to_date

regex_pattern = r"^\d{4}-\d{2}-\d{2}$"
df_with_checks = df.filter(col("o_orderdate").rlike(regex_pattern))

# COMMAND ----------

df_with_checks.count()

# COMMAND ----------

df_with_checks.show()

# COMMAND ----------

# MAGIC %md
# MAGIC It can be observed that all of the rows have the same date format.

# COMMAND ----------

df_priority = df

df_priority.groupBy("o_orderpriority").count().show()

# COMMAND ----------

from pyspark.sql.functions import when, col

df_priority = df_priority.withColumn(
    "o_orderpriority",
    when(
        col("o_orderpriority").isin("1-URGENT", "2-HIGH"),
        "High Priority"
    )
    .when(
        col("o_orderpriority").isin("3-MEDIUM", "5-LOW"), "Low Priority"
    )
    .otherwise(col("o_orderpriority"))
)

# COMMAND ----------

df_priority.groupBy("o_orderpriority").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC The following transformation transforms the priority status to two easy categories.

# COMMAND ----------

df = df_priority

# COMMAND ----------

df_orderstatus = df
df_orderstatus.groupBy("o_orderstatus").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC The orderstatus column does not have invalid values.

# COMMAND ----------

from pyspark.sql.functions import year, lit, quarter, concat

df_quarter = df
df_quarter = df_quarter.withColumn(
  "o_quarter",
  concat(year(col("o_orderdate")), 
         lit("-Q"), 
         quarter(col("o_orderdate")))
)

# COMMAND ----------

df_quarter.groupBy("o_quarter").count().orderBy("o_quarter", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Year Quarter business logic is applied.

# COMMAND ----------


