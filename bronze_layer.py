# Databricks notebook source
orders_data = spark.read.table('samples.tpch.orders')
orders_data.show()

# COMMAND ----------

orders_data.schema

# COMMAND ----------

output_path = "dbfs:/delta/orders/"
orders_data.write.format("delta").save(output_path)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS my_database;
# MAGIC CREATE TABLE my_database.orders
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/orders';

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE hive_metastore.my_database.orders;
# MAGIC
# MAGIC OPTIMIZE hive_metastore.my_database.orders ZORDER BY (o_orderkey, o_orderdate, o_orderpriority);

# COMMAND ----------

spark.sql('SELECT * FROM hive_metastore.my_database.orders').show()

# COMMAND ----------


