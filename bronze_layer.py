# Databricks notebook source
orders_data = spark.read.table('samples.tpch.orders')
orders_data.show()

# COMMAND ----------

orders_data.schema

# COMMAND ----------


