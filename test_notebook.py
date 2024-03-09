# Databricks notebook source
# MAGIC %run ./drop_table_script

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikestore.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikestore.orderitems where order_id=1
