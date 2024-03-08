# Databricks notebook source
# MAGIC %run ./drop_table_script

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from azure_dataengg_adb.bikestore.orderitems

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from azure_dataengg_adb.bikestore.orders
