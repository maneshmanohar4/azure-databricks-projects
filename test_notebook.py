# Databricks notebook source
# MAGIC %run ./drop_table_script

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikestore.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikestore.orderitems where order_id=1

# COMMAND ----------

# MAGIC %run /Workspace/Shared/adls_con_setup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS external_table
# MAGIC   USING DELTA
# MAGIC   LOCATION "abfss://azuredatastore@dataenggstore.dfs.core.windows.net/test/test"

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended external_table
