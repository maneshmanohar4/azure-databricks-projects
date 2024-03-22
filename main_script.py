# Databricks notebook source
# MAGIC %run ./create_table_script

# COMMAND ----------

# MAGIC %run ./adls_con_setup

# COMMAND ----------

filename=dbutils.widgets.get("filename")
containername=dbutils.widgets.get("containername")
storageaccount=dbutils.widgets.get("storageaccount")
tablename=dbutils.widgets.get("tablename")
catalog="azure_dataengg_adb"
database="bikestore"

# COMMAND ----------

data_df=spark.read.format("csv").option("header","true").load("abfss://"+containername+"@"+storageaccount+".dfs.core.windows.net/"+filename)
data_df.createOrReplaceTempView("TempData")

# COMMAND ----------

spark.sql("INSERT INTO "+database+"."+tablename+" SELECT * FROM TempData")
