# Databricks notebook source
# MAGIC %run ./table_script

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

spark.sql("INSERT INTO "+catalog+"."+database+"."+tablename+" SELECT * FROM TempData")
