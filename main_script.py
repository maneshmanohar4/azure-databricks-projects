# Databricks notebook source
# MAGIC %run ./create_table_script

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dataenggstore.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dataenggstore.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dataenggstore.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-07-26T00:01:22Z&st=2024-02-27T16:01:22Z&spr=https&sig=72itLH6fXssjCaTR04M38eZeOroM2DgXWLKPV9rR5SY%3D")

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
