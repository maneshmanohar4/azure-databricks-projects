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
keycol=dbutils.widgets.get("keycol")
columnlist=dbutils.widgets.get("columnlist")
database="bikestore"
fileflag=True

# COMMAND ----------

try:
    data_df=spark.read.format("csv").option("header","true").load("abfss://"+containername+"@"+storageaccount+".dfs.core.windows.net/"+filename)
    data_df.createOrReplaceTempView("TempData")
except:
    fileflag=False

# COMMAND ----------

col_arr=columnlist.split(",")
update_stmt=""
for up_col in col_arr:
    update_stmt+="T."+up_col+"=S."+up_col+","
update_stmt=update_stmt[:-1]


insert_stmt=""
for in_col in col_arr:
    insert_stmt+="S."+in_col+","
insert_stmt=insert_stmt[:-1]

# COMMAND ----------

merge_query = f"""
MERGE INTO {database}.{tablename} AS T
USING TempData AS S
ON T.{keycol} = S.{keycol}
WHEN MATCHED THEN
  UPDATE SET {update_stmt}
WHEN NOT MATCHED THEN
  INSERT ({columnlist})
  VALUES ({insert_stmt})
"""
if fileflag:
  spark.sql(merge_query)

# COMMAND ----------

dbutils.notebook.exit(fileflag)
