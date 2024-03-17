# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bikestore;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bikestore.customers_bronze (
# MAGIC customer_id varchar(50),
# MAGIC first_name varchar(150),
# MAGIC last_name varchar(150),
# MAGIC phone  varchar(150),
# MAGIC email varchar(250),
# MAGIC street varchar(150),
# MAGIC city varchar(150),
# MAGIC state varchar(150),
# MAGIC zip_code varchar(150));

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bikestore.customers_silver (
# MAGIC customer_id varchar(50),
# MAGIC first_name varchar(150),
# MAGIC last_name varchar(150),
# MAGIC phone  varchar(150),
# MAGIC email varchar(250),
# MAGIC street varchar(150),
# MAGIC city varchar(150),
# MAGIC state varchar(150),
# MAGIC zip_code varchar(150),
# MAGIC time_stamp varchar(150));

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bikestore.customers_gold (
# MAGIC customer_id varchar(50),
# MAGIC first_name varchar(150),
# MAGIC last_name varchar(150),
# MAGIC phone  varchar(150),
# MAGIC email varchar(250),
# MAGIC street varchar(150),
# MAGIC city varchar(150),
# MAGIC state varchar(150),
# MAGIC zip_code varchar(150));

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.dataenggstore.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dataenggstore.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dataenggstore.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-07-26T00:01:22Z&st=2024-02-27T16:01:22Z&spr=https&sig=72itLH6fXssjCaTR04M38eZeOroM2DgXWLKPV9rR5SY%3D")

# COMMAND ----------

containername="azuredatastore"
storageaccount="dataenggstore"
tablename="customers"
bronzetablename="customers_bronze"
silvertablename="customers_silver"
goldtablename="customers_gold"
database="bikestore"
foldername="autoloader/"
schema="/mnt/test/schema/"


# COMMAND ----------


    (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv") 
    .option("cloudFiles.schemaLocation",schema) 
    .load(f"abfss://{containername}@{storageaccount}.dfs.core.windows.net/{foldername}")
    .createOrReplaceTempView("customers_raw_temp"))


# COMMAND ----------

(spark.table("customers_raw_temp")
.writeStream
.format("delta")
.option("checkpointLocation","/mnt/test/multihop_bronze/checkpoint")
.option("mergeSchema", "true")
.outputMode("append")
.table(f"{database}.{bronzetablename}"))

# COMMAND ----------

spark.readStream.table(f"{database}.{bronzetablename}").createOrReplaceTempView("customer_bronze_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_bronze_tmp_final AS(
# MAGIC   SELECT *
# MAGIC   FROM customer_bronze_tmp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_bronze_tmp_final

# COMMAND ----------

(spark.table("customer_bronze_tmp_final")
.writeStream
.format("delta")
.option("checkpointLocation","/mnt/test/multihop_silver/checkpoint")
.option("mergeSchema", "true")
.outputMode("append")
.table(f"{database}.{silvertablename}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikestore.customers_silver
