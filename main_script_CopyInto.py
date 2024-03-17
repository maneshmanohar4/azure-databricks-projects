# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS bikestore;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bikestore.customers (
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
database="bikestore"
foldername="autoloader/"
schema="/mnt/test/schema/"


# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bikestore.customers
# MAGIC FROM 'abfss://azuredatastore@dataenggstore.dfs.core.windows.net/autoloader/'
# MAGIC FILEFORMAT = csv
# MAGIC FORMAT_OPTIONS ('mergeSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bikestore.customers
