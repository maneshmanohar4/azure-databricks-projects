# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %run /Workspace/Shared/adls_con_setup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customers_raw AS
# MAGIC SELECT * FROM cloud_files("abfss://azuredatastore@dataenggstore.dfs.core.windows.net/autoloader/","csv",map("schema","customer_id STRING,first_name STRING,last_name STRING,phone  STRING,email STRING,street STRING,city STRING,state STRING,zip_code STRING"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customers_silver AS
# MAGIC SELECT t.customer_id,t.first_name,t.last_name,t.phone,t.email,t.street,t.city,t.state,t.zip_code
# MAGIC FROM STREAM(LIVE.customers_raw) t

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE customers_gold AS
# MAGIC SELECT customer_id,first_name,last_name,phone,email,street,city,state,zip_code
# MAGIC FROM LIVE.customers_silver
