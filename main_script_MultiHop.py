# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %run ./drop_table_script
# MAGIC

# COMMAND ----------

# MAGIC %run ./multi_hop_create_table_script

# COMMAND ----------

# MAGIC %run ./adls_con_setup

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

# MAGIC %sql 
# MAGIC select * from customers_raw_temp

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

(spark.table("customer_bronze_tmp_final")
.writeStream
.format("delta")
.option("checkpointLocation","/mnt/test/multihop_silver/checkpoint")
.option("mergeSchema", "true")
.outputMode("append")
.table(f"{database}.{silvertablename}"))

# COMMAND ----------

spark.readStream.table(f"{database}.{silvertablename}").createOrReplaceTempView("customer_silver_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW customer_silver_tmp_final AS (
# MAGIC SELECT *
# MAGIC FROM customer_silver_tmp)

# COMMAND ----------

(spark.table("customer_silver_tmp_final")
.writeStream
.format("delta")
.option("checkpointLocation","/mnt/test/multihop_gold/checkpoint")
.option("mergeSchema","true")
.outputMode("append")
.table(f"{database}.{goldtablename}"))

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from bikestore.customers_gold
