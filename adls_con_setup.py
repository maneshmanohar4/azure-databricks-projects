# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
spark.conf.set("fs.azure.account.auth.type.dataenggstore.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dataenggstore.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dataenggstore.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2025-07-26T00:01:22Z&st=2024-02-27T16:01:22Z&spr=https&sig=72itLH6fXssjCaTR04M38eZeOroM2DgXWLKPV9rR5SY%3D")

# COMMAND ----------

dbutils.fs.ls('abfss://synapse@testexpstorageacc.dfs.core.windows.net/testdata/')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE creditcard_raw
# MAGIC USING CSV
# MAGIC OPTIONS (header = 'true', inferSchema = 'true')
# MAGIC LOCATION 'abfss://synapse@testexpstorageacc.dfs.core.windows.net/testdata/CreditCard.csv'

# COMMAND ----------

CreditCard=spark.read.csv('abfss://synapse@testexpstorageacc.dfs.core.windows.net/testdata/CreditCard.csv')
Customer=spark.read.csv('abfss://synapse@testexpstorageacc.dfs.core.windows.net/testdata/Customer.csv')


# COMMAND ----------

CreditCard.write.saveAsTable("CreditCard")

# COMMAND ----------

Customer.write.saveAsTable("Customer")

# COMMAND ----------

jdbc_url = ("jdbc:sqlserver://test-exp-sql-server.database.windows.net:1433;database=test-exp-db;"
"user=CloudSA787f302a@test-exp-sql-server;"
"password=M@n33shjc3@;"
"encrypt=true;"
"trustServerCertificate=false;"
"hostNameInCertificate=*.database.windows.net;"
"loginTimeout=30;")

# COMMAND ----------

df=spark.read.jdbc(
    url=jdbc_url, 
    table="dbo.[Producttest]"
)#.createOrReplaceTempView("test")

# COMMAND ----------

display(df)