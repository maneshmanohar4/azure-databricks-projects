-- Databricks notebook source
DROP TABLE IF EXISTS bikestore.customers;
DROP TABLE IF EXISTS bikestore.orderitems;
DROP TABLE IF EXISTS bikestore.orders;
DROP TABLE IF EXISTS bikestore.products;
DROP TABLE IF EXISTS bikestore.staffs;
DROP TABLE IF EXISTS bikestore.stocks;
DROP TABLE IF EXISTS bikestore.stores;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/mnt/test/autoloader/checkpoint/",True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("/mnt/test/autoloader/checkpoint/")
