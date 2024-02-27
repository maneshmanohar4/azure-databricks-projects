-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS azure_dataengg_adb.bikestore;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS azure_dataengg_adb.bikestore.customers (
customer_id int,
first_name varchar(150),
last_name varchar(150),
phone  varchar(150),
email varchar(250),
street varchar(150),
city varchar(150),
state varchar(150),
zip_code varchar(150));

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS azure_dataengg_adb.bikestore.orderitems(
order_id integer,
item_id integer,
product_id integer,
quantity integer,
list_price varchar(20),
discount varchar(20));


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS azure_dataengg_adb.bikestore.orders(
order_id integer,
customer_id integer,
order_status integer,
order_date date,
required_date date,
shipped_date date,
store_id integer,
staff_id integer);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS azure_dataengg_adb.bikestore.products(
product_id integer,
product_name varchar(200),
brand_id integer,
category_id integer,
model_year integer,
list_price varchar(200));

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS azure_dataengg_adb.bikestore.staffs(
staff_id integer,
first_name varchar(200),
last_name varchar(200),
email varchar(200),
phone varchar(200),
active integer,
store_id integer,
manager_id integer);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS azure_dataengg_adb.bikestore.stocks(
store_id integer,
product_id integer,
quantity integer);


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS azure_dataengg_adb.bikestore.stores(
store_id integer,
store_name varchar(200),
phone varchar(200),
email varchar(200),
street varchar(200),
city varchar(200),
state varchar(200),
zip_code varchar(200));

