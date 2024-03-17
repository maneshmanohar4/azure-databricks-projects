-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS bikestore;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bikestore.customers_bronze (
customer_id varchar(50),
first_name varchar(150),
last_name varchar(150),
phone  varchar(150),
email varchar(250),
street varchar(150),
city varchar(150),
state varchar(150),
zip_code varchar(150));

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bikestore.customers_silver (
customer_id varchar(50),
first_name varchar(150),
last_name varchar(150),
phone  varchar(150),
email varchar(250),
street varchar(150),
city varchar(150),
state varchar(150),
zip_code varchar(150));

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bikestore.customers_gold (
customer_id varchar(50),
first_name varchar(150),
last_name varchar(150),
phone  varchar(150),
email varchar(250),
street varchar(150),
city varchar(150),
state varchar(150),
zip_code varchar(150));
