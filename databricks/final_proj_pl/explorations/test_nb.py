# Databricks notebook source
spark.sql("USE CATALOG `workspace`")
spark.sql("USE SCHEMA `imdb`")

# COMMAND ----------

display(spark.sql("SELECT * FROM samples.wanderbricks.users"))
