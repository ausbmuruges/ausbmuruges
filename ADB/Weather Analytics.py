# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT logicalenv DEFAULT "prd001";

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables FROM im${logicalenv}_rawvault like '*inc*'

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/imlake/staging/DOMS/INC_INCIDENT/
