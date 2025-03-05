# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE CATALOG

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG cars_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE SCHEMA SILVER

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.silver;

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE SCHEMA GOLD

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA cars_catalog.gold;

# COMMAND ----------

