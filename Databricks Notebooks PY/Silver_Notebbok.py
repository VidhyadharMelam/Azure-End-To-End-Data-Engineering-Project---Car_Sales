# Databricks notebook source
# MAGIC %md
# MAGIC # DATA READING

# COMMAND ----------

df = spark.read.format('Parquet')\
            .option('inferSchema', 'true')\
            .load('abfss://bronze@salesdatalake1.dfs.core.windows.net/rawdata')                   

# COMMAND ----------

# DISPLAY THE DATA
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA TRANSFORMATION

# COMMAND ----------

# import all libraries
from pyspark.sql.functions import *

# COMMAND ----------

# We have 'Model_ID' column in which data is divided by hyphen. We will split the data and create new column 'Model_Category' in which only the first element of the data is stored. 

df =df.withColumn('Model_Category',split(col('Model_ID'),'-')[0])
df.display()

# COMMAND ----------

# We have 'Model_ID' column in which data is divided by hyphen. We will split the data and create new column 'Model_Name' in which only the second element of the data is stored. 

df =df.withColumn('Model_Name',split(col('Model_ID'),'-')[1])
df.display()

# COMMAND ----------

# Create a new column; Revenue Per Unit Sold

df= df.withColumn('Revenue_Per_Unit_Sold', col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC ANALYSIS

# COMMAND ----------

# Aggregations: How many units each branch name sold every year and sort the year to ascending and total units to desending.

df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units_Sold')).sort(col('Year').asc(), col('Total_Units_Sold').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA WRITING TO SILVER LAYER

# COMMAND ----------

df.write.format('Parquet')\
        .mode('overwrite')\
        .option('path', 'abfss://silver@salesdatalake1.dfs.core.windows.net/carsales')\
        .save()