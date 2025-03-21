# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSION MODEL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension - Branch_ID And BranchName

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Relative Columns

# COMMAND ----------

df_src = spark.sql('''
    select distinct(Dealer_ID)as Dealer_Id, DealerName 
    from parquet. `abfss://silver@salesdatalake1.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bring Only The Schema

# COMMAND ----------

#df_sink = spark.sql('''
#select 1 as dim_model_key, Model_Id, Model_Category
#from parquet.`abfss://silver@salesdatalake1.dfs.core.windows.net/carsales`
#where 1=0
#''')
#
#df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_Dealer_Sink - Initial and Incremental ( just bring the schema if the table NOT EXISTS)

# COMMAND ----------

if spark.catalog.tableExists('cars_catelog.gold.dim_dealer'):
    df_sink = spark.sql('''
    select dim_dealer_key, Dealer_Id, DealerName
    from parquet.`abfss://silver@salesdatalake1.dfs.core.windows.net/carsales`
''')
else:
    df_sink = spark.sql('''
    select 1 as dim_dealer_key, Dealer_Id, DealerName
    from parquet.`abfss://silver@salesdatalake1.dfs.core.windows.net/carsales`
    where 1=0
    ''')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering New Records and Old Records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Dealer_Id'] == df_sink['Dealer_Id'], 'left').select(df_src['Dealer_Id'], df_src['DealerName'], df_sink['dim_dealer_key'])

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Df Filter Old
# MAGIC
# MAGIC

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_dealer_key').isNotNull())
df_filter_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Df Filter New

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_dealer_key').isNull()).select(df_src['Dealer_Id'], df_src['DealerName'])
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE SURROGATE KEY

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch The Max Surrogate Key From Existing Table

# COMMAND ----------

if (incremental_flag == '0'):
    max_value = 1
else:
    max_value = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value = max_value.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key Column And Add Max Surrogate Key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_value + monotonically_increasing_id())
df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DF - Df Filter Old + Df Filter New

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)
df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Slowing Changing Dimension

# COMMAND ----------

# MAGIC %md
# MAGIC ### SCD TYPE - 1 (UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

# Incremental RUN
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@salesdatalake1.dfs.core.windows.net/dim_dealer')
    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_dealer_key = src.dim_dealer_key').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Initial RUN
else:
    df_final.write.format('delta').mode('overwrite').option("path",'abfss://gold@salesdatalake1.dfs.core.windows.net/dim_dealer').saveAsTable('cars_catalog.gold.dim_dealer')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer

# COMMAND ----------

