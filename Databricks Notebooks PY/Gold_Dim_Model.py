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

# Dimension - Model_ID And Model_Category

# COMMAND ----------

df_src = spark.sql('''
    select distinct(Model_ID)as Model_Id, Model_Category 
    from parquet. `abfss://silver@salesdatalake1.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bring Only The Schema

# COMMAND ----------

df_sink = spark.sql('''
select 1 as dim_model_key, Model_Id, Model_Category
from parquet.`abfss://silver@salesdatalake1.dfs.core.windows.net/carsales`
where 1=0
''')

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim_Model_Sink - Initial and Incremental

# COMMAND ----------

if spark.catalog.tableExists('cars_catelog.gold.dim_model'):
    df_sink = spark.sql('''
    select dim_model_key, Model_Id, Model_Category
    from parquet.`abfss://silver@salesdatalake1.dfs.core.windows.net/carsales`
''')
else:
    df_sink = spark.sql('''
    select 1 as dim_model_key, Model_Id, Model_Category
    from parquet.`abfss://silver@salesdatalake1.dfs.core.windows.net/carsales`
    where 1=0
    ''')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering New Records and Old Records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['Model_Id'] == df_sink['Model_Id'], 'left').select(df_src['Model_Id'], df_src['Model_Category'], df_sink['dim_model_key'])

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Df Filter Old
# MAGIC
# MAGIC

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_model_key').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Df Filter New

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_model_key').isNull()).select(df_src['Model_Id'], df_src['Model_Category'])

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
    max_value = spark.sql("select max(dim_model_key) from cars_catalog.gold.dim_model")
    max_value = max_value.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Surrogate Key Column And Add Max Surrogate Key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_model_key', max_value + monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Final DF - Df Filter Old + Df Filter New

# COMMAND ----------

df_final = df_filter_old.union(df_filter_new)

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
if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_tbl = DeltaTable.forPath(spark, 'abfss://gold@salesdatalake1.dfs.core.windows.net/dim_model')
    delta_tbl.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_key = src.dim_model_key').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Initial RUN
else:
    df_final.write.format('delta').mode('overwrite').option("path",'abfss://gold@salesdatalake1.dfs.core.windows.net/dim_model').saveAsTable('cars_catalog.gold.dim_model')
