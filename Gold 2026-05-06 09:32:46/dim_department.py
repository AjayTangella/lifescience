# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col
selected_Columns=["is_quarantined"]
df_silver_departments=spark.read.table("silver.departments")
df_silver_departments_final = df_silver_departments.filter(col("is_quarantined") == False).drop(*selected_Columns)\
        .withColumn("refreshed_at", current_timestamp())


# COMMAND ----------

import concurrent.futures
from pyspark.sql.functions import current_timestamp
file_path="abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_department"
mode="overwrite"
read_type="delta"
write_file(df_silver_departments_final, file_path, read_type, mode, None)
with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(df_silver_departments_final)
    concurrent.futures.wait([future])

# COMMAND ----------

# %sql
# insert into gold.dim_department
# select 
# distinct
# Dept_Id ,
# SRC_Dept_Id ,
# Name ,
# datasource 
#  from silver.departments
#  where is_quarantined=false

# COMMAND ----------

# %sql 
# select * from adfprojectd.gold.dim_department
