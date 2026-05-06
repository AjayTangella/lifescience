# Databricks notebook source
# %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
selected_Columns=["is_quarantined","audit_insertdate","audit_modifieddate","is_current","MD5"]
df_silver_cptcodes=spark.read.table("silver.cptcodes")
df_silver_cptcodes_final = df_silver_cptcodes.filter((col("is_quarantined")=="false") & (col("is_current")=="true")).drop(*selected_Columns)\
        .withColumn("refreshed_at", current_timestamp())


# COMMAND ----------

import concurrent.futures
from pyspark.sql.functions import current_timestamp
file_path="abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_cpt_code"
mode="overwrite"
read_type="delta"
write_file(df_silver_cptcodes_final, file_path, read_type, mode, None)
with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(df_silver_cptcodes_final)
    concurrent.futures.wait([future])
