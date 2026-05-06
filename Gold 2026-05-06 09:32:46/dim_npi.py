# Databricks notebook source
# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

dynamic_cols = {
    "refreshed_at": current_timestamp()
}

# Columns to remove
selected_Columns = ["inserted_date", "updated_date", "is_current_flag"]

df_silver_npi_extract = spark.read.table("silver.npi_extract")
df_silver_npi_extract_1 = add_dynamic_columns(
    df_silver_npi_extract,
    dynamic_cols,
    selected_Columns
)
df_silver_npi_extract_final= df_silver_npi_extract_1.filter(col("is_current_flag") == "true")

# COMMAND ----------

import concurrent.futures
from pyspark.sql.functions import current_timestamp
file_path="abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_npi"
mode="overwrite"
read_type="delta"
write_file(df_silver_npi_extract_final, file_path, read_type, mode, None)
with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(df_silver_npi_extract_final)
    concurrent.futures.wait([future])
