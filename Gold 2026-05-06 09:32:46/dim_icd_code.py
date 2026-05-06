# Databricks notebook source
# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

# %sql
# truncate table gold.dim_icd

# COMMAND ----------

# Columns to add dynamically
dynamic_cols = {
    "refreshed_at": current_timestamp()
}

# Columns to remove
selected_Columns = ["inserted_date", "updated_date", "is_current_flag", "MD5"]

df_silver_icd_codes = spark.read.table("silver.icd_codes")

df_silver_icd_codes_final_1 = add_dynamic_columns(
    df_silver_icd_codes,
    dynamic_cols,
    selected_Columns
)
df_silver_icd_codes_final= df_silver_icd_codes_final_1.filter(col("is_current_flag") == "true")


# COMMAND ----------

# DBTITLE 1,write in table
import concurrent.futures
from pyspark.sql.functions import current_timestamp
file_path="abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/icd_codes"
mode="overwrite"
read_type="delta"
write_file(df_silver_icd_codes_final, file_path, read_type, mode, None)
with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(df_silver_icd_codesac_final)
    concurrent.futures.wait([future])

# COMMAND ----------

# %sql
# select distinct
#   icd_code,
#   icd_code_type,
#   code_description,
#   current_timestamp() refreshed_at
# from
#   silver.icd_codes
# where
#   is_current_flag = true
