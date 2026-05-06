# Databricks notebook source
# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, col, lit

dynamic_cols = {
    # "refreshed_at": current_timestamp(),
    "deptid": concat(col("DeptID"), lit('-'), col("datasource"))
}

selected_Columns = ["Specialization", "updated_date","is_quarantined"]

df_silver_providers = spark.read.table("silver.providers")
df_silver_providers_1 = add_dynamic_columns(
    df_silver_providers,
    dynamic_cols,
    selected_Columns
)
df_silver_providers_final= df_silver_providers_1.filter(col("is_quarantined") == "false")

# COMMAND ----------

import concurrent.futures
from pyspark.sql.functions import current_timestamp
file_path="abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_provider"
mode="overwrite"
read_type="delta"
write_file(df_silver_providers_final, file_path, read_type, mode, None)
with concurrent.futures.ThreadPoolExecutor() as executor:
    future = executor.submit(df_silver_providers_final)
    concurrent.futures.wait([future])

# COMMAND ----------

# %sql
# select 
# ProviderID ,
# FirstName ,
# LastName ,
# concat(DeptID,'-',datasource) deptid,
# NPI ,
# datasource 
#  from silver.providers
#  where is_quarantined=false

# COMMAND ----------

# %sql
# insert into gold.dim_provider
# select 
# ProviderID ,
# FirstName ,
# LastName ,
# concat(DeptID,'-',datasource) deptid,
# NPI ,
# datasource 
#  from silver.providers
#  where is_quarantined=false
