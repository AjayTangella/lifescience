# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

from datetime import datetime
date = datetime.now().strftime("%Y/%m/%d")
file_path1=f"abfss://bronze@ajayinsurance.dfs.core.windows.net/hosa/providers/{date}"
file_path2=f"abfss://bronze@ajayinsurance.dfs.core.windows.net/hosb/providers/{date}"
read_type="parquet"
df_hosa=read_file(file_path1,read_type,schema=None, catalog=None, table=None)
df_hosb=read_file(file_path2,read_type,schema=None, catalog=None, table=None)
df_merged = df_hosa.unionByName(df_hosb)
df_merged.createOrReplaceTempView("providers")



# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table silver.providers

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into silver.providers
# MAGIC select 
# MAGIC distinct
# MAGIC ProviderID,
# MAGIC FirstName,
# MAGIC LastName,
# MAGIC Specialization,
# MAGIC DeptID,
# MAGIC cast(NPI as INT) NPI,
# MAGIC data_source,
# MAGIC     CASE 
# MAGIC         WHEN ProviderID IS NULL OR DeptID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC from providers

