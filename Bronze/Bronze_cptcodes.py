# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ./read_write_File_configs

# COMMAND ----------

from pyspark.sql.functions import col, when

read_file_df=read_file("abfss://landding@ajayinsurance.dfs.core.windows.net/cptcodes/*.csv", read_type='csv', schema=None, catalog=None, table=None)
# read_file_df.display()
cptcodes_df=clean_column_names(read_file_df)




# COMMAND ----------

# DBTITLE 1,file ingestion
file_path="abfss://bronze@ajayinsurance.dfs.core.windows.net/cptcodes/"
mode="overwrite"
read_type="parquet"
write_file(cptcodes_df, file_path, read_type, mode, None)

# COMMAND ----------

# DBTITLE 1,Table Ingestion
# # Filepath="abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/cptcodes/"
# mode="overwrite"
# read_type="table"
# table='cptcodes'
# catalog='adfprojectd'
# schema='azure_new_project'
# write_file(cptcodes_df, None, read_type, mode, catalog, schema, table)

# COMMAND ----------

# read_file_df=read_file("abfss://bronze@ajayinsurance.dfs.core.windows.net/cptcodes/", read_type='parquet', schema=None, catalog=None, table=None).count()
# display(read_file_df)