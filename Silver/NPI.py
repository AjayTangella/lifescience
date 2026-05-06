# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

file_path="abfss://bronze@ajayinsurance.dfs.core.windows.net/npi_extract/"
read_type="parquet"
npi_extract_df=read_file(file_path,read_type,schema=None, catalog=None, table=None)
npi_extract_df.createOrReplaceTempView("npi_extract")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO
# MAGIC   silver.npi_extract AS target
# MAGIC USING
# MAGIC   npi_extract AS source
# MAGIC ON target.npi_id = source.npi_id and target.is_current_flag = true
# MAGIC WHEN MATCHED AND
# MAGIC   target.first_name != source.first_name OR
# MAGIC   target.last_name != source.last_name OR
# MAGIC   target.position != source.position OR
# MAGIC   target.organisation_name != source.organisation_name OR
# MAGIC   target.last_updated != source.last_updated
# MAGIC   THEN UPDATE SET
# MAGIC   target.updated_date = current_date,
# MAGIC   target.is_current_flag = False
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO
# MAGIC   silver.npi_extract AS target
# MAGIC USING
# MAGIC   npi_extract AS source
# MAGIC ON target.npi_id = source.npi_id and target.is_current_flag = true
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC    npi_id,
# MAGIC   first_name ,
# MAGIC   last_name ,
# MAGIC   position ,
# MAGIC   organisation_name ,
# MAGIC   last_updated ,
# MAGIC   inserted_date ,
# MAGIC   updated_date ,
# MAGIC   is_current_flag 
# MAGIC )
# MAGIC   VALUES (
# MAGIC     source.npi_id,
# MAGIC   source.first_name ,
# MAGIC   source.last_name ,
# MAGIC   source.position ,
# MAGIC   source.organisation_name ,
# MAGIC   source.last_updated ,
# MAGIC   current_date,
# MAGIC   current_date, 
# MAGIC   true
# MAGIC   )

# COMMAND ----------


