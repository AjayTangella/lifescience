# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

file_path="abfss://bronze@ajayinsurance.dfs.core.windows.net/icd_codes/"
read_type="parquet"
icd_codes_df=read_file(file_path,read_type,schema=None, catalog=None, table=None)
icd_codes_df.createOrReplaceTempView("staging_icd_codes")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *,MD5(concat(icd_code,icd_code_type,code_description))MD5,
# MAGIC row_number()over(partition by icd_code ,concat(icd_code_type,code_description) order by updated_date desc) rn
# MAGIC  from staging_icd_codes limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.icd_codes AS target
# MAGIC USING (select * from(
# MAGIC   select *, MD5(concat(icd_code, icd_code_type, code_description)) as MD5,
# MAGIC     row_number() over (partition by icd_code, concat(icd_code_type, code_description) order by updated_date desc) as rn
# MAGIC   from staging_icd_codes) where rn=1
# MAGIC ) AS source
# MAGIC ON target.icd_code = source.icd_code
# MAGIC   AND target.is_current_flag = true
# MAGIC   AND target.MD5 != source.MD5
# MAGIC WHEN MATCHED AND target.code_description != source.code_description THEN
# MAGIC   UPDATE SET
# MAGIC     code_description = source.code_description,
# MAGIC     updated_date = source.updated_date,
# MAGIC     is_current_flag = false
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (icd_code, icd_code_type, code_description, inserted_date, updated_date, is_current_flag)
# MAGIC   VALUES (
# MAGIC     source.icd_code,
# MAGIC     source.icd_code_type,
# MAGIC     source.code_description,
# MAGIC     source.inserted_date,
# MAGIC     source.updated_date,
# MAGIC     source.is_current_flag
# MAGIC   )
# MAGIC
