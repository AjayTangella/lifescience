# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC
# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------


file_path="abfss://bronze@ajayinsurance.dfs.core.windows.net/cptcodes/"
read_type="parquet"
cptcodes_df=read_file(file_path,read_type,schema=None, catalog=None, table=None)
cptcodes_df.createOrReplaceTempView("cptcodes")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC  cpt_codes,
# MAGIC  procedure_code_category,
# MAGIC  procedure_code_descriptions,
# MAGIC  code_status,
# MAGIC current_timestamp() as SRC_InsertDate,
# MAGIC current_timestamp() as SRC_ModifiedDate,
# MAGIC     CASE 
# MAGIC         WHEN cpt_codes IS NULL OR procedure_code_descriptions IS NULL  THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM cptcodes

# COMMAND ----------

# %sql
# select * from quality_checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from cptcodes where CPT_Codes='58961'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adfprojectd.silver.cptcodes (
# MAGIC cpt_codes string,
# MAGIC procedure_code_category string,
# MAGIC procedure_code_descriptions string,
# MAGIC code_status string,
# MAGIC is_quarantined boolean,
# MAGIC audit_insertdate timestamp,
# MAGIC audit_modifieddate timestamp,
# MAGIC is_current boolean,
# MAGIC MD5 STRING
# MAGIC )
# MAGIC USING DELTA location  'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/silver_tables/cptcodes/';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(
# MAGIC   SELECT *, md5(concat(cpt_codes, procedure_code_category, code_status)) AS MD5,
# MAGIC          row_number() OVER (PARTITION BY cpt_codes, concat(cpt_codes, procedure_code_category, code_status) ORDER BY  SRC_InsertDate DESC ) rn
# MAGIC   FROM quality_checks
# MAGIC     )
# MAGIC   WHERE  --rn = 1 and
# MAGIC    cpt_codes='34830'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update old record to implement SCD Type 2
# MAGIC MERGE INTO silver.cptcodes AS target
# MAGIC USING ( 
# MAGIC   select * from(
# MAGIC   SELECT *, md5(concat(cpt_codes, procedure_code_category, code_status)) AS MD5,
# MAGIC          row_number() OVER (PARTITION BY cpt_codes, concat(cpt_codes, procedure_code_category, code_status) ORDER BY  SRC_InsertDate DESC ) rn
# MAGIC   FROM quality_checks
# MAGIC     )
# MAGIC   WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.cpt_codes = source.cpt_codes 
# MAGIC AND target.is_current = true
# MAGIC -- Remove target.MD5 = source.MD5 from ON clause to allow update when values change
# MAGIC WHEN MATCHED AND (
# MAGIC target.procedure_code_category!=source.procedure_code_category or 
# MAGIC target.procedure_code_descriptions!=source.procedure_code_descriptions or 
# MAGIC target.code_status!=source.code_status) THEN
# MAGIC UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# %sql
# -- Update old record to implement SCD Type 2
# MERGE INTO silver.cptcodes AS target
# USING (
#   SELECT *, md5(concat(cpt_codes, procedure_code_category, code_status)) AS MD5,
#          row_number() OVER (PARTITION BY cpt_codes, concat(cpt_codes, procedure_code_category, code_status) ORDER BY procedure_code_category, code_status) rk
#   FROM quality_checks
# ) AS source
# ON target.cpt_codes = source.cpt_codes 
# AND target.is_current = true
# AND target.MD5 <> source.MD5
# AND source.rk = 1
# WHEN MATCHED AND target.is_current = true THEN
#   UPDATE SET
#     target.is_current = false,
#     target.audit_modifieddate = current_timestamp()

# COMMAND ----------

# MAGIC %skip
# MAGIC # %sql
# MAGIC # select * from(
# MAGIC #   select *,md5(concat(cpt_codes,procedure_code_category,code_status))as MD5 ,
# MAGIC # row_number()over(partition by cpt_codes order by procedure_code_category,code_status desc) rk
# MAGIC # from quality_checks 
# MAGIC # )A
# MAGIC # where rk=1 and  cpt_codes='34830'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new record to implement SCD Type 2
# MAGIC MERGE INTO silver.cptcodes AS target
# MAGIC USING ( select * from(
# MAGIC   SELECT *, md5(concat(cpt_codes, procedure_code_category, code_status)) AS MD5,
# MAGIC          row_number() OVER (PARTITION BY cpt_codes, concat(cpt_codes, procedure_code_category, code_status) ORDER BY  SRC_InsertDate DESC ) rn
# MAGIC   FROM quality_checks
# MAGIC     )
# MAGIC   WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.cpt_codes = source.cpt_codes AND target.is_current = true AND target.MD5 = source.MD5
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     cpt_codes,
# MAGIC     procedure_code_category,
# MAGIC     procedure_code_descriptions,
# MAGIC     code_status,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current,
# MAGIC     MD5
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.cpt_codes,
# MAGIC     source.procedure_code_category,
# MAGIC     source.procedure_code_descriptions,
# MAGIC     source.code_status,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true,
# MAGIC     source.MD5
# MAGIC   );

# COMMAND ----------


# from pyspark.sql.functions import when, col
# df = spark.read.parquet("abfss://bronze@ajayinsurance.dfs.core.windows.net/cptcodes/")
# df_final=df.withColumn("code_status",when(col("cpt_codes") == 34830, "Change").otherwise(col("code_status")))
# df_final.write.mode("overwrite").parquet("abfss://bronze@ajayinsurance.dfs.core.windows.net/cptcodes/")
# # df_final.display()


# COMMAND ----------

# %sql
# select * from  silver.cptcodes where cpt_codes='34830'

# COMMAND ----------


# %sql
# select cpt_codes,count(*) from silver.cptcodes --where cpt_codes='58961'
# group by cpt_codes
# having count(*)>1

