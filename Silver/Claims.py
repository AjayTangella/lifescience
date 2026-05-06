# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

file_path="abfss://bronze@ajayinsurance.dfs.core.windows.net/claim/"
read_type="parquet"
claims_df=read_file(file_path,read_type,schema=None, catalog=None, table=None)
claims_df.createOrReplaceTempView("claims")

# COMMAND ----------

# %sql
# select * from claims

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE TEMP VIEW quality_checks 
# MAGIC AS
# MAGIC select concat(ClaimID,'-',datasource) as ClaimID,
# MAGIC ClaimID AS  SRC_ClaimID,
# MAGIC TransactionID,
# MAGIC PatientID,
# MAGIC EncounterID,
# MAGIC ProviderID,
# MAGIC DeptID,
# MAGIC cast(ServiceDate as date) ServiceDate,
# MAGIC cast(ClaimDate as date) ClaimDate,
# MAGIC PayorID,
# MAGIC ClaimAmount,
# MAGIC PaidAmount,
# MAGIC ClaimStatus,
# MAGIC PayorType,
# MAGIC Deductible,
# MAGIC Coinsurance,
# MAGIC Copay,
# MAGIC cast(InsertDate as date) as SRC_InsertDate,
# MAGIC cast(ModifiedDate as date) as SRC_ModifiedDate,
# MAGIC case when ClaimID is null or TransactionID is null or PatientID is null then 'True' else 'False' END AS is_quarantined
# MAGIC from claims 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.claims AS target
# MAGIC USING (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            md5(concat(
# MAGIC              ClaimID, SRC_ClaimID, TransactionID, PatientID, EncounterID,
# MAGIC              ProviderID, DeptID, ClaimStatus, PayorType, Deductible,
# MAGIC              Coinsurance, is_quarantined
# MAGIC            )) AS MD5,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY ClaimID, 
# MAGIC              concat(
# MAGIC                ClaimID, SRC_ClaimID, TransactionID, PatientID, EncounterID,
# MAGIC                ProviderID, DeptID, ClaimStatus, PayorType, Deductible,
# MAGIC                Coinsurance, is_quarantined
# MAGIC              )
# MAGIC              ORDER BY SRC_InsertDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM quality_checks
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.ClaimID = source.ClaimID
# MAGIC AND target.MD5 = source.MD5
# MAGIC AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.SRC_ClaimID != source.SRC_ClaimID OR
# MAGIC     target.TransactionID != source.TransactionID OR
# MAGIC     target.PatientID != source.PatientID OR
# MAGIC     target.EncounterID != source.EncounterID OR
# MAGIC     target.ProviderID != source.ProviderID OR
# MAGIC     target.DeptID != source.DeptID OR
# MAGIC     target.ServiceDate != source.ServiceDate OR
# MAGIC     target.ClaimDate != source.ClaimDate OR
# MAGIC     target.PayorID != source.PayorID OR
# MAGIC     target.ClaimAmount != source.ClaimAmount OR
# MAGIC     target.PaidAmount != source.PaidAmount OR
# MAGIC     target.ClaimStatus != source.ClaimStatus OR
# MAGIC     target.PayorType != source.PayorType OR
# MAGIC     target.Deductible != source.Deductible OR
# MAGIC     target.Coinsurance != source.Coinsurance OR
# MAGIC     target.Copay != source.Copay OR
# MAGIC     target.SRC_InsertDate != source.SRC_InsertDate OR
# MAGIC     target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp()
# MAGIC -- The error occurs because multiple source rows can match the same target row.
# MAGIC -- To fix, deduplicate the source by ClaimID and MD5.
# MAGIC -- Replace the source subquery with a deduplicated version:
# MAGIC
# MAGIC -- USAGE:
# MAGIC -- USING (
# MAGIC --   SELECT *
# MAGIC --   FROM (
# MAGIC --     SELECT *,
# MAGIC --            md5(concat(
# MAGIC --              ClaimID, SRC_ClaimID, TransactionID, PatientID, EncounterID,
# MAGIC --              ProviderID, DeptID, ClaimStatus, PayorType, Deductible,
# MAGIC --              Coinsurance, is_quarantined
# MAGIC --            )) AS MD5,
# MAGIC --            ROW_NUMBER() OVER (PARTITION BY ClaimID, MD5 ORDER BY SRC_InsertDate DESC) AS rn
# MAGIC --     FROM quality_checks
# MAGIC --   )
# MAGIC --   WHERE rn = 1
# MAGIC -- ) AS source

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.claims AS target
# MAGIC USING (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT *,
# MAGIC            md5(concat(
# MAGIC              ClaimID, SRC_ClaimID, TransactionID, PatientID, EncounterID,
# MAGIC              ProviderID, DeptID, ClaimStatus, PayorType, Deductible,
# MAGIC              Coinsurance, is_quarantined
# MAGIC            )) AS MD5,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY ClaimID, 
# MAGIC              concat(
# MAGIC                ClaimID, SRC_ClaimID, TransactionID, PatientID, EncounterID,
# MAGIC                ProviderID, DeptID, ClaimStatus, PayorType, Deductible,
# MAGIC                Coinsurance, is_quarantined
# MAGIC              )
# MAGIC              ORDER BY SRC_InsertDate DESC
# MAGIC            ) AS rn
# MAGIC     FROM quality_checks
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.ClaimID = source.ClaimID
# MAGIC AND target.MD5 = source.MD5
# MAGIC AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     ClaimID,
# MAGIC     SRC_ClaimID,
# MAGIC     TransactionID,
# MAGIC     PatientID,
# MAGIC     EncounterID,
# MAGIC     ProviderID,
# MAGIC     DeptID,
# MAGIC     ServiceDate,
# MAGIC     ClaimDate,
# MAGIC     PayorID,
# MAGIC     ClaimAmount,
# MAGIC     PaidAmount,
# MAGIC     ClaimStatus,
# MAGIC     PayorType,
# MAGIC     Deductible,
# MAGIC     Coinsurance,
# MAGIC     Copay,
# MAGIC     SRC_InsertDate,
# MAGIC     SRC_ModifiedDate,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current,
# MAGIC     MD5
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.ClaimID,
# MAGIC     source.SRC_ClaimID,
# MAGIC     source.TransactionID,
# MAGIC     source.PatientID,
# MAGIC     source.EncounterID,
# MAGIC     source.ProviderID,
# MAGIC     source.DeptID,
# MAGIC     source.ServiceDate,
# MAGIC     source.ClaimDate,
# MAGIC     source.PayorID,
# MAGIC     source.ClaimAmount,
# MAGIC     source.PaidAmount,
# MAGIC     source.ClaimStatus,
# MAGIC     source.PayorType,
# MAGIC     source.Deductible,
# MAGIC     source.Coinsurance,
# MAGIC     source.Copay,
# MAGIC     source.SRC_InsertDate,
# MAGIC     source.SRC_ModifiedDate,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true,
# MAGIC     source.MD5
# MAGIC   );

# COMMAND ----------

# from pyspark.sql.functions import when, col
# df = spark.read.parquet("abfss://bronze@ajayinsurance.dfs.core.windows.net/claim/")

# # Apply the update logic
# df_updated = df.withColumn(
#     "PaidAmount",
#     when(
#         (col("ClaimID") == "CLAIM000001") & (col("TransactionID") == "TRANS001819"),
#         "390.0"   # updated value
#     ).otherwise(col("PaidAmount"))
# )

# # Overwrite the Parquet file with updated data
# df_updated.write.mode("append").parquet("abfss://bronze@ajayinsurance.dfs.core.windows.net/claim/")


# COMMAND ----------

# MAGIC %sql
# MAGIC select ClaimID,count(*) from silver.claims
# MAGIC group by ClaimID
# MAGIC having count(*) >1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.claims where ClaimID='CLAIM000001-hosb'

