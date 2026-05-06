# Databricks notebook source
from datetime import datetime
date = datetime.now().strftime("%Y/%m/%d")

# COMMAND ----------

Azure_stotagekey=dbutils.secrets.get('tt-hc-kv','stotagekey')

# COMMAND ----------

# The error is due to missing or invalid Azure storage account credentials.
# Set the correct storage account key or use a credential scope before accessing abfss paths.

spark.conf.set("fs.azure.account.key.ajayinsurance.dfs.core.windows.net", Azure_stotagekey)

from datetime import datetime
date = datetime.now().strftime("%Y/%m/%d")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW hosa_encounters
USING parquet
OPTIONS (
  path "abfss://bronze@ajayinsurance.dfs.core.windows.net/hosa/encounters/{date}"
)
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW hosb_encounters
USING parquet
OPTIONS (
  path "abfss://bronze@ajayinsurance.dfs.core.windows.net/hosb/encounters/{date}"
)
""").createOrReplaceTempView("encounters")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW encounters AS
# MAGIC SELECT * FROM hosa_encounters
# MAGIC UNION ALL
# MAGIC SELECT * FROM hosb_encounters;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW quality_checks AS
# MAGIC SELECT 
# MAGIC concat(EncounterID,'-',datasource) as EncounterID,
# MAGIC EncounterID SRC_EncounterID,
# MAGIC PatientID,
# MAGIC EncounterDate,
# MAGIC EncounterType,
# MAGIC ProviderID,
# MAGIC DepartmentID,
# MAGIC ProcedureCode,
# MAGIC InsertedDate as SRC_InsertedDate,
# MAGIC ModifiedDate as SRC_ModifiedDate,
# MAGIC datasource,
# MAGIC     CASE 
# MAGIC         WHEN EncounterID IS NULL OR PatientID IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM encounters

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from(
# MAGIC SELECT *,
# MAGIC   MD5(CONCAT(EncounterID, SRC_EncounterID, PatientID, EncounterType, ProviderID, DepartmentID, ProcedureCode, datasource)) AS MD5,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY EncounterID, 
# MAGIC       CONCAT(EncounterID, SRC_EncounterID, PatientID, EncounterType, ProviderID, DepartmentID, ProcedureCode, datasource)
# MAGIC     ORDER BY SRC_InsertedDate DESC
# MAGIC   ) AS rn
# MAGIC FROM quality_checks
# MAGIC ) WHERE rn = 1
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update old record to implement SCD Type 2
# MAGIC MERGE INTO silver.encounters AS target
# MAGIC USING (select * from(
# MAGIC   SELECT *,
# MAGIC   MD5(CONCAT(EncounterID, SRC_EncounterID, PatientID, EncounterType, ProviderID, DepartmentID, ProcedureCode, datasource)) AS MD5,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY EncounterID, 
# MAGIC       CONCAT(EncounterID, SRC_EncounterID, PatientID, EncounterType, ProviderID, DepartmentID, ProcedureCode, datasource)
# MAGIC     ORDER BY SRC_InsertedDate DESC
# MAGIC   ) AS rn
# MAGIC FROM quality_checks
# MAGIC )A WHERE rn = 1
# MAGIC ) AS source
# MAGIC ON target.EncounterID = source.EncounterID AND target.is_current = true
# MAGIC AND target.MD5 != source.MD5
# MAGIC WHEN MATCHED AND (
# MAGIC     target.SRC_EncounterID != source.SRC_EncounterID OR
# MAGIC     target.PatientID != source.PatientID OR
# MAGIC     target.EncounterDate != source.EncounterDate OR
# MAGIC     target.EncounterType != source.EncounterType OR
# MAGIC     target.ProviderID != source.ProviderID OR
# MAGIC     target.DepartmentID != source.DepartmentID OR
# MAGIC     target.ProcedureCode != source.ProcedureCode OR
# MAGIC     target.SRC_InsertedDate != source.SRC_InsertedDate OR
# MAGIC     target.SRC_ModifiedDate != source.SRC_ModifiedDate OR
# MAGIC     target.datasource != source.datasource OR
# MAGIC     target.is_quarantined != source.is_quarantined
# MAGIC ) THEN
# MAGIC   UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.audit_modifieddate = current_timestamp()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new record to implement SCD Type 2
# MAGIC MERGE INTO silver.encounters AS target USING (select * from(
# MAGIC   SELECT *,
# MAGIC   MD5(CONCAT(EncounterID, SRC_EncounterID, PatientID, EncounterType, ProviderID, DepartmentID, ProcedureCode, datasource)) AS MD5,
# MAGIC   ROW_NUMBER() OVER (
# MAGIC     PARTITION BY EncounterID, 
# MAGIC       CONCAT(EncounterID, SRC_EncounterID, PatientID, EncounterType, ProviderID, DepartmentID, ProcedureCode, datasource)
# MAGIC     ORDER BY SRC_InsertedDate DESC
# MAGIC   ) AS rn
# MAGIC FROM quality_checks
# MAGIC )A WHERE rn = 1
# MAGIC ) AS source ON target.EncounterID = source.EncounterID
# MAGIC AND target.is_current = true
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   (
# MAGIC     EncounterID,
# MAGIC     SRC_EncounterID,
# MAGIC     PatientID,
# MAGIC     EncounterDate,
# MAGIC     EncounterType,
# MAGIC     ProviderID,
# MAGIC     DepartmentID,
# MAGIC     ProcedureCode,
# MAGIC     SRC_InsertedDate,
# MAGIC     SRC_ModifiedDate,
# MAGIC     datasource,
# MAGIC     is_quarantined,
# MAGIC     audit_insertdate,
# MAGIC     audit_modifieddate,
# MAGIC     is_current,
# MAGIC     MD5
# MAGIC   )
# MAGIC VALUES
# MAGIC   (
# MAGIC     source.EncounterID,
# MAGIC     source.SRC_EncounterID,
# MAGIC     source.PatientID,
# MAGIC     source.EncounterDate,
# MAGIC     source.EncounterType,
# MAGIC     source.ProviderID,
# MAGIC     source.DepartmentID,
# MAGIC     source.ProcedureCode,
# MAGIC     source.SRC_InsertedDate,
# MAGIC     source.SRC_ModifiedDate,
# MAGIC     source.datasource,
# MAGIC     source.is_quarantined,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp(),
# MAGIC     true,
# MAGIC     source.MD5
# MAGIC   );