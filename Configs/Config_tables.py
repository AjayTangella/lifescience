# Databricks notebook source
# DBTITLE 1,claims
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS adfprojectd.silver.claims (
# MAGIC ClaimID string,
# MAGIC SRC_ClaimID string,
# MAGIC TransactionID string,
# MAGIC PatientID string,
# MAGIC EncounterID string,
# MAGIC ProviderID string,
# MAGIC DeptID string,
# MAGIC ServiceDate date,
# MAGIC ClaimDate date,
# MAGIC PayorID string,
# MAGIC ClaimAmount string,
# MAGIC PaidAmount string,
# MAGIC ClaimStatus string,
# MAGIC PayorType string,
# MAGIC Deductible string,
# MAGIC Coinsurance string,
# MAGIC Copay string,
# MAGIC SRC_InsertDate date,
# MAGIC SRC_ModifiedDate date,
# MAGIC datasource string,
# MAGIC is_quarantined boolean,
# MAGIC audit_insertdate timestamp,
# MAGIC audit_modifieddate timestamp,
# MAGIC is_current boolean,
# MAGIC MD5 string
# MAGIC )
# MAGIC USING Delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/silver_tables/claims';

# COMMAND ----------

# DBTITLE 1,encounters
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.encounters (
# MAGIC EncounterID string,
# MAGIC SRC_EncounterID string,
# MAGIC PatientID string,
# MAGIC EncounterDate date,
# MAGIC EncounterType string,
# MAGIC ProviderID string,
# MAGIC DepartmentID string,
# MAGIC ProcedureCode integer,
# MAGIC SRC_InsertedDate date,
# MAGIC SRC_ModifiedDate date,
# MAGIC datasource string,
# MAGIC is_quarantined boolean,
# MAGIC audit_insertdate timestamp,
# MAGIC audit_modifieddate timestamp,
# MAGIC is_current boolean,
# MAGIC MD5 string 
# MAGIC )
# MAGIC USING DELTA Location  'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/silver_tables/encounters';

# COMMAND ----------

# DBTITLE 1,icd_codes
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.icd_codes (
# MAGIC     icd_code STRING,
# MAGIC     icd_code_type STRING,
# MAGIC     code_description STRING,
# MAGIC     inserted_date DATE,
# MAGIC     updated_date DATE,
# MAGIC     is_current_flag BOOLEAN,
# MAGIC     MD5 string
# MAGIC )
# MAGIC USING Delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/silver_tables/icd_codes';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.npi_extract (
# MAGIC   npi_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   position STRING,
# MAGIC   organisation_name STRING,
# MAGIC   last_updated STRING,
# MAGIC   inserted_date DATE,
# MAGIC   updated_date DATE,
# MAGIC   is_current_flag BOOLEAN
# MAGIC )
# MAGIC USING Delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/silver_tables/npi_extract';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.providers (
# MAGIC ProviderID string,
# MAGIC FirstName string,
# MAGIC LastName string,
# MAGIC Specialization string,
# MAGIC DeptID string,
# MAGIC NPI long,
# MAGIC datasource string,
# MAGIC is_quarantined boolean
# MAGIC )
# MAGIC USING Delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/silver_tables/providers';

# COMMAND ----------

# MAGIC %md
# MAGIC #gold Schema#
# MAGIC

# COMMAND ----------

# DBTITLE 1,dim_cpt_code
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_cpt_code
# MAGIC (
# MAGIC cpt_codes string,
# MAGIC procedure_code_category string,
# MAGIC procedure_code_descriptions string,
# MAGIC code_status string,
# MAGIC refreshed_at timestamp
# MAGIC ) using delta Location 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_cpt_code';

# COMMAND ----------

# DBTITLE 1,dim_department
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_department
# MAGIC (
# MAGIC Dept_Id string,
# MAGIC SRC_Dept_Id string,
# MAGIC Name string,
# MAGIC datasource string,
# MAGIC refreshed_at timestamp
# MAGIC )using delta Location 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_department';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_icd (
# MAGIC     icd_code STRING,
# MAGIC     icd_code_type STRING,
# MAGIC     code_description STRING,
# MAGIC     refreshed_at TIMESTAMP
# MAGIC )using delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_icd'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_npi (
# MAGIC   npi_id STRING,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   position STRING,
# MAGIC   organisation_name STRING,
# MAGIC   last_updated STRING,
# MAGIC   refreshed_at TIMESTAMP)
# MAGIC   using delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_npi'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_patient
# MAGIC (
# MAGIC     patient_key STRING,
# MAGIC     src_patientid STRING,
# MAGIC     firstname STRING,
# MAGIC     lastname STRING,
# MAGIC     middlename STRING,
# MAGIC     ssn STRING,
# MAGIC     phonenumber STRING,
# MAGIC     gender STRING,
# MAGIC     dob DATE,
# MAGIC     address STRING,
# MAGIC     datasource STRING
# MAGIC ) using delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_patient'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_provider
# MAGIC (
# MAGIC ProviderID string,
# MAGIC FirstName string,
# MAGIC LastName string,
# MAGIC DeptID string,
# MAGIC NPI long,
# MAGIC datasource string
# MAGIC ) using delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/dim_provider'

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists gold.fact_transactions
# MAGIC (
# MAGIC   TransactionID string,
# MAGIC   SRC_TransactionID string,
# MAGIC   EncounterID string,
# MAGIC   FK_PatientID string,
# MAGIC   FK_ProviderID string,
# MAGIC   FK_DeptID string,
# MAGIC   ICDCode string,
# MAGIC   ProcedureCode string,
# MAGIC   VisitType string,
# MAGIC   ServiceDate date,
# MAGIC   PaidDate date,
# MAGIC   Amount double,
# MAGIC   PaidAmount double,
# MAGIC   AmountType string,
# MAGIC   ClaimID string,
# MAGIC   datasource string,
# MAGIC   refreshed_at timestamp
# MAGIC )using delta LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/gold/fact_transactions'