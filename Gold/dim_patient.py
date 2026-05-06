# Databricks notebook source
# MAGIC %sql
# MAGIC select * from gold.dim_patient;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into gold.dim_patient
# MAGIC select 
# MAGIC      patient_key ,
# MAGIC     src_patientid ,
# MAGIC     firstname ,
# MAGIC     lastname ,
# MAGIC     middlename ,
# MAGIC     ssn ,
# MAGIC     phonenumber ,
# MAGIC     gender ,
# MAGIC     dob ,
# MAGIC     address ,
# MAGIC     datasource 
# MAGIC  from silver.patients
# MAGIC  where is_current=true and is_quarantined=false
