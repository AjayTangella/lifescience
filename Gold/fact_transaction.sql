-- Databricks notebook source
truncate table gold.fact_transactions

-- COMMAND ----------

insert into gold.fact_transactions
select 
  t.TransactionID, 
  t.SRC_TransactionID,
  t.EncounterID,
  concat(t.PatientID,'-',t.datasource ) as FK_Patient_ID,
  case when t.datasource='hos-a' then concat('H1-',t.providerID) else concat('H2-',t.providerID ) end as FK_Provider_ID, 
  concat(t.DeptID,'-',t.datasource ) as FK_Dept_ID, 
  t.ICDCode,
  t.ProcedureCode CPT_Code,
  t.VisitType,
  t.ServiceDate, 
  t.PaidDate,
  t.Amount Charge_Amt, 
  t.PaidAmount Paid_Amt, 
  t.AmountType,
  t.ClaimID,
  t.datasource,
  current_timestamp()
  from silver.transactions t 
  where t.is_current=true and t.is_quarantined=false



-- COMMAND ----------

select 
  t.TransactionID, 
  t.SRC_TransactionID,
  t.EncounterID,
  concat(t.PatientID,'-',t.datasource ) as FK_Patient_ID,
  case when t.datasource='hos-a' then concat('H1-',t.providerID) else concat('H2-',t.providerID ) end as FK_Provider_ID, 
  concat(t.DeptID,'-',t.datasource ) as FK_Dept_ID, 
  t.ICDCode,
  t.ProcedureCode CPT_Code,
  t.VisitType,
  t.ServiceDate, 
  t.PaidDate,
  t.Amount Charge_Amt, 
  t.PaidAmount Paid_Amt, 
  t.AmountType,
  t.ClaimID,
  t.datasource,
  current_timestamp()
  from silver.transactions t 
  where t.is_current=true and t.is_quarantined=false



-- COMMAND ----------

select * from   from silver.transactions limit 10;
