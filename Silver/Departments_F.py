# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ../Bronze/read_write_File_configs

# COMMAND ----------

from datetime import datetime
date = datetime.now().strftime("%Y/%m/%d")
file_path1=f"abfss://bronze@ajayinsurance.dfs.core.windows.net/hosa/departments/{date}"
file_path2=f"abfss://bronze@ajayinsurance.dfs.core.windows.net/hosb/departments/{date}"
read_type="parquet"
df_hosa=read_file(file_path1,read_type,schema=None, catalog=None, table=None)
df_hosb=read_file(file_path2,read_type,schema=None, catalog=None, table=None)


# COMMAND ----------

from pyspark.sql import SparkSession, functions as f

df_merged = df_hosa.unionByName(df_hosb)

# Create the dept_id column and rename deptid to src_dept_id
df_merged = df_merged.withColumn("SRC_Dept_id", f.col("deptid")) \
                     .withColumn("Dept_id", f.concat(f.col("deptid"),f.lit('-'), f.col("data_source"))) \
                     .drop("deptid")

df_merged.createOrReplaceTempView("departments")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.departments (
# MAGIC Dept_Id string,
# MAGIC SRC_Dept_Id string,
# MAGIC Name string,
# MAGIC datasource string,
# MAGIC is_quarantined boolean
# MAGIC )
# MAGIC USING DELTA LOCATION 'abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/silver_tables/departments';

# COMMAND ----------

# MAGIC %sql 
# MAGIC truncate table silver.departments

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into silver.departments
# MAGIC SELECT 
# MAGIC Dept_Id,
# MAGIC SRC_Dept_Id,
# MAGIC Name,
# MAGIC data_source,
# MAGIC     CASE 
# MAGIC         WHEN SRC_Dept_Id IS NULL OR Name IS NULL THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS is_quarantined
# MAGIC FROM departments

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.departments

