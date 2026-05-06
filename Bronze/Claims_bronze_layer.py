# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ./read_write_File_configs

# COMMAND ----------

dbutils.widgets.text("file_name","")
dbutils.widgets.text("file_name2","")
dbutils.widgets.text("final_source_name","")
dbutils.widgets.text("final_source_name2","")
file_name=dbutils.widgets.get("file_name")
file_name2=dbutils.widgets.get("file_name2")
final_source_name=dbutils.widgets.get("final_source_name")
final_source_name2=dbutils.widgets.get("final_source_name2")
# dbutils.widgets.removeAll()


# COMMAND ----------

read_file_df=read_file("abfss://landding@ajayinsurance.dfs.core.windows.net/claim/*.csv", read_type='csv', schema=None, catalog=None, table=None)
claims_df=clean_column_names_cpt(read_file_df)
# claims_df.display()

# COMMAND ----------

# from pyspark.sql.functions import col, when

# read_file_df = read_file("abfss://landding@ajayinsurance.dfs.core.windows.net/claim/*.csv", read_type='csv', schema=None, catalog=None, table=None)

# claims_df = read_file_df.withColumn(
#     "datasource",
#     when(col('_metadata.file_path').contains("hospital1"), "hosa")
#     .when(col('_metadata.file_path').contains("hospital2"), "hosb")
#     .otherwise(None)
# )
# display(claims_df)

# COMMAND ----------

# from pyspark.sql import SparkSession, functions as f

# claims_df=spark.read.csv("/mnt/landing/claims/*.csv",header=True)

# claims_df = claims_df.withColumn(
#     "datasource",
#     f.when(f.input_file_name().contains("hospital1"), "hosa").when(f.input_file_name().contains("hospital2"), "hosb")
#      .otherwise(None)
# )

# display(claims_df)

# COMMAND ----------

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit



# # Define the filenames
# filenames = ["hospital1_claim_data.csv", "hospital2_claim_data.csv"]

# # Iterate over each filename
# for filename in filenames:
#     # Read the file from ADLS
#     claims_df = spark.read.format("csv").option("header", "true").load("/mnt/landing/claims/" + filename)
    
#     # Check the filename and create a new column
#     if "hospital1_claim_data.csv" in filename:
#         claims_df = claims_df.withColumn("datasource", lit("hosta"))
#     elif "hospital2_claim_data.csv" in filename:
#         claims_df = claims_df.withColumn("datasource", lit("hostb"))
    
#     # claims_df=claims_df.filter(f.col("datasource")=="hosta")
#     # Display the DataFrame
#     display(claims_df)
    
#     # Write the DataFrame back to ADLS or another location if needed
#     # claims_df.write.format("delta").save("/mnt/landing/processed/" + filename)


# COMMAND ----------

file_path="abfss://bronze@ajayinsurance.dfs.core.windows.net/claim/"
mode="overwrite"
read_type="parquet"
write_file(claims_df, file_path, read_type, mode, None)

# COMMAND ----------


