# Databricks notebook source
from pyspark.sql.functions import col,when

# COMMAND ----------

# %run ../Mount/ADLS_config

# COMMAND ----------

# DBTITLE 1,Read function
def read_file(file_path, read_type, schema, catalog, table):
    if read_type == 'csv':
        return spark.read.format('csv').option('header', 'true').load(file_path)
    elif read_type == 'parquet':
        return spark.read.format('parquet').load(file_path)
    elif read_type == 'table':
        return spark.table(f"{catalog}.{schema}.{table}")
    else:
        raise ValueError("Unsupported file type")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def add_dynamic_columns(df, cols_to_add, cols_to_drop=None):
    # Add dynamic columns
    for col_name, col_value in cols_to_add.items():
        df = df.withColumn(col_name, col_value)
    
    # Drop columns if provided
    if cols_to_drop:
        df = df.drop(*cols_to_drop)
    
    return df

# COMMAND ----------

# DBTITLE 1,Remove spaces to '_'
def clean_column_names(df):
    new_columns = [col.replace(' ', '_') for col in df.columns]
    return df.toDF(*new_columns)

# COMMAND ----------

def clean_column_names_cpt(df):
    return df.withColumn(
        "datasource",
        when(col('_metadata.file_path').contains(file_name), final_source_name)
        .when(col('_metadata.file_path').contains(file_name2), final_source_name2)
        .otherwise(None)
    )

# COMMAND ----------

def df_union(df1,df2):
    return df1.union(df2)

# COMMAND ----------

# DBTITLE 1,write function
# def write_file(df, file_path, read_type, mode, catalog=None, schema=None, table=None):
#     if read_type == "csv":
#         if mode == "overwrite":
#             return df.write.save(file_path, format='csv', mode="overwrite")
#         elif mode == "append":
#             return df.write.save(file_path, format='csv', mode="append")
#     elif read_type == "parquet":
#         if mode == "overwrite":
#             return df.write.save(file_path, format='parquet', mode="overwrite")
#         elif mode == "append":
#             return df.write.save(file_path, format='parquet', mode="append")
#     elif read_type == "JSON":
#         if mode == "overwrite":
#             return df.write.save(file_path, format='JSON', mode="overwrite")
#         elif mode == "append":
#             return df.write.save(file_path, format='JSON', mode="append")
#     elif read_type == "table":
#         if mode == "overwrite":
#             return df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{table}")
#         elif mode == "append":
#             return df.write.mode("append").saveAsTable(f"{catalog}.{schema}.{table}")
#     else:
#         raise ValueError("Unsupported file type")

# COMMAND ----------

def write_file(df, file_path, read_type, mode, catalog=None, schema=None, table=None):
    if mode not in ["append", "overwrite"]:
        raise ValueError("Mode must be 'append' or 'overwrite'")

    read_type = read_type.lower()

    if read_type in ["csv", "parquet", "json","delta"]:
        return (
            df.write.format(read_type).mode(mode).save(file_path)
        )

    elif read_type == "table":
        if not all([catalog, schema, table]):
            raise ValueError("catalog, schema, and table must be provided for table write")

        full_table_name = f"{catalog}.{schema}.{table}"

        return (
            df.write.mode(mode).saveAsTable(full_table_name)
        )

    else:
        raise ValueError("Unsupported read_type")

# COMMAND ----------

# read_file_df=read_file("abfss://landding@ajayinsurance.dfs.core.windows.net/cptcodes/*.csv", read_type='csv', schema=None, catalog=None, table=None)
# cptcodes_df=clean_column_names(read_file_df)

# COMMAND ----------

# cptcodes_df.write.option("path", "abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/cptcodes_test/").saveAsTable("adfprojectd.azure_new_project.cptcodes")

# COMMAND ----------

# # Filepath="abfss://unitycatloag@ajayinsurance.dfs.core.windows.net/cptcodes/"
# mode="overwrite"
# read_type="table"
# table='cptcodes'
# catalog='adfprojectd'
# schema='azure_new_project'
# write_file(cptcodes_df, None, read_type, mode, catalog, schema, table)

# COMMAND ----------

# %sql
# select count(*) from adfprojectd.azure_new_project.cptcodes
