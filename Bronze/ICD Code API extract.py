# Databricks notebook source
# MAGIC %run ../Mount/ADLS_config

# COMMAND ----------

# MAGIC %run ./read_write_File_configs

# COMMAND ----------

file_path="abfss://bronze@ajayinsurance.dfs.core.windows.net/icd_codes/"
mode="overwrite"
read_type="parquet"

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType



client_id = 'c1b31e75-de89-4e9e-9738-87a4779e98e9_b7b0ac50-33a3-4ba5-abc2-4f4b1050ab15'
client_secret = 'UtTb8EIodKWT2g3BoIkgGsNbMcv0ClZKJbEij4ySzTs='
base_url = 'https://id.who.int/icd/'
current_date=datetime.now().date()

auth_url = 'https://icdaccessmanagement.who.int/connect/token'
auth_response = requests.post(auth_url, data={
    'client_id': client_id,
    'client_secret': client_secret,
    'grant_type': 'client_credentials'
})

if auth_response.status_code == 200:
    access_token = auth_response.json().get('access_token')
else:
    raise Exception(f"Failed to obtain access token: {auth_response.status_code} - {auth_response.text}")

headers = {
    'Authorization': f'Bearer {access_token}',
    'API-Version': 'v2',  # Add the API-Version header
    'Accept-Language': 'en',
}

def fetch_icd_codes(url):
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

def extract_codes(url):
    data = fetch_icd_codes(url)
    codes = []
    if 'child' in data:
        for child_url in data['child']:
            #print(f'defining child url: {child_url}')
            codes.extend(extract_codes(child_url))
    else:
        if 'code' in data and 'title' in data:
            # print(data['code'],data['title']['@value'])
            codes.append({
                'icd_code': data['code'],
                'icd_code_type': 'ICD-10',
                'code_description': data['title']['@value'],
                'inserted_date': current_date,
                'updated_date': current_date,
                'is_current_flag': True
            })
    return codes

# Start from the root URL
root_url = 'https://id.who.int/icd/release/10/2019/A00-A09'
icd_codes = extract_codes(root_url)


# Define the schema explicitly
schema = StructType([
    StructField("icd_code", StringType(), True),
    StructField("icd_code_type", StringType(), True),
    StructField("code_description", StringType(), True),
    StructField("inserted_date", DateType(), True),
    StructField("updated_date", DateType(), True),
    StructField("is_current_flag", BooleanType(), True)
])

#Create a DataFrame using the defined schema
print(icd_codes)
df = spark.createDataFrame(icd_codes, schema=schema)
write_file(df, file_path, read_type, mode, catalog=None, schema=None, table=None)



# COMMAND ----------

# # Databricks notebook source
# # Import necessary libraries
# import requests
# import pandas as pd
# from pyspark.sql import SparkSession
# from requests.adapters import HTTPAdapter
# from requests.packages.urllib3.util.retry import Retry

# # Initialize Spark session
# spark = SparkSession.builder.appName("CPTCodeAPI").getOrCreate()

# # API endpoint and parameters
# api_url = "https://api.cms.gov/ppl-api/codes"
# headers = {
#     "Accept": "application/json",
#     "API-Key": "YOUR_API_KEY"  # Replace with your actual API key
# }

# # Set up retry strategy
# retry_strategy = Retry(
#     total=3,
#     status_forcelist=[429, 500, 502, 503, 504],
#     method_whitelist=["HEAD", "GET", "OPTIONS"]
# )
# adapter = HTTPAdapter(max_retries=retry_strategy)
# http = requests.Session()
# http.mount("https://", adapter)

# # Fetch data from API with retry
# try:
#     response = http.get(api_url, headers=headers)
#     response.raise_for_status()  # Raise an error for bad status codes
#     data = response.json()
#     print("Data fetched successfully")
# except requests.ConnectionError as e:
#     print("Connection failed after retries:", e)
# except requests.HTTPError as e:
#     print("HTTP error occurred:", e)
# except Exception as e:
#     print("An error occurred:", e)

# # Convert to DataFrame if data is fetched successfully
# if 'data' in locals():
#     df = pd.DataFrame(data['results'])
    
#     # Convert to Spark DataFrame
#     spark_df = spark.createDataFrame(df)
    
#     # Display the Spark DataFrame
#     display(spark_df)
    
#     # Write to Delta table
#     spark_df.write.format("delta").save("/mnt/delta/cpt_codes")
# else:
#     print("No data to process")


# COMMAND ----------


