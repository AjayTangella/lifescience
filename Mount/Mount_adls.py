# Databricks notebook source
dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.get("tt-hc-kv",'tt-helath-sa')

# COMMAND ----------

storageAccountName = "ajayinsurance"
storageAccountAccessKey = dbutils.secrets.get("tt-hc-kv",'tt-helath-sa')
mountPoints=["gold","silver","bronze","landing","config"]
for mountPoint in mountPoints:
    if not any(mount.mountPoint == f"/mnt/{mountPoint}" for mount in dbutils.fs.mounts()):
        try:
            dbutils.fs.mount(
            source = "wasbs://{}@{}.blob.core.windows.net".format(mountPoint, storageAccountName),
            mount_point = f"/mnt/{mountPoint}",
            extra_configs = {'fs.azure.account.key.' + storageAccountName + 'dfs.core.windows.net': storageAccountAccessKey}
            )
            print(f"{mountPoint} mount succeeded!")
        except Exception as e:
            print("mount exception", e)

dbutils.fs.mounts()

# COMMAND ----------

# 1. Set the storage account key in Spark config
spark.conf.set(
    "fs.azure.account.key.ajayinsurance.blob.core.windows.net",
    dbutils.secrets.get(scope="tt-hc-kv", key="tt-helath-sa")   # safer: fetch from secret scope
)

# 2. Read the Parquet data from ADLS Gen2
df = spark.read.format("parquet").load(
    "abfss://bronze@ajayinsurance.blob.core.windows.net/hosa/departments"
)

# 3. Show a few rows
df.show()


