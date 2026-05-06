# Databricks notebook source
Azure_tenant_id=dbutils.secrets.get('tt-hc-kv','tenantid')
App_id=dbutils.secrets.get('tt-hc-kv','Appid')
Azuresecrets=dbutils.secrets.get('tt-hc-kv','Azuresecrets')

# COMMAND ----------

spark.conf.unset("fs.azure.account.key.ajayinsurance.dfs.core.windows.net")

# OAuth config
spark.conf.set("fs.azure.account.auth.type.ajayinsurance.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.ajayinsurance.dfs.core.windows.net", 
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.ajayinsurance.dfs.core.windows.net", App_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.ajayinsurance.dfs.core.windows.net", Azuresecrets)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.ajayinsurance.dfs.core.windows.net", 
               f"https://login.microsoftonline.com/Azure_tenant_id/oauth2/token")

