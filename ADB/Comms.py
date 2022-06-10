# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text('LogicalEnv',"prd001")

# COMMAND ----------

Commssql = """select
	reportRunDateTime,
	ROW_INSERT_DTM,
	ROW_UPDATE_DTM,
	EAI_READ_STATUS,
	count(9) CNT
from
	OUTBOUND.EAI_COMMS_FAULT
where
    ROW_INSERT_DTM > dateadd(month, -2,getdate())
group by
	reportRunDateTime,
	ROW_INSERT_DTM,
	ROW_UPDATE_DTM,
	EAI_READ_STATUS
"""

# COMMAND ----------

jdbcConnstring = dbutils.secrets.get(scope = "dfstore", key='imoutboundstore-jdbc-connstring')

CommsDF = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", jdbcConnstring) \
        .option("query", Commssql) \
        .load()	 

display(CommsDF.select("reportRunDateTime","ROW_INSERT_DTM","ROW_UPDATE_DTM","EAI_READ_STATUS","CNT").sort(['reportRunDateTime'], ascending=[False]))

# COMMAND ----------

Checksql = """select * from INFORMATION_SCHEMA.TABLES"""

# COMMAND ----------

jdbcConnstring = dbutils.secrets.get(scope = "dfstore", key='imoutboundstore-jdbc-connstring')

CheckDF = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", jdbcConnstring) \
        .option("query", Checksql) \
        .load()	 

display(CheckDF)

# COMMAND ----------

jdbcConnstring = dbutils.secrets.get(scope = "dfstore", key='imrealtimestore-jdbc-connstring')

CheckDF = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", jdbcConnstring) \
        .option("query", Checksql) \
        .load()	 

display(CheckDF)

# COMMAND ----------

jdbcConnstring = dbutils.secrets.get(scope = "pgsql-spatial-store", key='ipgsql-jdbc-rawspatialstore-sql-connstring')

CheckDF = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", jdbcConnstring) \
        .option("query", Checksql) \
        .load()	 

display(CheckDF)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

import adal

# COMMAND ----------

dict = {}
dict['tenant_id'] = 'a394e41c-cf8d-458e-ac1b-ddae1aa15629'
dict['authority'] = 'https://login.microsoftonline.com/'
dict['resource_app_id_url'] = 'https://database.windows.net/'
dict['service_principal_id'] = 'cdfcb7f2-0bc0-41f5-b54a-69fa069b2b41'
dict['service_principal_scope'] = 'eventhub-store'
dict['service_principal_key'] = 'svc-adb-aus-prd-im-01-azsql'
dict['jdbc_url'] = 'jdbc:sqlserver://sql-aus-prd-im-01.database.windows.net'
dict['database_name'] = 'IM-PRD-RealTimeStore'
dict['sql_server_encrypt'] = True
dict['sql_server_host_name_in_certificate'] = '"*.database.windows.net"'

tenant_id = dict['tenant_id']
authority = dict['authority'] + tenant_id
resource_app_id_url = dict['resource_app_id_url']
service_principal_id = dict['service_principal_id']
service_principal_secret = dbutils.secrets.get(scope = dict['service_principal_scope'], key = dict['service_principal_key'])

# SQL Server URL
url = dict['jdbc_url']
database_name = dict['database_name']
#db_table = dict['tagdata']['sql_table']

encrypt = dict['sql_server_encrypt']
host_name_in_certificate = dict['sql_server_host_name_in_certificate']

context = adal.AuthenticationContext(authority)

token = context.acquire_token_with_client_credentials(resource_app_id_url, service_principal_id, service_principal_secret)
access_token = token["accessToken"]


# COMMAND ----------

url='jdbc:sqlserver://sql-aus-prd-im-01.database.windows.net'
database_name='IM-PRD-RealTimeStore'

CheckDF = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("query", Checksql) \
        .option("accessToken", access_token)\
        .option("encrypt", encrypt)\
        .option("databaseName", database_name)\
        .load()	 

display(CheckDF)

# COMMAND ----------

dbutils.fs.mounts()
