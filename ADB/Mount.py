# Databricks notebook source
env='prd'
stg_store_keys = dbutils.secrets.get(scope="stgstore", key="anausim{}stg01-primary".format(env))
stg_store_keys

# COMMAND ----------

exisiting_mount_points = [mount_point.mountPoint for mount_point in dbutils.fs.mounts()]
exisiting_mount_points

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

env = "prd"
streaming_mount_path = "/mnt/streaming"

# COMMAND ----------

if streaming_mount_path not in exisiting_mount_points:
  dbutils.fs.mount(
    source = "wasbs://streaming@anausim{}stg01.blob.core.windows.net".format(env),
    mount_point = streaming_mount_path,
    extra_configs = {"fs.azure.account.key.anausim{}stg01.blob.core.windows.net".format(env) : stg_store_keys})

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/streaming/')

# COMMAND ----------

# dbutils.fs.rm('dbfs:/mnt/streaming/logs',True)

# COMMAND ----------

dbutils.fs.ls('/mnt/streaming')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

display(dbutils.secrets.list(scope = 'astimdevlake_scope'))

# COMMAND ----------

display(dbutils.secrets.list(scope = 'fsstore'))

# COMMAND ----------

 dbutils.secrets.get(scope="fsstore", key="fsstore-storage-account")

# COMMAND ----------

value = dbutils.secrets.get(scope="fsstore", key="fsstore-keyvault")

print('keyvault')
for char in value:
    print(char, end=" ")

print('\nmount')    
value = dbutils.secrets.get(scope="fsstore", key="fsstore-mount")

for char in value:
    print(char, end=" ")

print('\naccount')     
value = dbutils.secrets.get(scope="fsstore", key="fsstore-storage-account")

for char in value:
    print(char, end=" ")    

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/mlcurated
