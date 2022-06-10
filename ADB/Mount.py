# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

env = "dev"
mount_path = "/mnt/imrv"
store_key = dbutils.secrets.get(scope="rvstore", key="anausim{}rv01-primary".format(env))

# COMMAND ----------

# Mount a new path for RV
dbutils.fs.mount(
    source = "wasbs://streaming@anausim{}stg01.blob.core.windows.net".format(env),
    mount_point = mount_path,
    extra_configs = {"fs.azure.account.key.anausim{}stg01.blob.core.windows.net".format(env) : store_key})

# COMMAND ----------

# Update a exisitng Mount path 
dbutils.fs.updateMount(
    source = "wasbs://raw@anausim{}rv01.blob.core.windows.net".format(env),
    mount_point = mount_path,
    extra_configs = {"fs.azure.account.key.anausim{}rv01.blob.core.windows.net".format(env) : store_key})

# COMMAND ----------

# dbutils.fs.rm('dbfs:/mnt/streaming/logs',True)

# COMMAND ----------

display(dbutils.fs.ls(mount_path+'/deltalake'))

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

display(dbutils.secrets.listScopes())

# COMMAND ----------

display(dbutils.secrets.list(scope = 'rvstore'))

# COMMAND ----------

value = dbutils.secrets.get(scope="rvstore", key="anausimdevrv01-primary")

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
