# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT logicalenv DEFAULT "prd001"

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/landing/CIS/ZIIDTT_STAGEIDOC

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/ZIIDTT_STAGEIDOC/2022/05/16

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/archvault/staging/CIS/ZIIDTT_STAGEIDOC/2022/05/16

# COMMAND ----------

# MAGIC %sh
# MAGIC mv /dbfs/mnt/archvault/landing/CIS/ZIIDTT_STAGEIDOC/2022/05/16/ZIIDTT_STAGEIDOC.D2022136.T080159227 /dbfs/mnt/landing/CIS/ZIIDTT_STAGEIDOC

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/staging/CIS/ZIIDTT_STAGEIDOC

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/staging/CIS/AFVV/*D2022136.T093346452*

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136.T093346452*

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/staging/CIS/AFVV/*T071106845*

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*T071106845*

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/staging/CIS/AFVV | wc -l

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/landing/CIS/AFVV

# COMMAND ----------

# MAGIC %sh
# MAGIC # rm /dbfs/mnt/staging/CIS/AFVV/*D2022*

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/17

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/landing/CIS/AFVV | wc -l

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T071106845*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T072111750*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T073122715*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T074147926*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T075158208*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T080159227*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T081208217*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T082209490*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T083232972*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T084244865*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T085315229*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T090319316*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T091335906*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T092341509*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T093346452*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T094446503*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T095510849*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T100606811*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T101626893*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T102718752*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T103720573*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T104817375*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T105824505*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T110835112*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T111838255*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T112904355*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T113916292*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T114955873*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T120004893*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T121007599*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T122432940*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T123557782*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T124643421*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T125748128*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T130753988*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T133931137*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T134935108*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T135936269*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T142004337*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T144015051*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T150023230*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T151029153*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T161327305*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T170533022*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T195400153*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T211008008*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T212012118*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T213039069*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T214052794*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T215119759*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T220122080*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T221127442*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T222130108*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T223151467*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T224158086*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T225159543*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T230213825*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T231218552*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T232220759*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T233224245*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T234226880*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T235231051*
# MAGIC ls -lhrt /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022137*T000241308*

# COMMAND ----------

# MAGIC %sh
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T071106845*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T072111750*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T073122715*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T074147926*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T075158208*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T080159227*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T081208217*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T082209490*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T083232972*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T084244865*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T085315229*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T090319316*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T091335906*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T092341509*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T093346452*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T094446503*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T095510849*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T100606811*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T101626893*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T102718752*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T103720573*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T104817375*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T105824505*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T110835112*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T111838255*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T112904355*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T113916292*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T114955873*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T120004893*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T121007599*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T122432940*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T123557782*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T124643421*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T125748128*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T130753988*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T133931137*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T134935108*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T135936269*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T142004337*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T144015051*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T150023230*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T151029153*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T161327305*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T170533022*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T195400153*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T211008008*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T212012118*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T213039069*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T214052794*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T215119759*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T220122080*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T221127442*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T222130108*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T223151467*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T224158086*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T225159543*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T230213825*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T231218552*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T232220759*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T233224245*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T234226880*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/16/*D2022136*T235231051*
# MAGIC rm /dbfs/mnt/archvault/landing/CIS/AFVV/2022/05/17/*D2022137*T000241308*

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/landing/CIS/AFVV/ | wc -l

# COMMAND ----------

df_tagdata_rv = spark.read.format('parquet').load('/mnt/staging/CIS/AFVV')

df_tagdata_rv.createOrReplaceTempView("temp1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct record_source from imprd001_rawvault.hub_nmi where record_source not like '%.gz%';
