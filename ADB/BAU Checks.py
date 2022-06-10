# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT logicalenv DEFAULT "prd001"

# COMMAND ----------

# MAGIC %sql
# MAGIC select T1.*
# MAGIC FROM imprd001_rawvault.LSAT_NMI_SUBSTATION_POF_NMI_TO_SUBSTATION_DATA T1
# MAGIC JOIN imprd001_rawvault.LNK_NMI_SUBSTATION LNK on T1.LNK_NMI_SUBSTATION_HSH_KEY=LNK.LNK_NMI_SUBSTATION_HSH_KEY
# MAGIC JOIN imprd001_rawvault.HUB_NMI hub
# MAGIC on hub.HUB_NMI_HSH_KEY = LNK.HUB_NMI_HSH_KEY
# MAGIC AND hub.NMI ='6306057313'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended im${logicalenv}_rawvault.SAT_OSIPI_TAGDATA_HIST

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing/PolicyNet/AMI_READ_POWER_QUALITY2/

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC -- Get NMI 
# MAGIC EUITRANS_V1 as(
# MAGIC      select hub_nmi.NMI
# MAGIC           , salnk.HUB_NMI_HSH_KEY2
# MAGIC        from (select  sat.*
# MAGIC                     ,ROW_NUMBER() OVER ( partition BY sat.SALNK_NMI_HSH_KEY, sat.DATEFROM ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
# MAGIC                from im${logicalenv}_rawvault.LSAT_NMI_CIS_EUITRANS sat
# MAGIC               --FOR Each valid NMI there must be a check-sum value   
# MAGIC 			  where length(trim(sat.NMI_CHECKSUM)) > 0
# MAGIC             )EUITRANS
# MAGIC        join im${logicalenv}_rawvault.SALNK_NMI salnk
# MAGIC          on EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
# MAGIC        join im${logicalenv}_rawvault.HUB_NMI hub_nmi
# MAGIC          on salnk.HUB_NMI_HSH_KEY1 = hub_nmi.HUB_NMI_HSH_KEY
# MAGIC       where EUITRANS.rec_seq = 1
# MAGIC         and EUITRANS.CHANGE_TYPE <> 'D'
# MAGIC      ),
# MAGIC EUIINSTLN_V2 as
# MAGIC (
# MAGIC select EUIINSTLN.*
# MAGIC      , salnk.HUB_NMI_HSH_KEY1
# MAGIC      , salnk.HUB_NMI_HSH_KEY2
# MAGIC 
# MAGIC   from (select sat.*, 
# MAGIC               ROW_NUMBER() OVER ( partition BY SALNK_NMI_HSH_KEY, sat.DATEFROM ORDER BY sat.extract_date_time DESC ) as `rec_seq`
# MAGIC           from im${logicalenv}_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
# MAGIC        )EUIINSTLN
# MAGIC   join im${logicalenv}_rawvault.SALNK_NMI salnk
# MAGIC     on EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
# MAGIC  where EUIINSTLN.rec_seq = 1
# MAGIC    and EUIINSTLN.CHANGE_TYPE <> 'D'
# MAGIC ),
# MAGIC EASTS_V3 as
# MAGIC (
# MAGIC   select lnk.HUB_NMI_HSH_KEY
# MAGIC        , lnk.HUB_EQUIPMENT_HSH_KEY
# MAGIC        , EASTS.*
# MAGIC     from (select sat.*
# MAGIC              , ROW_NUMBER() OVER ( partition BY sat.LNK_NMI_EQUIPMENT_HSH_KEY, sat.AB ORDER BY sat.extract_date_time DESC) as `rec_seq`
# MAGIC             from im${logicalenv}_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTS sat
# MAGIC          )EASTS
# MAGIC     join im${logicalenv}_rawvault.LNK_NMI_EQUIPMENT lnk
# MAGIC       on EASTS.LNK_NMI_EQUIPMENT_HSH_KEY = lnk.LNK_NMI_EQUIPMENT_HSH_KEY
# MAGIC     where EASTS.rec_seq = 1
# MAGIC       and EASTS.CHANGE_TYPE <> 'D'
# MAGIC ),
# MAGIC -- GET suffix and timeline information 
# MAGIC ETDZ_V4 as
# MAGIC (select ETDZ.KENNZIFF
# MAGIC       , ETDZ.AB
# MAGIC       , ETDZ.BIS
# MAGIC       , ETDZ.AEDAT
# MAGIC       , salnk.HUB_EQUIPMENT_HSH_KEY2,
# MAGIC       ETDZ.*
# MAGIC    from (select sat.*
# MAGIC               , ROW_NUMBER() OVER ( partition BY sat.SALNK_EQUIPMENT_HSH_KEY, sat.ZWNUMMER ORDER BY sat.EXTRACT_DATE_TIME DESC) as `rec_seq`
# MAGIC            from im${logicalenv}_rawvault.LSAT_EQUIPMENT_CIS_ETDZ sat
# MAGIC           where ( Upper(KENNZIFF) like 'E%'
# MAGIC                or Upper(KENNZIFF) like 'B%'
# MAGIC                 )
# MAGIC --             Register type - C is for Current    
# MAGIC --             and ZWART = 'C'     
# MAGIC          )ETDZ
# MAGIC    join im${logicalenv}_rawvault.SALNK_EQUIPMENT salnk
# MAGIC      on ETDZ.SALNK_EQUIPMENT_HSH_KEY = salnk.SALNK_EQUIPMENT_HSH_KEY   
# MAGIC --   where ETDZ.rec_seq = 1  AND ETDZ.AB < ETDZ.BIS
# MAGIC )
# MAGIC SELECT DISTINCT EUITRANS_V1.NMI AS NMI,
# MAGIC 	ETDZ_V4.KENNZIFF AS NMI_SUFFIX,
# MAGIC 	min(ETDZ_V4.AB) OVER (
# MAGIC 		PARTITION BY EUITRANS_V1.NMI,
# MAGIC 		ETDZ_V4.KENNZIFF,
# MAGIC 		ETDZ_V4.BIS ORDER BY ETDZ_V4.AB
# MAGIC 		) AS FROM_DT,
# MAGIC 	max(ETDZ_V4.BIS) OVER (
# MAGIC 		PARTITION BY EUITRANS_V1.NMI,
# MAGIC 		ETDZ_V4.KENNZIFF,
# MAGIC 		ETDZ_V4.AB ORDER BY ETDZ_V4.AB
# MAGIC 		) AS TO_DT,
# MAGIC 	date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM,
# MAGIC      ETDZ_V4.*
# MAGIC FROM EUITRANS_V1
# MAGIC INNER JOIN EUIINSTLN_V2
# MAGIC 	ON EUITRANS_V1.HUB_NMI_HSH_KEY2 = EUIINSTLN_V2.HUB_NMI_HSH_KEY2
# MAGIC INNER JOIN EASTS_V3
# MAGIC 	ON EUIINSTLN_V2.HUB_NMI_HSH_KEY1 = EASTS_V3.HUB_NMI_HSH_KEY
# MAGIC INNER JOIN ETDZ_V4
# MAGIC 	ON EASTS_V3.HUB_EQUIPMENT_HSH_KEY = ETDZ_V4.HUB_EQUIPMENT_HSH_KEY2                                                         
# MAGIC where EUITRANS_V1.NMI in  ('6305150860' , '6305158946') order by 1, 2, 3

# COMMAND ----------

# MAGIC %sql
# MAGIC with 
# MAGIC -- Get NMI 
# MAGIC EUITRANS_V1 as(
# MAGIC      select hub_nmi.NMI
# MAGIC           , salnk.HUB_NMI_HSH_KEY2
# MAGIC        from (select  sat.*
# MAGIC                     ,ROW_NUMBER() OVER ( partition BY sat.SALNK_NMI_HSH_KEY, sat.DATEFROM ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
# MAGIC                from im${logicalenv}_rawvault.LSAT_NMI_CIS_EUITRANS sat
# MAGIC               --FOR Each valid NMI there must be a check-sum value   
# MAGIC 			  where length(trim(sat.NMI_CHECKSUM)) > 0
# MAGIC             )EUITRANS
# MAGIC        join im${logicalenv}_rawvault.SALNK_NMI salnk
# MAGIC          on EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
# MAGIC        join im${logicalenv}_rawvault.HUB_NMI hub_nmi
# MAGIC          on salnk.HUB_NMI_HSH_KEY1 = hub_nmi.HUB_NMI_HSH_KEY
# MAGIC       where EUITRANS.rec_seq = 1
# MAGIC         and EUITRANS.CHANGE_TYPE <> 'D'
# MAGIC      ),
# MAGIC EUIINSTLN_V2 as
# MAGIC (
# MAGIC select EUIINSTLN.*
# MAGIC      , salnk.HUB_NMI_HSH_KEY1
# MAGIC      , salnk.HUB_NMI_HSH_KEY2
# MAGIC 
# MAGIC   from (select sat.*, 
# MAGIC               ROW_NUMBER() OVER ( partition BY SALNK_NMI_HSH_KEY, sat.DATEFROM ORDER BY sat.extract_date_time DESC ) as `rec_seq`
# MAGIC           from im${logicalenv}_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
# MAGIC        )EUIINSTLN
# MAGIC   join im${logicalenv}_rawvault.SALNK_NMI salnk
# MAGIC     on EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
# MAGIC  where EUIINSTLN.rec_seq = 1
# MAGIC    and EUIINSTLN.CHANGE_TYPE <> 'D'
# MAGIC ),
# MAGIC EASTS_V3 as
# MAGIC (
# MAGIC   select lnk.HUB_NMI_HSH_KEY
# MAGIC        , lnk.HUB_EQUIPMENT_HSH_KEY
# MAGIC        , EASTS.*
# MAGIC     from (select sat.*
# MAGIC              , ROW_NUMBER() OVER ( partition BY sat.LNK_NMI_EQUIPMENT_HSH_KEY, sat.AB ORDER BY sat.extract_date_time DESC) as `rec_seq`
# MAGIC             from im${logicalenv}_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTS sat
# MAGIC          )EASTS
# MAGIC     join im${logicalenv}_rawvault.LNK_NMI_EQUIPMENT lnk
# MAGIC       on EASTS.LNK_NMI_EQUIPMENT_HSH_KEY = lnk.LNK_NMI_EQUIPMENT_HSH_KEY
# MAGIC     where EASTS.rec_seq = 1
# MAGIC       and EASTS.CHANGE_TYPE <> 'D'
# MAGIC ),
# MAGIC -- GET suffix and timeline information 
# MAGIC ETDZ_V4 as
# MAGIC (select ETDZ.KENNZIFF
# MAGIC       , ETDZ.AB
# MAGIC       , ETDZ.BIS
# MAGIC       , ETDZ.AEDAT
# MAGIC       , salnk.HUB_EQUIPMENT_HSH_KEY2
# MAGIC    from (select sat.*
# MAGIC               , ROW_NUMBER() OVER ( partition BY sat.SALNK_EQUIPMENT_HSH_KEY, sat.ZWNUMMER, sat.AB ORDER BY sat.EXTRACT_DATE_TIME DESC) as `rec_seq`
# MAGIC            from im${logicalenv}_rawvault.LSAT_EQUIPMENT_CIS_ETDZ sat
# MAGIC           where ( Upper(KENNZIFF) like 'E%'
# MAGIC                or Upper(KENNZIFF) like 'B%'
# MAGIC                 )
# MAGIC             -- Register type - C is for Current    
# MAGIC             and ZWART = 'C'     
# MAGIC          )ETDZ
# MAGIC    join im${logicalenv}_rawvault.SALNK_EQUIPMENT salnk
# MAGIC      on ETDZ.SALNK_EQUIPMENT_HSH_KEY = salnk.SALNK_EQUIPMENT_HSH_KEY   
# MAGIC   where ETDZ.rec_seq = 1
# MAGIC   AND ETDZ.AB < ETDZ.BIS
# MAGIC )
# MAGIC SELECT DISTINCT EUITRANS_V1.NMI AS NMI,
# MAGIC 	ETDZ_V4.KENNZIFF AS NMI_SUFFIX,
# MAGIC 	min(ETDZ_V4.AB) OVER (
# MAGIC 		PARTITION BY EUITRANS_V1.NMI,
# MAGIC 		ETDZ_V4.KENNZIFF,
# MAGIC 		ETDZ_V4.BIS ORDER BY ETDZ_V4.AB
# MAGIC 		) AS FROM_DT,
# MAGIC 	max(ETDZ_V4.BIS) OVER (
# MAGIC 		PARTITION BY EUITRANS_V1.NMI,
# MAGIC 		ETDZ_V4.KENNZIFF,
# MAGIC 		ETDZ_V4.AB ORDER BY ETDZ_V4.AB
# MAGIC 		) AS TO_DT,
# MAGIC 	date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
# MAGIC FROM EUITRANS_V1
# MAGIC INNER JOIN EUIINSTLN_V2
# MAGIC 	ON EUITRANS_V1.HUB_NMI_HSH_KEY2 = EUIINSTLN_V2.HUB_NMI_HSH_KEY2
# MAGIC INNER JOIN EASTS_V3
# MAGIC 	ON EUIINSTLN_V2.HUB_NMI_HSH_KEY1 = EASTS_V3.HUB_NMI_HSH_KEY
# MAGIC INNER JOIN ETDZ_V4
# MAGIC 	ON EASTS_V3.HUB_EQUIPMENT_HSH_KEY = ETDZ_V4.HUB_EQUIPMENT_HSH_KEY2                                                         
# MAGIC where EUITRANS_V1.NMI in  ('6305150860' , '6305158946') order by 1, 2, 3

# COMMAND ----------

# MAGIC %fs ls /mnt/imlake/staging/DOMS/INC_INCIDENT/

# COMMAND ----------

# MAGIC %fs ls /mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/

# COMMAND ----------

df_tagdata_rv = spark.read.format('parquet').load('/mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/')

df_tagdata_rv.createOrReplaceTempView("temp1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp1 

# COMMAND ----------

# MAGIC %fs ls /mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/AMI_READ_POWER_QUALITY2* | wc -l

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/AMI_READ_POWER_QUALITY2.D202212* | wc -l
# MAGIC mv /dbfs/mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/AMI_READ_POWER_QUALITY2* /dbfs/mnt/staging/PolicyNet/POWER_BKUP/

# COMMAND ----------

# MAGIC %sh
# MAGIC mv /dbfs/mnt/staging/PolicyNet/POWER_BKUP/AMI_READ_POWER_QUALITY2.* /dbfs/mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/AMI_READ_POWER_QUALITY2* | wc -l

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/staging/PolicyNet/AMI_READ_POWER_QUALITY2/AMI_READ_POWER_QUALITY2* | wc -l

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables 

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE HISTORY 'dbfs:/mnt/imrv/deltalake/imprd001/rawvault/SAT_PQ_SIQ_DEVICE_EVENT/'    LIMIT 100      -- get the full history of the table

# COMMAND ----------

# MAGIC %sql
# MAGIC describe HISTORY imprd001_rawvault.SAT_PQ_SIQ_DEVICE_EVENT

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL imprd001_rawvault.SAT_PQ_SIQ_DEVICE_EVENT

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE imprd001_rawvault.SAT_PQ_SIQ_DEVICE_EVENT UNSET TBLPROPERTIES IF EXISTS ('delta.autoOptimize.autoCompact', 'delta.autoOptimize.optimizeWrite')

# COMMAND ----------

# MAGIC %sql 
# MAGIC ALTER TABLE delta.`/mnt/imrv/deltalake/imprd001/rawvault/SAT_PQ_SIQ_DEVICE_EVENT/` UNSET TBLPROPERTIES IF EXISTS ('delta.autoOptimize.autoCompact', 'delta.autoOptimize.optimizeWrite')

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- =============================================
# MAGIC -- Author:      Sainath 
# MAGIC -- Create Date: 23/08/2021
# MAGIC -- Description: Extracts EIP Standing Data for MHE feed
# MAGIC --              Time Slice based on NMI, Meter, Suffix, Channel
# MAGIC -- =============================================
# MAGIC WITH EIP_SVC_PT_V1
# MAGIC 	-- Get Service Point Details
# MAGIC AS (
# MAGIC 	SELECT hub1.HUB_NMI_HSH_KEY AS HUB_NMI_HSH_KEY,
# MAGIC 		hub1.NMI AS SVC_PT_ID,
# MAGIC 		hub.NMI AS NMI,
# MAGIC 		SVC_PT.*
# MAGIC 	FROM (
# MAGIC 		SELECT sat.*,
# MAGIC 			ROW_NUMBER() OVER (
# MAGIC 				PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
# MAGIC 				) AS `rec_seq`
# MAGIC 		FROM im${logicalenv}_rawvault.LSAT_NMI_EIP_SVC_PT sat
# MAGIC 		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
# MAGIC 		) SVC_PT
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk ON SVC_PT.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_NMI AS hub ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_NMI AS hub1 ON salnk.HUB_NMI_HSH_KEY2 = hub1.HUB_NMI_HSH_KEY
# MAGIC 	WHERE SVC_PT.rec_seq = 1
# MAGIC 		AND SVC_PT.CHANGE_TYPE != 'D'
# MAGIC 		AND hub.NMI in ('6305434641')
# MAGIC 	),
# MAGIC SVC_PT_DEVICE_REL_V2
# MAGIC AS (
# MAGIC 	-- Get Service Point to Device Relationship 
# MAGIC 	SELECT hub_nmi.HUB_NMI_HSH_KEY,
# MAGIC 		hub_nmi.NMI,
# MAGIC 		hub_dvc.DEVICE_ID,
# MAGIC 		hub_dvc.HUB_DEVICE_HSH_KEY,
# MAGIC 		eip_spd_rel.EFF_START_TIME,
# MAGIC 		nvl(eip_spd_rel.EFF_END_TIME, to_timestamp('9999-12-31')) AS EFF_END_TIME
# MAGIC 	FROM (
# MAGIC 		SELECT sat.*,
# MAGIC 			ROW_NUMBER() OVER (
# MAGIC 				PARTITION BY LNK_NMI_DEVICE_HSH_KEY,
# MAGIC 				sat.EFF_START_TIME ORDER BY extract_date_time DESC
# MAGIC 				) AS `rec_seq`
# MAGIC 		FROM im${logicalenv}_rawvault.LSAT_NMI_DEVICE_EIP_SVC_PT_DEVICE_REL sat
# MAGIC 		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
# MAGIC 		) eip_spd_rel
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_DEVICE lnk ON eip_spd_rel.LNK_NMI_DEVICE_HSH_KEY = lnk.LNK_NMI_DEVICE_HSH_KEY
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hub_dvc ON lnk.HUB_DEVICE_HSH_KEY = hub_dvc.HUB_DEVICE_HSH_KEY
# MAGIC 	WHERE rec_seq = 1
# MAGIC 		AND eip_spd_rel.CHANGE_TYPE != 'D'
# MAGIC 	),
# MAGIC EIP_DEVICE_V3
# MAGIC AS (
# MAGIC 	-- Get Device Details
# MAGIC 	SELECT hubdev.HUB_DEVICE_HSH_KEY,
# MAGIC 		hubdev.DEVICE_ID,
# MAGIC 		eip_device.MFG_SERIAL_NUM,
# MAGIC 		eip_device.STATUS_CD
# MAGIC 	FROM (
# MAGIC 		SELECT sat.*,
# MAGIC 			ROW_NUMBER() OVER (
# MAGIC 				PARTITION BY sat.HUB_DEVICE_HSH_KEY ORDER BY sat.extract_date_time DESC
# MAGIC 				) AS `rec_seq`
# MAGIC 		FROM im${logicalenv}_rawvault.SAT_DEVICE_EIP_DEVICE_DETAILS sat
# MAGIC 		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
# MAGIC 		) eip_device
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hubdev ON eip_device.HUB_DEVICE_HSH_KEY = hubdev.HUB_DEVICE_HSH_KEY
# MAGIC 	WHERE eip_device.rec_seq = 1
# MAGIC 	),
# MAGIC CHANNEL_DETAILS_V4
# MAGIC AS (
# MAGIC 	-- Get channel Details
# MAGIC 	SELECT hub_chan.HUB_CHANNEL_HSH_KEY,
# MAGIC 		hub_chan.CHANNEL_ID,
# MAGIC 		hub_nmi.HUB_NMI_HSH_KEY,
# MAGIC 		eip_channel.TYPE AS CHANNEL_TYPE,
# MAGIC 		eip_channel.NAME AS CHANNEL_NAME,
# MAGIC 		eip_channel.INTERVAL_LEN,
# MAGIC 		eip_channel.IS_VIRTUAL_FLG,
# MAGIC 		eip_channel.STATUS_CD,
# MAGIC 		hub_nmi.NMI AS SVC_PT_ID,
# MAGIC 		eip_channel.UDC_ID
# MAGIC 	FROM (
# MAGIC 		SELECT sat.*,
# MAGIC 			ROW_NUMBER() OVER (
# MAGIC 				PARTITION BY LNK_NMI_CHANNEL_HSH_KEY ORDER BY extract_date_time DESC
# MAGIC 				) AS `rec_seq`
# MAGIC 		FROM im${logicalenv}_rawvault.LSAT_NMI_CHANNEL_EIP_CHANNEL_DETAILS sat
# MAGIC 		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
# MAGIC 			AND TYPE = 'Interval Data'
# MAGIC 		) eip_channel
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_CHANNEL lnk ON eip_channel.LNK_NMI_CHANNEL_HSH_KEY = lnk.LNK_NMI_CHANNEL_HSH_KEY
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hub_chan ON lnk.HUB_CHANNEL_HSH_KEY = hub_chan.HUB_CHANNEL_HSH_KEY
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
# MAGIC 	WHERE rec_seq = 1
# MAGIC 		AND eip_channel.CHANGE_TYPE != 'D'
# MAGIC 	),
# MAGIC DEVICE_CHANNEL_REL_V5
# MAGIC AS (
# MAGIC 	-- Get Device to Channel Details
# MAGIC 	SELECT hub_chan.HUB_CHANNEL_HSH_KEY,
# MAGIC 		hub_chan.CHANNEL_ID,
# MAGIC 		hub_dvc.DEVICE_ID,
# MAGIC 		hub_dvc.HUB_DEVICE_HSH_KEY,
# MAGIC 		eip_channel_rel.EFF_START_TIME,
# MAGIC 		nvl(eip_channel_rel.EFF_END_TIME, to_timestamp('9999-12-31')) AS EFF_END_TIME
# MAGIC 	FROM (
# MAGIC 		SELECT sat.*,
# MAGIC 			ROW_NUMBER() OVER (
# MAGIC 				PARTITION BY LNK_DEVICE_CHANNEL_HSH_KEY,
# MAGIC 				sat.EFF_START_TIME ORDER BY extract_date_time DESC
# MAGIC 				) AS `rec_seq`
# MAGIC 		FROM im${logicalenv}_rawvault.LSAT_DEVICE_CHANNEL_EIP_DEVICE_CHANNEL_REL sat
# MAGIC 		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
# MAGIC 		) eip_channel_rel
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.LNK_DEVICE_CHANNEL lnk ON eip_channel_rel.LNK_DEVICE_CHANNEL_HSH_KEY = lnk.LNK_DEVICE_CHANNEL_HSH_KEY
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hub_chan ON lnk.HUB_CHANNEL_HSH_KEY = hub_chan.HUB_CHANNEL_HSH_KEY
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hub_dvc ON lnk.HUB_DEVICE_HSH_KEY = hub_dvc.HUB_DEVICE_HSH_KEY
# MAGIC 	WHERE rec_seq = 1
# MAGIC 		AND eip_channel_rel.CHANGE_TYPE != 'D'
# MAGIC 	),
# MAGIC EIP_CHANEEL_PARAM_V6
# MAGIC AS (
# MAGIC 	-- Get Channel Parameters
# MAGIC 	SELECT hubchan.HUB_CHANNEL_HSH_KEY,
# MAGIC 		hubchan.CHANNEL_ID,
# MAGIC 		eip_channel_param.NAME AS NAME,
# MAGIC 		eip_channel_param.VALUE AS VALUE,
# MAGIC 		eip_channel_param.EFF_START_TIME AS EFF_START_TIME,
# MAGIC 		nvl(eip_channel_param.EFF_END_TIME, to_timestamp('9999-12-31')) AS EFF_END_TIME
# MAGIC 	FROM (
# MAGIC 		SELECT sat.*,
# MAGIC 			ROW_NUMBER() OVER (
# MAGIC 				PARTITION BY sat.HUB_CHANNEL_HSH_KEY,
# MAGIC 				sat.ID,
# MAGIC                 sat.NAME
# MAGIC 				ORDER BY EXTRACT_DATE_TIME DESC
# MAGIC 				) AS `rec_seq`
# MAGIC 		--used record version column from the data in sorting to get the latest records
# MAGIC 		FROM im${logicalenv}_rawvault.SAT_CHANNEL_EIP_CHANNEL_PARAM sat
# MAGIC 		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
# MAGIC 			AND upper(trim(sat.NAME)) = 'DATA STREAM SUFFIX'
# MAGIC 			-- Take only Data Stream Suffix Channel which is required for MHE
# MAGIC 		) eip_channel_param
# MAGIC 	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hubchan ON eip_channel_param.HUB_CHANNEL_HSH_KEY = hubchan.HUB_CHANNEL_HSH_KEY
# MAGIC 	WHERE eip_channel_param.rec_seq = 1
# MAGIC 		AND eip_channel_param.CHANGE_TYPE != 'D'
# MAGIC 	),
# MAGIC date_lst1
# MAGIC AS (
# MAGIC 	SELECT DISTINCT espv.NMI,
# MAGIC 		stack(99, cast(spdl.EFF_START_TIME AS DATE), CASE 
# MAGIC 				WHEN cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
# MAGIC 					THEN cast('9999-12-31' AS DATE)
# MAGIC 				ELSE date_sub(cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
# MAGIC 				END, cast(dcr.EFF_START_TIME AS DATE), CASE 
# MAGIC 				WHEN cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
# MAGIC 					THEN cast('9999-12-31' AS DATE)
# MAGIC 				ELSE date_sub(cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
# MAGIC 				END, cast(ecpv.EFF_START_TIME AS DATE), CASE 
# MAGIC 				WHEN cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
# MAGIC 					THEN cast('9999-12-31' AS DATE)
# MAGIC 				ELSE date_sub(cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
# MAGIC 				END) AS DT_LST
# MAGIC 	FROM EIP_SVC_PT_V1 AS espv
# MAGIC 	INNER JOIN SVC_PT_DEVICE_REL_V2 AS spdl ON spdl.NMI = espv.SVC_PT_ID
# MAGIC 	INNER JOIN EIP_DEVICE_V3 AS edev ON spdl.DEVICE_ID = edev.DEVICE_ID
# MAGIC 	INNER JOIN DEVICE_CHANNEL_REL_V5 dcr ON edev.DEVICE_ID = dcr.DEVICE_ID
# MAGIC 	INNER JOIN CHANNEL_DETAILS_V4 cd ON dcr.CHANNEL_ID = cd.CHANNEL_ID
# MAGIC 		AND spdl.NMI = cd.SVC_PT_ID
# MAGIC 	INNER JOIN EIP_CHANEEL_PARAM_V6 ecpv ON cd.CHANNEL_ID = ecpv.CHANNEL_ID
# MAGIC 		/*WHERE espv.NMI IN (
# MAGIC 			'6305109416', '6305499448'
# MAGIC 			)*/
# MAGIC 	),
# MAGIC date_lst2
# MAGIC AS (
# MAGIC 	SELECT NMI,
# MAGIC 		DT_LST AS FROM_DT,
# MAGIC 		LEAD(DT_LST, 1) OVER (
# MAGIC 			PARTITION BY NMI ORDER BY DT_LST
# MAGIC 			) TO_DT
# MAGIC 	FROM date_lst1
# MAGIC 	),
# MAGIC date_lst3
# MAGIC AS (
# MAGIC 	SELECT NMI,
# MAGIC 		to_date(FROM_DT, "yyyyMMdd") AS FROM_DT,
# MAGIC 		CASE 
# MAGIC 			WHEN lead(to_date(FROM_DT, "yyyyMMdd")) OVER (
# MAGIC 					PARTITION BY NMI ORDER BY FROM_DT
# MAGIC 					) = to_date(TO_DT, "yyyyMMdd")
# MAGIC 				THEN date_sub(to_date(TO_DT, "yyyyMMdd"), 1)
# MAGIC 			ELSE to_date(TO_DT, "yyyyMMdd")
# MAGIC 			END AS TO_DT
# MAGIC 	FROM date_lst2
# MAGIC 	WHERE datediff(to_date(TO_DT, "yyyyMMdd"), to_date(FROM_DT, "yyyyMMdd")) > 1
# MAGIC 	),
# MAGIC 	-- Timeline framing end --    
# MAGIC date_lst4
# MAGIC AS (
# MAGIC 	SELECT espv.NMI,
# MAGIC 		edev.MFG_SERIAL_NUM AS METER_NUM,
# MAGIC 		ecpv.VALUE AS NMI_SUFFIX,
# MAGIC 		cd.CHANNEL_ID,
# MAGIC 		cd.IS_VIRTUAL_FLG,
# MAGIC 		dl.FROM_DT,
# MAGIC 		dl.TO_DT,
# MAGIC 		ROW_NUMBER() OVER (
# MAGIC 			PARTITION BY espv.NMI,
# MAGIC 			edev.MFG_SERIAL_NUM,
# MAGIC 			ecpv.VALUE,
# MAGIC             dl.FROM_DT
# MAGIC             ORDER BY cd.CHANNEL_ID
# MAGIC 			) AS RNO,
# MAGIC 		date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
# MAGIC 	FROM EIP_SVC_PT_V1 AS espv
# MAGIC 	INNER JOIN date_lst3 dl ON (espv.NMI = dl.NMI)
# MAGIC 	INNER JOIN SVC_PT_DEVICE_REL_V2 AS spdl ON (
# MAGIC 			spdl.NMI = espv.SVC_PT_ID
# MAGIC 			AND CASE 
# MAGIC 				WHEN cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
# MAGIC 					THEN cast('9999-12-31' AS DATE)
# MAGIC 				ELSE date_sub(cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
# MAGIC 				END > dl.FROM_DT
# MAGIC 			AND cast(spdl.EFF_START_TIME AS DATE) < dl.TO_DT
# MAGIC 			)
# MAGIC 	INNER JOIN EIP_DEVICE_V3 AS edev ON spdl.DEVICE_ID = edev.DEVICE_ID
# MAGIC 	INNER JOIN DEVICE_CHANNEL_REL_V5 dcr ON (
# MAGIC 			edev.DEVICE_ID = dcr.DEVICE_ID
# MAGIC 			AND CASE 
# MAGIC 				WHEN cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
# MAGIC 					THEN cast('9999-12-31' AS DATE)
# MAGIC 				ELSE date_sub(cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
# MAGIC 				END > dl.FROM_DT
# MAGIC 			AND cast(dcr.EFF_START_TIME AS DATE) < dl.TO_DT
# MAGIC 			)
# MAGIC 	INNER JOIN CHANNEL_DETAILS_V4 cd ON dcr.CHANNEL_ID = cd.CHANNEL_ID
# MAGIC 		AND spdl.NMI = cd.SVC_PT_ID
# MAGIC 		AND upper(trim(cd.IS_VIRTUAL_FLG)) = 'N'
# MAGIC 	INNER JOIN EIP_CHANEEL_PARAM_V6 ecpv ON (
# MAGIC 			cd.CHANNEL_ID = ecpv.CHANNEL_ID
# MAGIC 			AND CASE 
# MAGIC 				WHEN cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
# MAGIC 					THEN cast('9999-12-31' AS DATE)
# MAGIC 				ELSE date_sub(cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
# MAGIC 				END > dl.FROM_DT
# MAGIC 			AND cast(ecpv.EFF_START_TIME AS DATE) < dl.TO_DT
# MAGIC 			)
# MAGIC 	)
# MAGIC SELECT NMI,
# MAGIC 	METER_NUM,
# MAGIC 	NMI_SUFFIX,
# MAGIC 	CHANNEL_ID,
# MAGIC 	IS_VIRTUAL_FLG,
# MAGIC 	FROM_DT,
# MAGIC 	TO_DT,
# MAGIC 	ROW_INSERT_DTM
# MAGIC FROM date_lst4
# MAGIC WHERE RNO = 1 -- Handle duplicate channel id for same nmi, meter, suffix

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize im${logicalenv}_rawvault.hub_srvorder

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from imprd001_rawvault like '*OSIPI*'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_rawvault.sat_osipi_tagdata

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended imprd001_rawvault.sat_osipi_tagdata

# COMMAND ----------

# MAGIC %sh
# MAGIC du -sh /dbfs/mnt/imrv/deltalake/imprd001/rawvault/SAT_OSIPI_*

# COMMAND ----------



# COMMAND ----------

df = spark.sql("show tables from imprd001_rawvault like '*OSIPI*'")
print(df.database)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_rawvault.sat_pq_pnt_siq_ami_read_power_quality2

# COMMAND ----------

# MAGIC %sql
# MAGIC desc imprd001_rawvault.sat_pq_pnt_siq_ami_read_power_quality2

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables from im${logicalenv}_rawvault like '*meter*'

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH SVC_PT_DEVICE_REL AS(
# MAGIC SELECT * FROM (
# MAGIC    select distinct hub_dev.DEVICE_ID
# MAGIC         , DEVICE_REL.* 
# MAGIC         , ROW_NUMBER() OVER ( partition BY SVC_PT_ID, EFF_START_DATE ORDER BY EFF_START_DATE desc, EFF_END_DATE DESC) as `rec_seq2`
# MAGIC      from (    select sat.*
# MAGIC                       , CAST(sat.EFF_START_TIME AS DATE) AS EFF_START_DATE
# MAGIC                       , CASE WHEN sat.EFF_END_TIME IS NULL THEN CAST('9999-12-31' AS DATE)
# MAGIC                              ELSE CAST(sat.EFF_END_TIME AS DATE)
# MAGIC                         END AS EFF_END_DATE
# MAGIC                   ,    hub_nmi.NMI AS SVC_PT_ID
# MAGIC                   ,    ROW_NUMBER() OVER ( partition BY hub_nmi.NMI, sat.EFF_START_TIME ORDER BY sat.extract_date_time DESC, EFF_START_TIME desc) as `rec_seq`
# MAGIC                  from im${logicalenv}_rawvault.LSAT_NMI_DEVICE_EIP_SVC_PT_DEVICE_REL sat
# MAGIC                  join im${logicalenv}_rawvault.LNK_NMI_DEVICE lnk
# MAGIC                    on lnk.LNK_NMI_DEVICE_HSH_KEY = sat.LNK_NMI_DEVICE_HSH_KEY
# MAGIC                  join im${logicalenv}_rawvault.HUB_NMI hub_nmi
# MAGIC                    on lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
# MAGIC                 where date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") < date_format(from_utc_timestamp(current_timestamp,'Australia/Brisbane'),"yyyyMMdd")
# MAGIC                   and sat.ORG_ID != '102'
# MAGIC          ) DEVICE_REL
# MAGIC       join im${logicalenv}_rawvault.LNK_NMI_DEVICE lnk
# MAGIC         on lnk.LNK_NMI_DEVICE_HSH_KEY = DEVICE_REL.LNK_NMI_DEVICE_HSH_KEY
# MAGIC       join im${logicalenv}_rawvault.HUB_DEVICE hub_dev
# MAGIC         on lnk.HUB_DEVICE_HSH_KEY = hub_dev.HUB_DEVICE_HSH_KEY
# MAGIC       where DEVICE_REL.rec_seq = 1
# MAGIC   )temp
# MAGIC   where temp.rec_seq2 = 1
# MAGIC ),
# MAGIC SVC_PT AS(
# MAGIC select distinct hub1.NMI AS NMI
# MAGIC       , hub2.NMI AS SVC_PT_ID
# MAGIC       , svc_pt.UDC_ID AS MDM_NMI
# MAGIC       , svc_pt.STATUS_CD 
# MAGIC  from (    select sat.*,
# MAGIC                   ROW_NUMBER() OVER ( partition BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC) as `rec_seq`
# MAGIC              from im${logicalenv}_rawvault.LSAT_NMI_EIP_SVC_PT sat
# MAGIC             where date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") < date_format(from_utc_timestamp(current_timestamp,'Australia/Brisbane'),"yyyyMMdd")
# MAGIC             and lower(STATUS_CD) = 'active'
# MAGIC       ) svc_pt
# MAGIC   join im${logicalenv}_rawvault.SALNK_NMI salnk 
# MAGIC     on SVC_PT.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
# MAGIC    and SVC_PT.rec_seq = 1
# MAGIC   join im${logicalenv}_rawvault.HUB_NMI as hub1
# MAGIC     on salnk.HUB_NMI_HSH_KEY1 = hub1.HUB_NMI_HSH_KEY
# MAGIC   join im${logicalenv}_rawvault.HUB_NMI as hub2 
# MAGIC     on salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
# MAGIC   where svc_pt.rec_seq = 1
# MAGIC ),
# MAGIC DEVICE_DETAILS AS(
# MAGIC select  hubdev.DEVICE_ID
# MAGIC       , eip_device.*
# MAGIC  from ( select sat.*
# MAGIC                    , ROW_NUMBER() over ( partition by sat.HUB_DEVICE_HSH_KEY order by sat.extract_date_time desc ) as `rec_seq`
# MAGIC                 from im${logicalenv}_rawvault.SAT_DEVICE_EIP_DEVICE_DETAILS sat
# MAGIC                where date_format(sat.EXTRACT_DATE_TIME,"yyyyMMdd") < date_format(from_utc_timestamp(current_timestamp,'Australia/Brisbane'),"yyyyMMdd")
# MAGIC      ) eip_device
# MAGIC   join im${logicalenv}_rawvault.HUB_DEVICE hubdev 
# MAGIC     on eip_device.HUB_DEVICE_HSH_KEY=hubdev.HUB_DEVICE_HSH_KEY
# MAGIC  where eip_device.rec_seq =1
# MAGIC    and (
# MAGIC        eip_device.MFG_SERIAL_NUM is not null
# MAGIC    or  lower(eip_device.MFG_SERIAL_NUM) <> 'null'
# MAGIC    or  length(trim(eip_device.MFG_SERIAL_NUM)) > 0
# MAGIC        )
# MAGIC )
# MAGIC select distinct 
# MAGIC        T2.NMI
# MAGIC      , T2.MDM_NMI
# MAGIC      , T3.MFG_SERIAL_NUM    AS SERIAL_NUMBER
# MAGIC      , T1.EFF_START_DATE
# MAGIC      , T1.EFF_END_DATE
# MAGIC      , T1.DEVICE_ID
# MAGIC      , T1.SVC_PT_ID
# MAGIC      , T1.DATA_SRC
# MAGIC      , T1.ORG_ID
# MAGIC      , T1.INSERT_TIME
# MAGIC      , T1.INSERT_BY
# MAGIC      , T1.LAST_UPD_BY
# MAGIC      , T1.LAST_UPD_TIME
# MAGIC      , T1.REC_VERSION_NUM
# MAGIC FROM SVC_PT_DEVICE_REL T1
# MAGIC JOIN SVC_PT T2
# MAGIC ON T1.SVC_PT_ID = T2.SVC_PT_ID
# MAGIC JOIN DEVICE_DETAILS T3
# MAGIC ON T3.DEVICE_ID = T1.DEVICE_ID
# MAGIC order by T2.NMI

# COMMAND ----------

staging_temp = '/mnt/staging/OSIPI/HIST_LOAD/OSIPI_TAGDATA_HIST/extracted'
tag_data = spark.read.csv(staging_temp)


# COMMAND ----------

# MAGIC %sh
# MAGIC wc -l `find /dbfs/mnt/staging/OSIPI/HIST_LOAD/OSIPI_TAGDATA_HIST/ -type f`
# MAGIC wc -l `find /dbfs/mnt/staging/OSIPI/HIST_LOAD/OSIPI_TAGDATA_HIST/extracted -type f`

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /dbfs/mnt/staging/OSIPI/HIST_LOAD/OSIPI_TAGDATA_HIST/extracted

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /dbfs/mnt/staging/OSIPI/HIST_LOAD/OSIPI_TAGDATA_HIST/extracted/VBB.W2TR.W2TR_TOP.TEMP.TEMP_20200101-000000_20210101-000000.txt

# COMMAND ----------

tag_data.show()

# COMMAND ----------

connectionProperties = {
  "user" : "im-prd-sql-admin",
  "password" : "",
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

jdbcUrl = "jdbc:sqlserver://astimas1prdwarehouse.database.windows.net:1433;database=IMprddw;user=im-prd-sql-admin@astimas1prdwarehouse;password=;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"


pushdown = "(select sap.*, doms.composite_key, doms.device_name, case when ((sap_scada_key <> composite_key) and (composite_key is not NULL)) then composite_key else sap_scada_key end as derived_key from (select sa.*, ss.sap_scada_key from (select e.equipment_id, e.floc, e.user_status, e.object_type, c.value_char as sap_scada_key from [imprd001_selfserv_eam].[EAM_EQUIPMENT] e, [imprd001_selfserv_eam].[EAM_CHARACTERISTIC_VALUE] c where 	e.floc = c.object_no and e.cat_profile = 'CB' and e.user_status = 'SERV' and (e.floc like 'DS-%' or e.floc like 'TS-%') and	c.char_name = 'SCADA_KEY') ss, (select e.equipment_id, e.floc, e.user_status, e.object_type, c.value_char as alias from [imprd001_selfserv_eam].[EAM_EQUIPMENT] e, [imprd001_selfserv_eam].[EAM_CHARACTERISTIC_VALUE] c where 	e.floc = c.object_no and	e.cat_profile = 'CB' and	e.user_status = 'SERV' and 	(e.floc like 'DS-%' or e.floc like 'TS-%') and	c.char_name = 'ALIAS') sa where ss.floc = sa.floc) sap left join (select c.component_alias, a.composite_key, a.device_short_name as device_name from [imprd001_analytics].[DOMS_DQ_COMPONENT] c, [imprd001_analytics].[DOMS_DQ_COMPONENT_ATTRIBUTE] a where	c.HUB_DOMS_COMPONENT_HSH_KEY  = a.HUB_DOMS_COMPONENT_HSH_KEY) doms	on sap.alias = doms.component_alias) tab"

df_sap_doms = spark.read.jdbc(url=jdbcUrl, table=pushdown, properties=connectionProperties)

# COMMAND ----------

df_sap_doms = df_sap_doms.toPandas()

# COMMAND ----------

#Parameterizing the variables to be used in the script
dbutils.widgets.text('LogicalEnv','prd001')
LogicalEnv=dbutils.widgets.get('LogicalEnv').strip()
dbutils.widgets.text('source_schema','dimlayer')
source_schema=dbutils.widgets.get('source_schema').strip()
dbutils.widgets.text('target_schema','transform')
target_schema=dbutils.widgets.get('target_schema').strip()
dbutils.widgets.text('table','dim_connection_point_device')
table=dbutils.widgets.get('table').strip()

# COMMAND ----------

#----------------------------------------------------------------------------------------------------------------------
# Below parameters retrieved from databrics secret scope linked to key vault
# Authentication with service principals is not supported for loading data into and unloading data from Azure Synapse.
#----------------------------------------------------------------------------------------------------------------------
 
jdbcKeyName = "imdw-jdbc-large-connstring"
#prepare connection string for sqldw
sqldwConnstring = dbutils.secrets.get(scope = "dfstore", key=jdbcKeyName)
print(sqldwConnstring)

# COMMAND ----------

table_name = "im" + LogicalEnv + "_" + source_schema + "." + table
jdbcDF = spark.read.format("jdbc") \
.option("url", sqldwConnstring) \
.option("dbtable", table_name) \
.option('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
.load()
#display(jdbcDF)
jdbcDF.createOrReplaceTempView("jdbcDFtable")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jdbcDFtable
