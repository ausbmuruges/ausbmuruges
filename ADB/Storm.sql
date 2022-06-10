-- Databricks notebook source
-- MAGIC %sql
-- MAGIC show tables FROM im${logicalenv}_rawvault like '*device*'

-- COMMAND ----------

select * from im${logicalenv}_rawvault.lsat_device_job_uiq_event_log

-- COMMAND ----------

  select  
        SRC.DEVICE_ID AS SERIAL_NUMBER
      , SAT.EVENT_LOG_ID
      , SAT.EVENT_ID
      , SAT.EPOCH_NUM
      , SAT.EL_SEQ_NUM
      , SAT.EMITTER_TYPE_ID AS EMITTER_TYP_ID
      , SAT.EMITTER_NAME AS EMITTER_NM
      , SAT.EMITTER_MAC_ID as EMITTER_MAC_ID
      , SAT.EVENT_TS as EVENT_DTM
      , SAT.EVENT_TEXT
      , SAT.SOURCE_TYPE_ID
      , SAT.SEVERITY_ID
      , SAT.ACK
      , SAT.JOB_OPERATION_TYPE_ID AS JOB_OPERATION_TYP_ID
      , SAT.ASSIGNED_TO_USER as ASSIGNED_TO_USER_ID
      , SAT.UPDATED_BY_USER as UPDATED_BY_USER_ID
      , SAT.INSERT_TS AS INSERT_DTM
      , SAT.UPDATE_TS AS UPDATE_DTM
      , SAT.EMITTER_ID
,sat.*
from im${logicalenv}_rawvault.LSAT_DEVICE_JOB_UIQ_EVENT_LOG SAT
JOIN im${logicalenv}_rawvault.SALNK_DEVICE_JOB SALNK
ON SAT.SALNK_DEVICE_JOB_HSH_KEY=SALNK.SALNK_DEVICE_JOB_HSH_KEY
JOIN im${logicalenv}_rawvault.HUB_DEVICE SRC 
ON SRC.HUB_DEVICE_HSH_KEY=SALNK.HUB_DEVICE_HSH_KEY3
WHERE SAT.EVENT_ID IN ('15237', '15194', '16007', '15019', '5882 ', '16006', '16010', '15021', '15033', '30192', '16071', '10592', '16011', '16021', '16075', '15029', '300142', '5883', '15190', '5878', '16039', '15231', '16064', '15083', '15078', '16063', '40002', '15193', '30182', '30132', '5877', '30160', '5879', '16026', '15259', '5887', '16022', '30068', '16019', '16005', '30050', '30000', '15030', '15200', '16003', '40003', '16004', '300147', '16061', '15035', '16065', '15036', '16044', '16078', '15022', '15080', '15026', '15082', '15195', '15213', '16073', '15079', '30196', '16070', '16020', '16062', '15031', '40004', '16024', '15085', '16008', '16001', '30152', '300148', '15075', '15234', '16066', '16074', '15228', '16059', '16029', '16002', '16067', '30056', '15088', '15203', '30002', '16047', '30004', '30154', '300025', '16056', '30205', '16077', '16012', '16045', '30001', '16025', '30201', '16009', '16049', '16055', '16030', '15191', '40001', '15207', '300027', '16023', '15024', '15027', '15023', '300143', '15212', '16072', '15077', '30042', '30134' ) and date_format(sat.EXTRACT_DATE_PARTITION,"yyyyMMdd") >= date_format(date_add('${run_date}',-4),"yyyyMMdd") 
and date_format(sat.EXTRACT_DATE_PARTITION,"yyyyMMdd") <= date_format(date_add('${run_date}',4),"yyyyMMdd") 
	        -- and sat.EXTRACT_DATE_TIME > CAST(date_add('${run_date}',-1) AS timestamp) + MAKE_INTERVAL(0, 0, 0, 0, -10, 0, 0)
	        -- and sat.EXTRACT_DATE_TIME <= CAST('${run_date}' AS timestamp) + MAKE_INTERVAL(0, 0, 0, 0, -10, 0, 0)
--           and sat.EXTRACT_DATE_TIME > CAST(to_date(date_add('${run_date}',-1)) AS timestamp) + MAKE_INTERVAL(0, 0, 0, 0, -4, 0, 0)
-- 	        and sat.EXTRACT_DATE_TIME <= CAST(to_date('${run_date}') AS timestamp) + MAKE_INTERVAL(0, 0, 0, 0, -4, 0, 0)
	        and sat.CHANGE_TYPE !='D'
and SRC.DEVICE_ID = '4071020'            
