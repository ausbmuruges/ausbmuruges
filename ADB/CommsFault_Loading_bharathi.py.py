# Databricks notebook source
# MAGIC %md ## CommsFault_Loading
# MAGIC ### 
# MAGIC ### ------------------------------------------------------------------------------------------------------------------
# MAGIC ### This notebook loads data to im<env>_busvault.bsat_comms_fault_outbound delta table from many rv sources.
# MAGIC ### This notebook takes the input arguments from ADF pipeline and then pass that to the notebook.
# MAGIC ### ------------------------------------------------------------------------------------------------------------------
# MAGIC #### Date         : 14/01/2022
# MAGIC #### Author       : Asutosh Sarangi
# MAGIC #### 
# MAGIC ##### Modification History : 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE WIDGET TEXT comms_fault_threshold_num_days DEFAULT "5";
# MAGIC CREATE WIDGET TEXT run_dt DEFAULT "2022-05-11";
# MAGIC CREATE WIDGET TEXT logical_env DEFAULT "prd001";
# MAGIC CREATE WIDGET TEXT intervalScanDays DEFAULT "30";

# COMMAND ----------

comms_fault_threshold_num_days=dbutils.widgets.get('comms_fault_threshold_num_days').strip()
run_dt=dbutils.widgets.get('run_dt').strip()
logical_env=dbutils.widgets.get('logical_env').strip()
interval_scan_days=dbutils.widgets.get('intervalScanDays').strip()
print('comms_fault_threshold_num_days:',comms_fault_threshold_num_days)
print('run_dt:',run_dt)
print('logical_env:',logical_env)
print('interval_scan_days:',interval_scan_days)

# COMMAND ----------

transform_tbl_schema='im'+logical_env+'_transform'
rawvault_tbl_schema='im'+logical_env+'_rawvault'
busvault_tbl_schema='im'+logical_env+'_busvault'
print('transform_tbl_schema:',transform_tbl_schema)
print('rawvault_tbl_schema:',rawvault_tbl_schema)
print('busvault_tbl_schema:',busvault_tbl_schema)


# COMMAND ----------

DEVICE_METER_TBL=transform_tbl_schema+'.device_meter_BAU'
ACTV_NMI_DEVICE_METER_TBL=transform_tbl_schema+'.ACTV_NMI_DEVICE_METER_BAU'
ACTIVE_NMI_METER_TBL=transform_tbl_schema+'.ACTIVE_NMI_METER_BAU1'
DEVICE_METER_COMMS_CARD_TBL=transform_tbl_schema+'.DEVICE_METER_COMMS_CARD_BAU'
DEVICE_INTERVAL_TBL=transform_tbl_schema+'.DEVICE_INTERVAL_BAU1'
DEVICE_READ_TYPE_CD_TBL=transform_tbl_schema+'.DEVICE_READ_TYPE_CD_BAU'
ACTIVE_NMI_DEVICE_READ_TYPE_TBL=transform_tbl_schema+'.ACTIVE_NMI_DEVICE_READ_TYPE_BAU'

HUB_NMI_TBL=rawvault_tbl_schema+'.HUB_NMI'
HUB_DEVICE_TBL=rawvault_tbl_schema+'.HUB_DEVICE'
HUB_EQUIPMENT_TBL=rawvault_tbl_schema+'.HUB_EQUIPMENT'
HUB_SRVORDER_TBL=rawvault_tbl_schema+'.HUB_SRVORDER'

SAT_EQUIPMENT_CIS_ZIPMTT_SYNC_TS_TBL=rawvault_tbl_schema+'.SAT_EQUIPMENT_CIS_ZIPMTT_SYNC_TS'
SAT_DEVICE_UIQ_LP_ITVL_TBL=rawvault_tbl_schema+'.SAT_DEVICE_UIQ_LP_ITVL' 
SAT_DEVICE_PNT_AMI_READ_INTERVAL_TBL=rawvault_tbl_schema+'.SAT_DEVICE_PNT_AMI_READ_INTERVAL'
SAT_NMI_CIS_ZIIDTT_NMI_STAT_TBL=rawvault_tbl_schema+'.SAT_NMI_CIS_ZIIDTT_NMI_STAT'
SAT_SRVORDER_CIS_JEST_TBL=rawvault_tbl_schema+'.SAT_SRVORDER_CIS_JEST'
LSAT_NMI_CIS_EUIINSTLN_TBL=rawvault_tbl_schema+'.LSAT_NMI_CIS_EUIINSTLN'
LSAT_NMI_EQUIPMENT_CIS_EASTL_TBL=rawvault_tbl_schema+'.LSAT_NMI_EQUIPMENT_CIS_EASTL'
LSAT_NMI_CIS_EUITRANS_TBL=rawvault_tbl_schema+'.LSAT_NMI_CIS_EUITRANS'
LSAT_EQUIPMENT_DEVICE_MATERIAL_CIS_EQUI_TBL=rawvault_tbl_schema+'.LSAT_EQUIPMENT_DEVICE_MATERIAL_CIS_EQUI'
LSAT_EQUIPMENT_COMMS_CIS_EZUG_TBL=rawvault_tbl_schema+'.LSAT_EQUIPMENT_COMMS_CIS_EZUG'
LSAT_NMI_EQUIPMENT2_CIS_EGERH_TBL=rawvault_tbl_schema+'.LSAT_NMI_EQUIPMENT2_CIS_EGERH'
LSAT_NMI_DEVICE_CIS_ZIIDTT_DATASTRM_TBL=rawvault_tbl_schema+'.LSAT_NMI_DEVICE_CIS_ZIIDTT_DATASTRM'

LNK_NMI_DEVICE_TBL=rawvault_tbl_schema+'.LNK_NMI_DEVICE'
LNK_NMI_EQUIPMENT_TBL=rawvault_tbl_schema+'.LNK_NMI_EQUIPMENT'
LNK_EQUIPMENT_DEVICE_MATERIAL_TBL=rawvault_tbl_schema+'.LNK_EQUIPMENT_DEVICE_MATERIAL'
LNK_NMI_EQUIPMENT2_TBL=rawvault_tbl_schema+'.LNK_NMI_EQUIPMENT2'
SALNK_NMI_TBL=rawvault_tbl_schema+'.SALNK_NMI'
SALNK_EQUIPMENT_COMMS_TBL=rawvault_tbl_schema+'.SALNK_EQUIPMENT_COMMS'
SALNK_DEVICE_NMI_TBL=rawvault_tbl_schema+'.SALNK_DEVICE_NMI'

BSAT_METER_DETAILS_MESH_TBL=busvault_tbl_schema+'.BSAT_METER_DETAILS_MESH'
BSAT_METER_DETAILS_WIMAX_TBL=busvault_tbl_schema+'.BSAT_METER_DETAILS_WIMAX'
BSAT_COMMS_FAULT_OUTBOUND_TBL=busvault_tbl_schema+'.BSAT_COMMS_FAULT_OUTBOUND_BAU'

# COMMAND ----------

str_drop_active_nmi_meter_qry='''DROP table IF EXISTS {}'''.format(ACTIVE_NMI_METER_TBL)
str_insert_active_nmi_meter_qry='''
CREATE TABLE {} AS 
with ZIIDTT_NMI_STAT_V0 as(
        SELECT ZIIDTT_NMI_STAT.*
        FROM (SELECT sat.*
                    ,ROW_NUMBER() OVER ( PARTITION BY HUB_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC ) as `rec_seq`
              FROM {} sat
              WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
                and ({} BETWEEN  TO_DATE(FROM_dt,'yyyyMMdd') AND  TO_DATE(to_dt,'yyyyMMdd'))
              )ZIIDTT_NMI_STAT
        where ZIIDTT_NMI_STAT.rec_seq =1
        and ZIIDTT_NMI_STAT.CHANGE_TYPE <> 'D'
		and LENGTH(TRIM(ZIIDTT_NMI_STAT.NMI_CHECKSUM)) > 0
		and ZIIDTT_NMI_STAT.NMI_STATUS IN ('A')
        ),
     EUITRANS_V1 as(
     SELECT hub1_nmi.NMI as EXT_UI
           ,salnk.HUB_NMI_HSH_KEY1, salnk.HUB_NMI_HSH_KEY2
       FROM (SELECT  sat.*
                    ,ROW_NUMBER() OVER ( PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               FROM {} sat
               WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
			     AND LENGTH(TRIM(sat.NMI_CHECKSUM)) > 0
                 AND ({} BETWEEN  TO_DATE(sat.DATEFROM,'yyyyMMdd') AND  TO_DATE(sat.DATETO,'yyyyMMdd'))
            )EUITRANS
       JOIN {} salnk
         ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
        and EUITRANS.rec_seq = 1
        and EUITRANS.CHANGE_TYPE <> 'D'
       JOIN {} hub1_nmi
         ON salnk.HUB_NMI_HSH_KEY1 = hub1_nmi.HUB_NMI_HSH_KEY
       LEFT OUTER JOIN {} hub2_nmi
         ON salnk.HUB_NMI_HSH_KEY2 = hub2_nmi.HUB_NMI_HSH_KEY 
     ),
   EUIINSTLN_V2 as(
     SELECT hub.nmi as ANLAGE
          , salnk.HUB_NMI_HSH_KEY1
          , salnk.HUB_NMI_HSH_KEY2
       FROM (SELECT  sat.*
                    ,ROW_NUMBER() OVER ( PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               FROM {} sat
               WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
               AND ({} BETWEEN  TO_DATE(sat.DATEFROM,'yyyyMMdd') AND  TO_DATE(sat.DATETO,'yyyyMMdd'))
            )EUIINSTLN
       JOIN {} salnk
         ON EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
        and EUIINSTLN.rec_seq = 1
        and EUIINSTLN.CHANGE_TYPE <> 'D'
       JOIN {} hub
         ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
     ), 
    EASTL_V3 as(
     SELECT eastl.*
          , lnk.HUB_NMI_HSH_KEY
          , lnk.HUB_EQUIPMENT_HSH_KEY
       FROM (SELECT sat.*
                  , ROW_NUMBER() OVER ( PARTITION BY sat.LNK_NMI_EQUIPMENT_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               FROM {} sat
               WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
               AND ({} BETWEEN  TO_DATE(sat.AB,'yyyyMMdd') AND  TO_DATE(sat.BIS,'yyyyMMdd'))
           )EASTL
       JOIN {} lnk
         ON EASTL.LNK_NMI_EQUIPMENT_HSH_KEY = LNK.LNK_NMI_EQUIPMENT_HSH_KEY
        and EASTL.rec_seq = 1
        and EASTL.CHANGE_TYPE <> 'D'
       JOIN {} hub
         ON lnk.HUB_EQUIPMENT_HSH_KEY = hub.HUB_EQUIPMENT_HSH_KEY
     )
SELECT DISTINCT a.HUB_NMI_HSH_KEY1,
    a.HUB_NMI_HSH_KEY2,
    c.HUB_EQUIPMENT_HSH_KEY
FROM ZIIDTT_NMI_STAT_V0 z
JOIN EUITRANS_V1 a
    ON z.HUB_NMI_HSH_KEY=a.HUB_NMI_HSH_KEY1
JOIN EUIINSTLN_V2 b
	ON a.HUB_NMI_HSH_KEY2 = b.HUB_NMI_HSH_KEY2
JOIN EASTL_V3 c
	ON b.HUB_NMI_HSH_KEY1 = c.HUB_NMI_HSH_KEY
where
  a.HUB_NMI_HSH_KEY1 = sha1('6305009045:')
  or a.HUB_NMI_HSH_KEY2 = sha1('6305009045:')    
'''.format(ACTIVE_NMI_METER_TBL,SAT_NMI_CIS_ZIIDTT_NMI_STAT_TBL,repr(run_dt),repr(run_dt),LSAT_NMI_CIS_EUITRANS_TBL,repr(run_dt),repr(run_dt),SALNK_NMI_TBL,HUB_NMI_TBL,HUB_NMI_TBL,LSAT_NMI_CIS_EUIINSTLN_TBL,repr(run_dt),repr(run_dt),SALNK_NMI_TBL,HUB_NMI_TBL,LSAT_NMI_EQUIPMENT_CIS_EASTL_TBL,repr(run_dt),repr(run_dt),LNK_NMI_EQUIPMENT_TBL,HUB_EQUIPMENT_TBL)
print(str_drop_active_nmi_meter_qry)
print(str_insert_active_nmi_meter_qry)

# COMMAND ----------

spark.sql(str_drop_active_nmi_meter_qry)
spark.sql(str_insert_active_nmi_meter_qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_transform.ACTIVE_NMI_METER_BAU

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE imprd001_transform.ACTIVE_NMI_METER_BAU AS 
# MAGIC with ZIIDTT_NMI_STAT_V0 as(
# MAGIC         SELECT ZIIDTT_NMI_STAT.*
# MAGIC         FROM (SELECT sat.*
# MAGIC                     ,ROW_NUMBER() OVER ( PARTITION BY HUB_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC ) as `rec_seq`
# MAGIC               FROM imprd001_rawvault.SAT_NMI_CIS_ZIIDTT_NMI_STAT sat
# MAGIC               WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                 and ('2022-05-11' BETWEEN  TO_DATE(FROM_dt,'yyyyMMdd') AND  TO_DATE(to_dt,'yyyyMMdd'))
# MAGIC               )ZIIDTT_NMI_STAT
# MAGIC         where ZIIDTT_NMI_STAT.rec_seq =1
# MAGIC         and ZIIDTT_NMI_STAT.CHANGE_TYPE <> 'D'
# MAGIC 		and LENGTH(TRIM(ZIIDTT_NMI_STAT.NMI_CHECKSUM)) > 0
# MAGIC 		and ZIIDTT_NMI_STAT.NMI_STATUS IN ('A')
# MAGIC         ),
# MAGIC      EUITRANS_V1 as(
# MAGIC      SELECT hub1_nmi.NMI as EXT_UI
# MAGIC            ,salnk.HUB_NMI_HSH_KEY1, salnk.HUB_NMI_HSH_KEY2
# MAGIC        FROM (SELECT  sat.*
# MAGIC                     ,ROW_NUMBER() OVER ( PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
# MAGIC                FROM imprd001_rawvault.LSAT_NMI_CIS_EUITRANS sat
# MAGIC                WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC 			     AND LENGTH(TRIM(sat.NMI_CHECKSUM)) > 0
# MAGIC                  AND ('2022-05-11' BETWEEN  TO_DATE(sat.DATEFROM,'yyyyMMdd') AND  TO_DATE(sat.DATETO,'yyyyMMdd'))
# MAGIC             )EUITRANS
# MAGIC        JOIN imprd001_rawvault.SALNK_NMI salnk
# MAGIC          ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
# MAGIC         and EUITRANS.rec_seq = 1
# MAGIC         and EUITRANS.CHANGE_TYPE <> 'D'
# MAGIC        JOIN imprd001_rawvault.HUB_NMI hub1_nmi
# MAGIC          ON salnk.HUB_NMI_HSH_KEY1 = hub1_nmi.HUB_NMI_HSH_KEY
# MAGIC        LEFT OUTER JOIN imprd001_rawvault.HUB_NMI hub2_nmi
# MAGIC          ON salnk.HUB_NMI_HSH_KEY2 = hub2_nmi.HUB_NMI_HSH_KEY 
# MAGIC      ),
# MAGIC    EUIINSTLN_V2 as(
# MAGIC      SELECT hub.nmi as ANLAGE
# MAGIC           , salnk.HUB_NMI_HSH_KEY1
# MAGIC           , salnk.HUB_NMI_HSH_KEY2
# MAGIC        FROM (SELECT  sat.*
# MAGIC                     ,ROW_NUMBER() OVER ( PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
# MAGIC                FROM imprd001_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
# MAGIC                WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                AND ('2022-05-11' BETWEEN  TO_DATE(sat.DATEFROM,'yyyyMMdd') AND  TO_DATE(sat.DATETO,'yyyyMMdd'))
# MAGIC             )EUIINSTLN
# MAGIC        JOIN imprd001_rawvault.SALNK_NMI salnk
# MAGIC          ON EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
# MAGIC         and EUIINSTLN.rec_seq = 1
# MAGIC         and EUIINSTLN.CHANGE_TYPE <> 'D'
# MAGIC        JOIN imprd001_rawvault.HUB_NMI hub
# MAGIC          ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
# MAGIC      ), 
# MAGIC     EASTL_V3 as(
# MAGIC      SELECT eastl.*
# MAGIC           , lnk.HUB_NMI_HSH_KEY
# MAGIC           , lnk.HUB_EQUIPMENT_HSH_KEY
# MAGIC        FROM (SELECT sat.*
# MAGIC                   , ROW_NUMBER() OVER ( PARTITION BY sat.LNK_NMI_EQUIPMENT_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
# MAGIC                FROM imprd001_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTL sat
# MAGIC                WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                AND ('2022-05-11' BETWEEN  TO_DATE(sat.AB,'yyyyMMdd') AND  TO_DATE(sat.BIS,'yyyyMMdd'))
# MAGIC            )EASTL
# MAGIC        JOIN imprd001_rawvault.LNK_NMI_EQUIPMENT lnk
# MAGIC          ON EASTL.LNK_NMI_EQUIPMENT_HSH_KEY = LNK.LNK_NMI_EQUIPMENT_HSH_KEY
# MAGIC         and EASTL.rec_seq = 1
# MAGIC         and EASTL.CHANGE_TYPE <> 'D'
# MAGIC        JOIN imprd001_rawvault.HUB_EQUIPMENT hub
# MAGIC          ON lnk.HUB_EQUIPMENT_HSH_KEY = hub.HUB_EQUIPMENT_HSH_KEY
# MAGIC      )
# MAGIC SELECT DISTINCT a.HUB_NMI_HSH_KEY1,
# MAGIC     a.HUB_NMI_HSH_KEY2,
# MAGIC     c.HUB_EQUIPMENT_HSH_KEY
# MAGIC FROM ZIIDTT_NMI_STAT_V0 z
# MAGIC JOIN EUITRANS_V1 a
# MAGIC     ON z.HUB_NMI_HSH_KEY=a.HUB_NMI_HSH_KEY1
# MAGIC JOIN EUIINSTLN_V2 b
# MAGIC 	ON a.HUB_NMI_HSH_KEY2 = b.HUB_NMI_HSH_KEY2
# MAGIC JOIN EASTL_V3 c
# MAGIC 	ON b.HUB_NMI_HSH_KEY1 = c.HUB_NMI_HSH_KEY
# MAGIC where
# MAGIC   a.HUB_NMI_HSH_KEY1 = sha1('6305009045:')
# MAGIC   or a.HUB_NMI_HSH_KEY2 = sha1('6305009045:')

# COMMAND ----------

str_drop_device_meter_comms_card_qry='''DROP table IF EXISTS {}'''.format(DEVICE_METER_COMMS_CARD_TBL)
str_insert_device_meter_comms_card_qry='''
CREATE TABLE {} AS 
WITH JEST_V4 AS ( 
     SELECT DISTINCT hubsrv.ORDER_NUMBER AS OBJNR
       FROM ( SELECT sat.*
                FROM {} sat
               WHERE date_format(sat.EXTRACT_DATE_TIME,"yyyyMMdd") < date_format({},"yyyyMMdd") and CHANGE_TYPE <> 'D'
            ) JEST 
         join {} hubsrv
         on JEST.HUB_SRVORDER_HSH_KEY=hubsrv.HUB_SRVORDER_HSH_KEY   
        where INACT != 'X' and (stat = 'I0076' or stat =  'I0320')
     ),
EQUI_V5 AS ( 
    SELECT eq.*
    FROM (SELECT lnk.HUB_EQUIPMENT_HSH_KEY
              ,lnk.HUB_DEVICE_HSH_KEY
              ,hub_dev.DEVICE_ID
              ,hub_equi.EQUIPMENT_ID AS EQUI_EQUNR
              ,hub_dev.DEVICE_ID AS SERIAL_NUMBER
              ,equi.ZWAN_TYPE
              ,equi.OBJNR
              ,equi.ERDAT
           FROM ( SELECT lsat.*
                       , ROW_NUMBER() over ( PARTITION BY lsat.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY order by lsat.extract_date_time desc ) as `rec_seq`
                   FROM {} lsat
                   WHERE DATE_FORMAT(lsat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
                      ) equi
            JOIN {} lnk
              ON equi.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY=lnk.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY 
            JOIN {} hub_dev
              ON lnk.HUB_DEVICE_HSH_KEY = hub_dev.HUB_DEVICE_HSH_KEY
            JOIN {} hub_equi
              ON lnk.HUB_EQUIPMENT_HSH_KEY = hub_equi.HUB_EQUIPMENT_HSH_KEY
            WHERE equi.rec_seq =1
                AND equi.CHANGE_TYPE <> 'D'
            )eq
		   LEFT ANTI JOIN JEST_V4 je
             ON eq.OBJNR=je.OBJNR),
EZUG_V7 AS(
     SELECT  salnk.HUB_EQUIPMENT_HSH_KEY1 AS EZUG_LOGIKNR_HSH_KEY
           , salnk.HUB_EQUIPMENT_HSH_KEY2 AS EZUG_LOGIKNR_2_HSH_KEY
           , EZUG.*          
       FROM (SELECT sat.*
                  , ROW_NUMBER() OVER ( PARTITION BY sat.SALNK_EQUIPMENT_COMMS_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               FROM {} sat
               WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
               AND ({} BETWEEN  TO_DATE(sat.AB,'yyyyMMdd') AND  TO_DATE(sat.BIS,'yyyyMMdd'))
               AND CHANGE_TYPE <> 'D'
           )EZUG
       JOIN {} salnk
         ON EZUG.SALNK_EQUIPMENT_COMMS_HSH_KEY = salnk.SALNK_EQUIPMENT_COMMS_HSH_KEY
      WHERE EZUG.rec_seq = 1
     ),
 EGERH_METER_V8 AS(
     SELECT  salnk.HUB_EQUIPMENT_HSH_KEY1 AS EGERH_EQUNR_METER_HSH_KEY
           , salnk.HUB_EQUIPMENT_HSH_KEY2 AS EGERH_LOGIKNR_METER_HSH_KEY
       FROM (SELECT sat.*
                  , ROW_NUMBER() OVER ( PARTITION BY LNK_NMI_EQUIPMENT2_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               FROM {} sat
               WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
                 and sat.KOMBINAT = 'Z'
                 AND ({} BETWEEN  TO_DATE(sat.AB,'yyyyMMdd') AND  TO_DATE(sat.BIS,'yyyyMMdd'))
           )egerh
       JOIN {} salnk
         ON egerh.LNK_NMI_EQUIPMENT2_HSH_KEY = salnk.LNK_NMI_EQUIPMENT2_HSH_KEY
        and egerh.rec_seq = 1
        and egerh.CHANGE_TYPE <> 'D'
     ),
EGERH_COMMSCARD_V9 AS(
     SELECT  salnk.HUB_EQUIPMENT_HSH_KEY1 AS EGERH_EQUNR_COMMSCARD_HSH_KEY
           , salnk.HUB_EQUIPMENT_HSH_KEY2 AS EGERH_LOGIKNR_COMMSCARD_HSH_KEY
       FROM (SELECT sat.*
                  , ROW_NUMBER() OVER ( PARTITION BY LNK_NMI_EQUIPMENT2_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               FROM {} sat
               WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
                 and sat.KOMBINAT = 'S'
                 AND ({} BETWEEN  TO_DATE(sat.AB,'yyyyMMdd') AND  TO_DATE(sat.BIS,'yyyyMMdd'))
           )egerh
       JOIN {} salnk
         ON egerh.LNK_NMI_EQUIPMENT2_HSH_KEY = salnk.LNK_NMI_EQUIPMENT2_HSH_KEY
        and egerh.rec_seq = 1
        and egerh.CHANGE_TYPE <> 'D'
     ),
GET_METER_V10 as (SELECT *
                FROM (SELECT *
                        ,ROW_NUMBER() OVER(PARTITION BY HUB_DEVICE_HSH_KEY ORDER BY AB DESC) AS REC_SEQ
                      FROM (SELECT EZUG_V7.EZUG_LOGIKNR_2_HSH_KEY
                          , EQUI_V5.HUB_DEVICE_HSH_KEY
                          , EGERH_METER_V8.EGERH_EQUNR_METER_HSH_KEY
                          , EZUG_V7.AB
                          , EZUG_V7.BIS 
                          , EQUI_V5.SERIAL_NUMBER
                          , EQUI_V5.ZWAN_TYPE
                          ,  COALESCE(EQUI_V5.ZWAN_TYPE,'MESH') AS COMMS_MODULE_TYPE
                       FROM EGERH_METER_V8 
                       LEFT JOIN EZUG_V7
                         ON EZUG_V7.EZUG_LOGIKNR_HSH_KEY = EGERH_METER_V8.EGERH_LOGIKNR_METER_HSH_KEY
                       JOIN EQUI_V5
                         ON EQUI_V5.HUB_EQUIPMENT_HSH_KEY = EGERH_METER_V8.EGERH_EQUNR_METER_HSH_KEY
                       JOIN {} a_nmi
                         ON a_nmi.HUB_EQUIPMENT_HSH_KEY = EGERH_METER_V8.EGERH_LOGIKNR_METER_HSH_KEY
                         )X
                 )y
                 where REC_SEQ=1
     )
SELECT HUB_DEVICE_HSH_KEY
    ,EGERH_EQUNR_METER_HSH_KEY
    ,COMMS_MODULE_TYPE
FROM(
    SELECT GET_METER_V10.HUB_DEVICE_HSH_KEY
        , GET_METER_V10.EGERH_EQUNR_METER_HSH_KEY
        , EQUI_V5.ZWAN_TYPE AS COMMS_MODULE_TYPE
        ,ROW_NUMBER() OVER(PARTITION BY EQUI_V5.HUB_DEVICE_HSH_KEY ORDER BY EQUI_V5.ERDAT DESC) AS rec_seq_er_dt
    FROM GET_METER_V10
    JOIN EGERH_COMMSCARD_V9
         ON GET_METER_V10.EZUG_LOGIKNR_2_HSH_KEY = EGERH_COMMSCARD_V9.EGERH_LOGIKNR_COMMSCARD_HSH_KEY
    JOIN EQUI_V5
         ON EQUI_V5.HUB_EQUIPMENT_HSH_KEY = EGERH_COMMSCARD_V9.EGERH_EQUNR_COMMSCARD_HSH_KEY
    JOIN {} a_nmi
    ON a_nmi.HUB_EQUIPMENT_HSH_KEY = EGERH_COMMSCARD_V9.EGERH_LOGIKNR_COMMSCARD_HSH_KEY
    WHERE  ({} BETWEEN  TO_DATE(GET_METER_V10.AB,'yyyyMMdd') AND  TO_DATE(GET_METER_V10.BIS,'yyyyMMdd'))
    UNION ALL
    SELECT HUB_DEVICE_HSH_KEY
        , EGERH_EQUNR_METER_HSH_KEY
        ,'MESH' AS COMMS_MODULE_TYPE
        ,1 as rec_seq_er_dt
    FROM GET_METER_V10
    WHERE EZUG_LOGIKNR_2_HSH_KEY IS NULL
     )x
 WHERE rec_seq_er_dt=1
  and HUB_DEVICE_HSH_KEY = sha1('4244699:')
 or EGERH_EQUNR_METER_HSH_KEY =  sha1('4244699:')
'''.format(DEVICE_METER_COMMS_CARD_TBL,SAT_SRVORDER_CIS_JEST_TBL,repr(run_dt),HUB_SRVORDER_TBL,LSAT_EQUIPMENT_DEVICE_MATERIAL_CIS_EQUI_TBL,repr(run_dt),LNK_EQUIPMENT_DEVICE_MATERIAL_TBL,HUB_DEVICE_TBL,HUB_EQUIPMENT_TBL,LSAT_EQUIPMENT_COMMS_CIS_EZUG_TBL,repr(run_dt),repr(run_dt),SALNK_EQUIPMENT_COMMS_TBL,LSAT_NMI_EQUIPMENT2_CIS_EGERH_TBL,repr(run_dt),repr(run_dt),LNK_NMI_EQUIPMENT2_TBL,LSAT_NMI_EQUIPMENT2_CIS_EGERH_TBL,repr(run_dt),repr(run_dt),LNK_NMI_EQUIPMENT2_TBL,ACTIVE_NMI_METER_TBL,ACTIVE_NMI_METER_TBL,repr(run_dt))
print(str_drop_device_meter_comms_card_qry)
print(str_insert_device_meter_comms_card_qry)

# COMMAND ----------

spark.sql(str_drop_device_meter_comms_card_qry)
spark.sql(str_insert_device_meter_comms_card_qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_transform.DEVICE_METER_COMMS_CARD_BAU

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH JEST_V4 AS ( 
# MAGIC      SELECT DISTINCT hubsrv.ORDER_NUMBER AS OBJNR
# MAGIC        FROM ( SELECT sat.*
# MAGIC                 FROM imprd001_rawvault.SAT_SRVORDER_CIS_JEST sat
# MAGIC                WHERE date_format(sat.EXTRACT_DATE_TIME,"yyyyMMdd") < date_format('2022-05-11',"yyyyMMdd") and CHANGE_TYPE <> 'D'
# MAGIC             ) JEST 
# MAGIC          join imprd001_rawvault.HUB_SRVORDER hubsrv
# MAGIC          on JEST.HUB_SRVORDER_HSH_KEY=hubsrv.HUB_SRVORDER_HSH_KEY   
# MAGIC         where INACT != 'X' and (stat = 'I0076' or stat =  'I0320')
# MAGIC      ),
# MAGIC EQUI_V5 AS ( 
# MAGIC     SELECT eq.*
# MAGIC     FROM (SELECT lnk.HUB_EQUIPMENT_HSH_KEY
# MAGIC               ,lnk.HUB_DEVICE_HSH_KEY
# MAGIC               ,hub_dev.DEVICE_ID
# MAGIC               ,hub_equi.EQUIPMENT_ID AS EQUI_EQUNR
# MAGIC               ,hub_dev.DEVICE_ID AS SERIAL_NUMBER
# MAGIC               ,equi.ZWAN_TYPE
# MAGIC               ,equi.OBJNR
# MAGIC               ,equi.ERDAT
# MAGIC            FROM ( SELECT lsat.*
# MAGIC                        , ROW_NUMBER() over ( PARTITION BY lsat.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY order by lsat.extract_date_time desc ) as `rec_seq`
# MAGIC                    FROM imprd001_rawvault.LSAT_EQUIPMENT_DEVICE_MATERIAL_CIS_EQUI lsat
# MAGIC                    WHERE DATE_FORMAT(lsat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                       ) equi
# MAGIC             JOIN imprd001_rawvault.LNK_EQUIPMENT_DEVICE_MATERIAL lnk
# MAGIC               ON equi.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY=lnk.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY 
# MAGIC             JOIN imprd001_rawvault.HUB_DEVICE hub_dev
# MAGIC               ON lnk.HUB_DEVICE_HSH_KEY = hub_dev.HUB_DEVICE_HSH_KEY
# MAGIC             JOIN imprd001_rawvault.HUB_EQUIPMENT hub_equi
# MAGIC               ON lnk.HUB_EQUIPMENT_HSH_KEY = hub_equi.HUB_EQUIPMENT_HSH_KEY
# MAGIC             WHERE equi.rec_seq =1
# MAGIC                 AND equi.CHANGE_TYPE <> 'D'
# MAGIC             )eq
# MAGIC 		   LEFT ANTI JOIN JEST_V4 je
# MAGIC              ON eq.OBJNR=je.OBJNR),
# MAGIC EZUG_V7 AS(
# MAGIC      SELECT  salnk.HUB_EQUIPMENT_HSH_KEY1 AS EZUG_LOGIKNR_HSH_KEY
# MAGIC            , salnk.HUB_EQUIPMENT_HSH_KEY2 AS EZUG_LOGIKNR_2_HSH_KEY
# MAGIC            , EZUG.*          
# MAGIC        FROM (SELECT sat.*
# MAGIC                   , ROW_NUMBER() OVER ( PARTITION BY sat.SALNK_EQUIPMENT_COMMS_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
# MAGIC                FROM imprd001_rawvault.LSAT_EQUIPMENT_COMMS_CIS_EZUG sat
# MAGIC                WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                AND ('2022-05-11' BETWEEN  TO_DATE(sat.AB,'yyyyMMdd') AND  TO_DATE(sat.BIS,'yyyyMMdd'))
# MAGIC                AND CHANGE_TYPE <> 'D'
# MAGIC            )EZUG
# MAGIC        JOIN imprd001_rawvault.SALNK_EQUIPMENT_COMMS salnk
# MAGIC          ON EZUG.SALNK_EQUIPMENT_COMMS_HSH_KEY = salnk.SALNK_EQUIPMENT_COMMS_HSH_KEY
# MAGIC       WHERE EZUG.rec_seq = 1
# MAGIC      ),
# MAGIC  EGERH_METER_V8 AS(
# MAGIC      SELECT  salnk.HUB_EQUIPMENT_HSH_KEY1 AS EGERH_EQUNR_METER_HSH_KEY
# MAGIC            , salnk.HUB_EQUIPMENT_HSH_KEY2 AS EGERH_LOGIKNR_METER_HSH_KEY
# MAGIC        FROM (SELECT sat.*
# MAGIC                   , ROW_NUMBER() OVER ( PARTITION BY LNK_NMI_EQUIPMENT2_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
# MAGIC                FROM imprd001_rawvault.LSAT_NMI_EQUIPMENT2_CIS_EGERH sat
# MAGIC                WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                  and sat.KOMBINAT = 'Z'
# MAGIC                  AND ('2022-05-11' BETWEEN  TO_DATE(sat.AB,'yyyyMMdd') AND  TO_DATE(sat.BIS,'yyyyMMdd'))
# MAGIC            )egerh
# MAGIC        JOIN imprd001_rawvault.LNK_NMI_EQUIPMENT2 salnk
# MAGIC          ON egerh.LNK_NMI_EQUIPMENT2_HSH_KEY = salnk.LNK_NMI_EQUIPMENT2_HSH_KEY
# MAGIC         and egerh.rec_seq = 1
# MAGIC         and egerh.CHANGE_TYPE <> 'D'
# MAGIC      ),
# MAGIC EGERH_COMMSCARD_V9 AS(
# MAGIC      SELECT  salnk.HUB_EQUIPMENT_HSH_KEY1 AS EGERH_EQUNR_COMMSCARD_HSH_KEY
# MAGIC            , salnk.HUB_EQUIPMENT_HSH_KEY2 AS EGERH_LOGIKNR_COMMSCARD_HSH_KEY
# MAGIC        FROM (SELECT sat.*
# MAGIC                   , ROW_NUMBER() OVER ( PARTITION BY LNK_NMI_EQUIPMENT2_HSH_KEY ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
# MAGIC                FROM imprd001_rawvault.LSAT_NMI_EQUIPMENT2_CIS_EGERH sat
# MAGIC                WHERE DATE_FORMAT(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                  and sat.KOMBINAT = 'S'
# MAGIC                  AND ('2022-05-11' BETWEEN  TO_DATE(sat.AB,'yyyyMMdd') AND  TO_DATE(sat.BIS,'yyyyMMdd'))
# MAGIC            )egerh
# MAGIC        JOIN imprd001_rawvault.LNK_NMI_EQUIPMENT2 salnk
# MAGIC          ON egerh.LNK_NMI_EQUIPMENT2_HSH_KEY = salnk.LNK_NMI_EQUIPMENT2_HSH_KEY
# MAGIC         and egerh.rec_seq = 1
# MAGIC         and egerh.CHANGE_TYPE <> 'D'
# MAGIC      ),
# MAGIC GET_METER_V10 as (SELECT *
# MAGIC                 FROM (SELECT *
# MAGIC                         ,ROW_NUMBER() OVER(PARTITION BY HUB_DEVICE_HSH_KEY ORDER BY AB DESC) AS REC_SEQ
# MAGIC                       FROM (SELECT EZUG_V7.EZUG_LOGIKNR_2_HSH_KEY
# MAGIC                           , EQUI_V5.HUB_DEVICE_HSH_KEY
# MAGIC                           , EGERH_METER_V8.EGERH_EQUNR_METER_HSH_KEY
# MAGIC                           , EZUG_V7.AB
# MAGIC                           , EZUG_V7.BIS 
# MAGIC                           , EQUI_V5.SERIAL_NUMBER
# MAGIC                           , EQUI_V5.ZWAN_TYPE
# MAGIC                           ,  COALESCE(EQUI_V5.ZWAN_TYPE,'MESH') AS COMMS_MODULE_TYPE
# MAGIC                        FROM EGERH_METER_V8 
# MAGIC                        LEFT JOIN EZUG_V7
# MAGIC                          ON EZUG_V7.EZUG_LOGIKNR_HSH_KEY = EGERH_METER_V8.EGERH_LOGIKNR_METER_HSH_KEY
# MAGIC                        JOIN EQUI_V5
# MAGIC                          ON EQUI_V5.HUB_EQUIPMENT_HSH_KEY = EGERH_METER_V8.EGERH_EQUNR_METER_HSH_KEY
# MAGIC                        JOIN imprd001_transform.ACTIVE_NMI_METER a_nmi
# MAGIC                          ON a_nmi.HUB_EQUIPMENT_HSH_KEY = EGERH_METER_V8.EGERH_LOGIKNR_METER_HSH_KEY
# MAGIC                          )X
# MAGIC                  )y
# MAGIC                  where REC_SEQ=1
# MAGIC      )
# MAGIC SELECT HUB_DEVICE_HSH_KEY
# MAGIC     ,EGERH_EQUNR_METER_HSH_KEY
# MAGIC     ,COMMS_MODULE_TYPE
# MAGIC FROM(
# MAGIC     SELECT GET_METER_V10.HUB_DEVICE_HSH_KEY
# MAGIC         , GET_METER_V10.EGERH_EQUNR_METER_HSH_KEY
# MAGIC         , EQUI_V5.ZWAN_TYPE AS COMMS_MODULE_TYPE
# MAGIC         ,ROW_NUMBER() OVER(PARTITION BY EQUI_V5.HUB_DEVICE_HSH_KEY ORDER BY EQUI_V5.ERDAT DESC) AS rec_seq_er_dt
# MAGIC     FROM GET_METER_V10
# MAGIC     JOIN EGERH_COMMSCARD_V9
# MAGIC          ON GET_METER_V10.EZUG_LOGIKNR_2_HSH_KEY = EGERH_COMMSCARD_V9.EGERH_LOGIKNR_COMMSCARD_HSH_KEY
# MAGIC     JOIN EQUI_V5
# MAGIC          ON EQUI_V5.HUB_EQUIPMENT_HSH_KEY = EGERH_COMMSCARD_V9.EGERH_EQUNR_COMMSCARD_HSH_KEY
# MAGIC     JOIN imprd001_transform.ACTIVE_NMI_METER a_nmi
# MAGIC     ON a_nmi.HUB_EQUIPMENT_HSH_KEY = EGERH_COMMSCARD_V9.EGERH_LOGIKNR_COMMSCARD_HSH_KEY
# MAGIC     WHERE  ('2022-05-11' BETWEEN  TO_DATE(GET_METER_V10.AB,'yyyyMMdd') AND  TO_DATE(GET_METER_V10.BIS,'yyyyMMdd'))
# MAGIC     UNION ALL
# MAGIC     SELECT HUB_DEVICE_HSH_KEY
# MAGIC         , EGERH_EQUNR_METER_HSH_KEY
# MAGIC         ,'MESH' AS COMMS_MODULE_TYPE
# MAGIC         ,1 as rec_seq_er_dt
# MAGIC     FROM GET_METER_V10
# MAGIC     WHERE EZUG_LOGIKNR_2_HSH_KEY IS NULL
# MAGIC      )x
# MAGIC  WHERE rec_seq_er_dt=1
# MAGIC  and HUB_DEVICE_HSH_KEY = sha1('4244699:')
# MAGIC  or EGERH_EQUNR_METER_HSH_KEY =  sha1('4244699:')

# COMMAND ----------

str_drop_device_interval_qry='''DROP table IF EXISTS {}'''.format(DEVICE_INTERVAL_TBL)
str_insert_device_interval_qry='''
CREATE TABLE {} AS 
SELECT HUB_DEVICE_HSH_KEY,
    INTERVAL_TIME_LOCAL AS ITVL_TS,
    'WIMAX' AS COMMS_MODULE_TYPE,
     CASE WHEN TO_DATE(INTERVAL_TIME_LOCAL,'yyyyMMdd') >= DATE_FORMAT(DATE_SUB({}, CAST({} AS INT)), "yyyy-MM-dd") THEN 'Y' ELSE 'N' END AS INTRVL_RCV_FLAG
FROM (SELECT *,row_number() over(PARTITION BY HUB_DEVICE_HSH_KEY order by INTERVAL_TIME_LOCAL desc) as rec_req 
      FROM {} 
      WHERE DATE_FORMAT(EXTRACT_DATE_PARTITION,"yyyyMMdd") BETWEEN DATE_FORMAT(DATE_SUB({}, CAST({} AS INT)), "yyyyMMdd") AND DATE_FORMAT({},"yyyyMMdd")
      ) intvl 
WHERE rec_req=1
UNION
SELECT slink.HUB_DEVICE_HSH_KEY1 AS HUB_DEVICE_HSH_KEY,
   sat.ITVL_TS AS ITVL_TS,
    'MESH' AS COMMS_MODULE_TYPE,
     CASE WHEN TO_DATE(sat.ITVL_TS,'yyyyMMdd') >= DATE_FORMAT(DATE_SUB({}, CAST({} AS INT)), "yyyy-MM-dd") THEN 'Y' ELSE 'N' END AS INTRVL_RCV_FLAG
FROM (select * from (SELECT *,row_number() over(PARTITION BY HUB_DEVICE_HSH_KEY order by ITVL_TS desc) as rec_req 
      FROM {}
      WHERE DATE_FORMAT(EXTRACT_DATE_PARTITION,"yyyyMMdd") BETWEEN DATE_FORMAT(DATE_SUB({}, CAST({} AS INT)), "yyyyMMdd") AND DATE_FORMAT({},"yyyyMMdd")
      ) uiq WHERE rec_req=1) sat 
JOIN {} hubdev 
  on sat.HUB_DEVICE_HSH_KEY=hubdev.HUB_DEVICE_HSH_KEY
JOIN (select  HUB_DEVICE_HSH_KEY1,HUB_DEVICE_HSH_KEY3 
      from {} 
	  group by HUB_DEVICE_HSH_KEY1,HUB_DEVICE_HSH_KEY3
	  ) slink 
  on hubdev.HUB_DEVICE_HSH_KEY=slink.HUB_DEVICE_HSH_KEY3
JOIN {} hubdev_UDI 
  on slink.HUB_DEVICE_HSH_KEY1=hubdev_UDI.HUB_DEVICE_HSH_KEY
'''.format(DEVICE_INTERVAL_TBL,repr(run_dt),comms_fault_threshold_num_days,SAT_DEVICE_PNT_AMI_READ_INTERVAL_TBL,repr(run_dt),interval_scan_days,repr(run_dt),repr(run_dt),comms_fault_threshold_num_days,SAT_DEVICE_UIQ_LP_ITVL_TBL,repr(run_dt),interval_scan_days,repr(run_dt),HUB_DEVICE_TBL,SALNK_DEVICE_NMI_TBL,HUB_DEVICE_TBL)
print(str_drop_device_interval_qry)
print(str_insert_device_interval_qry)

# COMMAND ----------

spark.sql(str_drop_device_interval_qry)
spark.sql(str_insert_device_interval_qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_transform.DEVICE_INTERVAL_BAU

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT HUB_DEVICE_HSH_KEY,
# MAGIC     INTERVAL_TIME_LOCAL AS ITVL_TS,
# MAGIC     'WIMAX' AS COMMS_MODULE_TYPE,
# MAGIC      CASE WHEN TO_DATE(INTERVAL_TIME_LOCAL,'yyyyMMdd') >= DATE_FORMAT(DATE_SUB('2022-05-11', CAST(5 AS INT)), "yyyy-MM-dd") THEN 'Y' ELSE 'N' END AS INTRVL_RCV_FLAG
# MAGIC FROM (SELECT *,row_number() over(PARTITION BY HUB_DEVICE_HSH_KEY order by INTERVAL_TIME_LOCAL desc) as rec_req 
# MAGIC       FROM imprd001_rawvault.SAT_DEVICE_PNT_AMI_READ_INTERVAL 
# MAGIC       WHERE DATE_FORMAT(EXTRACT_DATE_PARTITION,"yyyyMMdd") BETWEEN DATE_FORMAT(DATE_SUB('2022-05-11', CAST(30 AS INT)), "yyyyMMdd") AND DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC       ) intvl 
# MAGIC WHERE rec_req=1
# MAGIC UNION
# MAGIC SELECT slink.HUB_DEVICE_HSH_KEY1 AS HUB_DEVICE_HSH_KEY,
# MAGIC    sat.ITVL_TS AS ITVL_TS,
# MAGIC     'MESH' AS COMMS_MODULE_TYPE,
# MAGIC      CASE WHEN TO_DATE(sat.ITVL_TS,'yyyyMMdd') >= DATE_FORMAT(DATE_SUB('2022-05-11', CAST(5 AS INT)), "yyyy-MM-dd") THEN 'Y' ELSE 'N' END AS INTRVL_RCV_FLAG
# MAGIC FROM (select * from (SELECT *,row_number() over(PARTITION BY HUB_DEVICE_HSH_KEY order by ITVL_TS desc) as rec_req 
# MAGIC       FROM imprd001_rawvault.SAT_DEVICE_UIQ_LP_ITVL
# MAGIC       WHERE DATE_FORMAT(EXTRACT_DATE_PARTITION,"yyyyMMdd") BETWEEN DATE_FORMAT(DATE_SUB('2022-05-11', CAST(30 AS INT)), "yyyyMMdd") AND DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC       ) uiq WHERE rec_req=1) sat 
# MAGIC JOIN imprd001_rawvault.HUB_DEVICE hubdev 
# MAGIC   on sat.HUB_DEVICE_HSH_KEY=hubdev.HUB_DEVICE_HSH_KEY
# MAGIC JOIN (select  HUB_DEVICE_HSH_KEY1,HUB_DEVICE_HSH_KEY3 
# MAGIC       from imprd001_rawvault.SALNK_DEVICE_NMI 
# MAGIC 	  group by HUB_DEVICE_HSH_KEY1,HUB_DEVICE_HSH_KEY3
# MAGIC 	  ) slink 
# MAGIC   on hubdev.HUB_DEVICE_HSH_KEY=slink.HUB_DEVICE_HSH_KEY3
# MAGIC JOIN imprd001_rawvault.HUB_DEVICE hubdev_UDI 
# MAGIC   on slink.HUB_DEVICE_HSH_KEY1=hubdev_UDI.HUB_DEVICE_HSH_KEY
# MAGIC   where slink.HUB_DEVICE_HSH_KEY1 = sha1('4244699:')

# COMMAND ----------


str_drop_device_read_type_cd_qry='''DROP table IF EXISTS {}'''.format(DEVICE_READ_TYPE_CD_TBL)
str_insert_device_read_type_cd_qry='''
CREATE TABLE {} AS 
WITH CIS_ZIPMTT_SYNC_TS_CV as (
                        SELECT *
                               FROM( SELECT * , 
                                  ROW_NUMBER() over ( PARTITION BY HUB_EQUIPMENT_HSH_KEY order by extract_date_time desc )
                              as `rec_seq` 
                              FROM {} 
                              WHERE DATE_FORMAT(EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT({},"yyyyMMdd")
                              AND ({} BETWEEN  TO_DATE(AB,'yyyyMMdd') AND  TO_DATE(BIS,'yyyyMMdd'))
							  AND fname='READ_TYPE_CODE'
                                )
                              WHERE rec_seq=1 and CHANGE_TYPE  != 'D'
                              AND  VALUE like 'RWD%'),
                              
            
READ_TYPE_CODE AS(
                    SELECT * 
                    FROM (SELECT HUB_EQUIPMENT_HSH_KEY,
                               CASE WHEN FNAME='READ_TYPE_CODE' THEN AB END AS  READ_TYPE_CODE_FROM,
                               CASE WHEN FNAME='READ_TYPE_CODE' THEN BIS END AS  READ_TYPE_CODE_TO,
                               CASE WHEN FNAME='READ_TYPE_CODE' THEN VALUE END AS  READ_TYPE_CODE 
                               FROM CIS_ZIPMTT_SYNC_TS_CV)
                               WHERE READ_TYPE_CODE is not null),
SATELLITE AS (
               SELECT DISTINCT HUB_EQUIPMENT_HSH_KEY 
                        FROM CIS_ZIPMTT_SYNC_TS_CV),
SYNTC_TS_READ_TYPE_CD AS (
              SELECT DISTINCT SAT.HUB_EQUIPMENT_HSH_KEY,
                              D.READ_TYPE_CODE
                              FROM SATELLITE AS SAT 
                              LEFT OUTER JOIN  READ_TYPE_CODE AS D 
                              ON SAT.HUB_EQUIPMENT_HSH_KEY=D.HUB_EQUIPMENT_HSH_KEY),
ZIIDTT_DATASTRM AS(
SELECT DISTINCT hub_equi.HUB_EQUIPMENT_HSH_KEY
FROM ( SELECT * 
       FROM   ( SELECT lsat.LNK_NMI_DEVICE_HSH_KEY,
		            IF(TRIM(lsat.DATATYPE) ='',null,TRIM(lsat.DATATYPE))           AS DATA_STREAM_TYPE,
		            IF(TRIM(lsat.DATASTATUS) ='',null,TRIM(lsat.DATASTATUS))       AS DATA_STREAM_STATUS_CODE,
		            IF(TRIM(lsat.MARKETREG) ='',null,TRIM(lsat.MARKETREG))         AS MARKET_REGISTRATION_FLAG,
                    ROW_NUMBER() OVER ( PARTITION BY lsat.LNK_NMI_DEVICE_HSH_KEY ORDER BY lsat.extract_date_time desc,lsat.DATASUFFIX ) as rec_seq
                FROM {} lsat
                WHERE date_format(lsat.EXTRACT_DATE_TIME,"yyyyMMdd") < date_format({},"yyyyMMdd")
            ) d_strm
         WHERE d_strm.rec_seq = 1) dastrm
		JOIN {} lnk
         ON dastrm.LNK_NMI_DEVICE_HSH_KEY = lnk.LNK_NMI_DEVICE_HSH_KEY
		JOIN {} hubdev 
         on lnk.HUB_DEVICE_HSH_KEY = hubdev.HUB_DEVICE_HSH_KEY
       JOIN {} lnk_equi 
         ON lnk.HUB_DEVICE_HSH_KEY = lnk_equi.HUB_DEVICE_HSH_KEY
       JOIN {} hub_equi 
         ON lnk_equi.HUB_EQUIPMENT_HSH_KEY = hub_equi.HUB_EQUIPMENT_HSH_KEY
WHERE DATA_STREAM_TYPE = 'I'
		AND DATA_STREAM_STATUS_CODE = 'A'
		AND IF(TRIM(hubdev.DEVICE_ID) ='',null,TRIM(hubdev.DEVICE_ID)) IS NOT NULL)
SELECT DISTINCT st.HUB_EQUIPMENT_HSH_KEY,
    st.READ_TYPE_CODE
FROM SYNTC_TS_READ_TYPE_CD st
JOIN ZIIDTT_DATASTRM zd 
ON st.HUB_EQUIPMENT_HSH_KEY=zd.HUB_EQUIPMENT_HSH_KEY
'''.format(DEVICE_READ_TYPE_CD_TBL,SAT_EQUIPMENT_CIS_ZIPMTT_SYNC_TS_TBL,repr(run_dt),repr(run_dt),LSAT_NMI_DEVICE_CIS_ZIIDTT_DATASTRM_TBL,repr(run_dt),LNK_NMI_DEVICE_TBL,HUB_DEVICE_TBL,LNK_EQUIPMENT_DEVICE_MATERIAL_TBL,HUB_EQUIPMENT_TBL)
print(str_drop_device_read_type_cd_qry)
print(str_insert_device_read_type_cd_qry)

# COMMAND ----------

spark.sql(str_drop_device_read_type_cd_qry)
spark.sql(str_insert_device_read_type_cd_qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_transform.DEVICE_READ_TYPE_CD_BAU

# COMMAND ----------

str_drop_active_nmi_device_read_type_qry='''DROP table IF EXISTS {}'''.format(ACTIVE_NMI_DEVICE_READ_TYPE_TBL)
str_insert_active_nmi_device_read_type_qry='''
CREATE TABLE {} AS 
select *
from (SELECT a.*,
    b.READ_TYPE_CODE,
    row_number() over(partition by HUB_DEVICE_HSH_KEY order by READ_TYPE_CODE desc) as rec_seq
    FROM {} a 
    JOIN {} b 
    ON a.EGERH_EQUNR_METER_HSH_KEY = b.HUB_EQUIPMENT_HSH_KEY
    )x
where rec_seq=1
'''.format(ACTIVE_NMI_DEVICE_READ_TYPE_TBL,DEVICE_METER_COMMS_CARD_TBL,DEVICE_READ_TYPE_CD_TBL)
print(str_drop_active_nmi_device_read_type_qry)
print(str_insert_active_nmi_device_read_type_qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from (SELECT a.*,
# MAGIC     b.READ_TYPE_CODE,
# MAGIC     row_number() over(partition by HUB_DEVICE_HSH_KEY order by READ_TYPE_CODE desc) as rec_seq
# MAGIC     FROM imprd001_transform.DEVICE_METER_COMMS_CARD a 
# MAGIC     JOIN imprd001_transform.DEVICE_READ_TYPE_CD b 
# MAGIC     ON a.EGERH_EQUNR_METER_HSH_KEY = b.HUB_EQUIPMENT_HSH_KEY
# MAGIC     )x
# MAGIC where 
# MAGIC HUB_DEVICE_HSH_KEY = sha1('4244699:') or 
# MAGIC EGERH_EQUNR_METER_HSH_KEY = sha1('4244699:')

# COMMAND ----------

spark.sql(str_drop_active_nmi_device_read_type_qry)
spark.sql(str_insert_active_nmi_device_read_type_qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_transform.ACTIVE_NMI_DEVICE_READ_TYPE_BAU

# COMMAND ----------

str_drop_device_meter_qry='''DROP table IF EXISTS {}'''.format(DEVICE_METER_TBL)
str_insert_device_meter_qry='''
CREATE TABLE {} 
AS SELECT HUB_DEVICE_HSH_KEY,
       SERIAL_NUMBER AS METER_SERIAL_NUMBER,
       NIC_MAC_NAME AS MAC_ADDRESS,
       METER_FIRMWARE,
       NIC_FIRMWARE AS COMMS_FIRMWARE,
       LAST_COMM_TS AS METER_LAST_COMMS_DATE_TIME,
       'MESH' AS  COMMS_MODULE_TYPE 
   FROM (SELECT *,ROW_NUMBER() over ( partition BY HUB_DEVICE_HSH_KEY order by extract_date_time desc ) as rec_req  
         FROM {}
         ) mesh 
   WHERE rec_req=1 
   UNION 
   SELECT HUB_DEVICE_HSH_KEY,
       SERIAL_NUMBER AS METER_SERIAL_NUMBER,
       MAC_ADDRESS,
       METER_FRIMWARE,
       COMMS_FIRMWARE,
       LAST_NETWORK_ENTRY AS METER_LAST_COMMS_DATE_TIME,
       'WIMAX' AS COMMS_MODULE_TYPE  
   FROM (SELECT *,ROW_NUMBER() over ( partition BY HUB_DEVICE_HSH_KEY order by extract_date_time desc ) as rec_req
         FROM {}
         ) wimax 
   WHERE rec_req=1'''.format(DEVICE_METER_TBL,BSAT_METER_DETAILS_MESH_TBL,BSAT_METER_DETAILS_WIMAX_TBL)
#print('str_drop_device_meter_qry:',str_drop_device_meter_qry)
print('str_insert_device_meter_qry:',str_insert_device_meter_qry)

# COMMAND ----------

spark.sql(str_drop_device_meter_qry)
spark.sql(str_insert_device_meter_qry)

# COMMAND ----------

str_drop_actv_nmi_device_meter_qry='''DROP table IF EXISTS {}'''.format(ACTV_NMI_DEVICE_METER_TBL)
str_insert_actv_nmi_device_meter_qry='''
CREATE TABLE {} AS
SELECT DISTINCT a.*,
    hub_dev. DEVICE_ID AS METER_SERIAL_NUMBER,
    b.MAC_ADDRESS,
    b.METER_FIRMWARE,
    b.COMMS_FIRMWARE,
    b.METER_LAST_COMMS_DATE_TIME 
FROM {} a 
JOIN {} hub_dev
  ON a.HUB_DEVICE_HSH_KEY = hub_dev.HUB_DEVICE_HSH_KEY
LEFT JOIN {} b 
	ON (a.HUB_DEVICE_HSH_KEY = b.HUB_DEVICE_HSH_KEY 
		AND CASE WHEN a.COMMS_MODULE_TYPE IN ('MICROAP','MESH') THEN 'MESH' WHEN a.COMMS_MODULE_TYPE IN ('3G','WIMAX')  THEN 'WIMAX' ELSE NULL END = b.COMMS_MODULE_TYPE)
'''.format(ACTV_NMI_DEVICE_METER_TBL,ACTIVE_NMI_DEVICE_READ_TYPE_TBL,HUB_DEVICE_TBL,DEVICE_METER_TBL) 
#print(str_drop_actv_nmi_device_meter_qry)
print(str_insert_actv_nmi_device_meter_qry)

# COMMAND ----------

spark.sql(str_drop_actv_nmi_device_meter_qry)
spark.sql(str_insert_actv_nmi_device_meter_qry)

# COMMAND ----------

str_dlt_bsat_comms_fault_outbound_qry='''DELETE FROM {} WHERE RUN_DATE={}'''.format(BSAT_COMMS_FAULT_OUTBOUND_TBL,repr(run_dt))
str_insert_bsat_comms_fault_outbound_qry='''
INSERT INTO {} 
SELECT HUB_DEVICE_HSH_KEY,
	METER_SERIAL_NUMBER,
	DATE_FORMAT(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(),'GMT+10'),'yyyy-MM-dd HH:mm:ss') AS REPORT_RUN_DATE_TIME,
	"OUTBOUND_COMMS_FAULT" AS RECORD_SOURCE,
	SHA1(CONCAT(
            TRIM(COALESCE(cast(METER_SERIAL_NUMBER as string),'')),':',
            TRIM(COALESCE(cast(MAC_ADDRESS as string),'')),':',
            TRIM(COALESCE(cast(METER_FIRMWARE as string),'')),':',
            TRIM(COALESCE(cast(READ_TYPE_CODE as string),'')),':',
            TRIM(COALESCE(cast(COMMS_CARD_TYPE as string),'')),':',
            TRIM(COALESCE(cast(COMMS_FIRMWARE as string),'')),':',
            TRIM(COALESCE(cast(METER_LAST_COMMS_DATE_TIME as string),'')),':',
			TRIM(COALESCE(cast(ITVL_TS as string),'')) )
     ) AS BSAT_COMMS_FAULT_OUTBOUND_HSH_DIFF,
	MAC_ADDRESS,
	METER_FIRMWARE,
	READ_TYPE_CODE,
	COMMS_CARD_TYPE,
	COMMS_FIRMWARE,
	METER_LAST_COMMS_DATE_TIME,
	ITVL_TS,
    {}
FROM (SELECT DISTINCT c.HUB_DEVICE_HSH_KEY,
          c.METER_SERIAL_NUMBER,
          c.MAC_ADDRESS,
          c.METER_FIRMWARE,
          c.READ_TYPE_CODE,
          c.COMMS_MODULE_TYPE AS COMMS_CARD_TYPE,
          c.COMMS_FIRMWARE,
          c.METER_LAST_COMMS_DATE_TIME,
          CASE WHEN d.HUB_DEVICE_HSH_KEY IS NULL THEN NULL ELSE DATE(d.ITVL_TS) END AS ITVL_TS 
      FROM (SELECT a.*
	        FROM {} a 
            WHERE NOT EXISTS
            			(SELECT 1 
	                    FROM (SELECT * 
						      FROM {} 
							  WHERE INTRVL_RCV_FLAG='Y'
							  ) b 
						WHERE a.HUB_DEVICE_HSH_KEY = b.HUB_DEVICE_HSH_KEY
						    AND CASE WHEN a.COMMS_MODULE_TYPE IN ('MICROAP','MESH') THEN 'MESH' WHEN a.COMMS_MODULE_TYPE IN ('3G','WIMAX')  THEN 'WIMAX' ELSE NULL END = b.COMMS_MODULE_TYPE
						)
			)c
		LEFT JOIN (SELECT *
				   FROM {}
				   WHERE INTRVL_RCV_FLAG='N')d
	    ON c.HUB_DEVICE_HSH_KEY=d.HUB_DEVICE_HSH_KEY
            AND CASE WHEN c.COMMS_MODULE_TYPE IN ('MICROAP','MESH') THEN 'MESH' WHEN c.COMMS_MODULE_TYPE IN ('3G','WIMAX')  THEN 'WIMAX' ELSE NULL END = d.COMMS_MODULE_TYPE
   )z
'''.format(BSAT_COMMS_FAULT_OUTBOUND_TBL,repr(run_dt),ACTV_NMI_DEVICE_METER_TBL,DEVICE_INTERVAL_TBL,DEVICE_INTERVAL_TBL) 
#print(str_dlt_bsat_comms_fault_outbound_qry)
print(str_insert_bsat_comms_fault_outbound_qry)

# COMMAND ----------

spark.sql(str_dlt_bsat_comms_fault_outbound_qry)
spark.sql(str_insert_bsat_comms_fault_outbound_qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clean Up
# MAGIC 
# MAGIC DROP table IF EXISTS imprd001_transform.ACTIVE_NMI_METER_BAU;
# MAGIC DROP table IF EXISTS imprd001_transform.DEVICE_METER_COMMS_CARD_TBL_BAU;

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT *
# MAGIC                                FROM( SELECT * , 
# MAGIC                                   ROW_NUMBER() over ( PARTITION BY HUB_EQUIPMENT_HSH_KEY order by extract_date_time desc )
# MAGIC                               as `rec_seq` 
# MAGIC                               FROM imprd001_rawvault.SAT_EQUIPMENT_CIS_ZIPMTT_SYNC_TS 
# MAGIC                               WHERE DATE_FORMAT(EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                               AND ('2022-05-11' BETWEEN  TO_DATE(AB,'yyyyMMdd') AND  TO_DATE(BIS,'yyyyMMdd'))
# MAGIC 							  AND fname='READ_TYPE_CODE'
# MAGIC                                 )
# MAGIC                               WHERE rec_seq=1 and CHANGE_TYPE  != 'D'
# MAGIC                               AND  VALUE like 'RWD%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_rawvault.SAT_EQUIPMENT_CIS_ZIPMTT_SYNC_TS
# MAGIC where HUB_EQUIPMENT_HSH_KEY in ('9a16803ed7c0f0949fff65151382dc483cc7e020','e1e9dbdbe982da3a36f718cc51c0503f053ab15e','2e000a053ad2c5117050020de06f55bca218d4c8')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_rawvault.HUB_EQUIPMENT
# MAGIC where HUB_EQUIPMENT_HSH_KEY in ('9a16803ed7c0f0949fff65151382dc483cc7e020','e1e9dbdbe982da3a36f718cc51c0503f053ab15e','2e000a053ad2c5117050020de06f55bca218d4c8')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lnk.HUB_EQUIPMENT_HSH_KEY
# MAGIC               ,lnk.HUB_DEVICE_HSH_KEY
# MAGIC               ,hub_dev.DEVICE_ID
# MAGIC               ,hub_equi.EQUIPMENT_ID AS EQUI_EQUNR
# MAGIC               ,hub_dev.DEVICE_ID AS SERIAL_NUMBER
# MAGIC               ,equi.ZWAN_TYPE
# MAGIC               ,equi.OBJNR
# MAGIC               ,equi.ERDAT
# MAGIC            FROM ( SELECT lsat.*
# MAGIC                        , ROW_NUMBER() over ( PARTITION BY lsat.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY order by lsat.extract_date_time desc ) as `rec_seq`
# MAGIC                    FROM imprd001_rawvault.LSAT_EQUIPMENT_DEVICE_MATERIAL_CIS_EQUI lsat
# MAGIC                    WHERE DATE_FORMAT(lsat.EXTRACT_DATE_TIME,"yyyyMMdd") <= DATE_FORMAT('2022-05-11',"yyyyMMdd")
# MAGIC                       ) equi
# MAGIC             JOIN imprd001_rawvault.LNK_EQUIPMENT_DEVICE_MATERIAL lnk
# MAGIC               ON equi.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY=lnk.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY 
# MAGIC             JOIN imprd001_rawvault.HUB_DEVICE hub_dev
# MAGIC               ON lnk.HUB_DEVICE_HSH_KEY = hub_dev.HUB_DEVICE_HSH_KEY
# MAGIC             JOIN imprd001_rawvault.HUB_EQUIPMENT hub_equi
# MAGIC               ON lnk.HUB_EQUIPMENT_HSH_KEY = hub_equi.HUB_EQUIPMENT_HSH_KEY
# MAGIC             WHERE equi.rec_seq =1
# MAGIC                 AND equi.CHANGE_TYPE <> 'D'
# MAGIC                 and hub_dev.DEVICE_ID = '4244699'
