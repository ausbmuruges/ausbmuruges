-- Databricks notebook source
desc imprd001_rawvault.LSAT_NMI_SWITCHDOC_CIS_EIDESWITDOC

-- COMMAND ----------

with 
-- Get NMI 
EUITRANS_V1 as(
     select hub_nmi.NMI
          , salnk.HUB_NMI_HSH_KEY2
       from (select  sat.*
                    ,ROW_NUMBER() OVER ( partition BY sat.SALNK_NMI_HSH_KEY, sat.DATEFROM ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               from im${logicalenv}_rawvault.LSAT_NMI_CIS_EUITRANS sat
              --FOR Each valid NMI there must be a check-sum value   
			  where length(trim(sat.NMI_CHECKSUM)) > 0
            )EUITRANS
       join im${logicalenv}_rawvault.SALNK_NMI salnk
         on EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
       join im${logicalenv}_rawvault.HUB_NMI hub_nmi
         on salnk.HUB_NMI_HSH_KEY1 = hub_nmi.HUB_NMI_HSH_KEY
      where EUITRANS.rec_seq = 1
        and EUITRANS.CHANGE_TYPE <> 'D'
     ),
EUIINSTLN_V2 as
(
select EUIINSTLN.*
     , salnk.HUB_NMI_HSH_KEY1
     , salnk.HUB_NMI_HSH_KEY2

  from (select sat.*, 
              ROW_NUMBER() OVER ( partition BY SALNK_NMI_HSH_KEY, sat.DATEFROM ORDER BY sat.extract_date_time DESC ) as `rec_seq`
          from im${logicalenv}_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
       )EUIINSTLN
  join im${logicalenv}_rawvault.SALNK_NMI salnk
    on EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
 where EUIINSTLN.rec_seq = 1
   and EUIINSTLN.CHANGE_TYPE <> 'D'
),
EASTS_V3 as
(
  select lnk.HUB_NMI_HSH_KEY
       , lnk.HUB_EQUIPMENT_HSH_KEY
       , EASTS.*
    from (select sat.*
             , ROW_NUMBER() OVER ( partition BY sat.LNK_NMI_EQUIPMENT_HSH_KEY, sat.AB ORDER BY sat.extract_date_time DESC) as `rec_seq`
            from im${logicalenv}_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTS sat
         )EASTS
    join im${logicalenv}_rawvault.LNK_NMI_EQUIPMENT lnk
      on EASTS.LNK_NMI_EQUIPMENT_HSH_KEY = lnk.LNK_NMI_EQUIPMENT_HSH_KEY
    where EASTS.rec_seq = 1
      and EASTS.CHANGE_TYPE <> 'D'
),
-- GET suffix and timeline information 
ETDZ_V4 as
(select ETDZ.KENNZIFF
      , ETDZ.AB
      , ETDZ.BIS
      , ETDZ.AEDAT
      , salnk.HUB_EQUIPMENT_HSH_KEY2
   from (select sat.*
              , ROW_NUMBER() OVER ( partition BY sat.SALNK_EQUIPMENT_HSH_KEY, sat.ZWNUMMER ORDER BY sat.EXTRACT_DATE_TIME DESC) as `rec_seq`
           from im${logicalenv}_rawvault.LSAT_EQUIPMENT_CIS_ETDZ sat
          where ( Upper(KENNZIFF) like 'E%'
               or Upper(KENNZIFF) like 'B%'
                )
            -- Register type - C is for Current    
            and ZWART = 'C'     
         )ETDZ
   join im${logicalenv}_rawvault.SALNK_EQUIPMENT salnk
     on ETDZ.SALNK_EQUIPMENT_HSH_KEY = salnk.SALNK_EQUIPMENT_HSH_KEY   
  where ETDZ.rec_seq = 1
)
SELECT distinct
        EUITRANS_V1.NMI  AS NMI
      , ETDZ_V4.KENNZIFF AS NMI_SUFFIX
      , ETDZ_V4.AB AS FROM_DT
      , ETDZ_V4.BIS AS TO_DT
      , date_format(from_utc_timestamp(current_timestamp(),'GMT+10'),'yyyy-MM-dd HH:mm:ss') as ROW_INSERT_DTM
FROM EUITRANS_V1 
JOIN EUIINSTLN_V2  
  on EUITRANS_V1.HUB_NMI_HSH_KEY2 = EUIINSTLN_V2.HUB_NMI_HSH_KEY2
JOIN EASTS_V3 
  on EUIINSTLN_V2.HUB_NMI_HSH_KEY1 = EASTS_V3.HUB_NMI_HSH_KEY
JOIN ETDZ_V4
  on EASTS_V3.HUB_EQUIPMENT_HSH_KEY = ETDZ_V4.HUB_EQUIPMENT_HSH_KEY2
  where EUITRANS_V1.NMI = '6305004913'
