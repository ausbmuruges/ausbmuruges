-- Databricks notebook source
WITH EQUI_V1
AS (
	SELECT lnk.HUB_EQUIPMENT_HSH_KEY AS EQUI_HSH_KEY,
		hub_dev.DEVICE_ID
	FROM (
		SELECT lsat.*,
			ROW_NUMBER() OVER (
				PARTITION BY lsat.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY ORDER BY lsat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_EQUIPMENT_DEVICE_MATERIAL_CIS_EQUI lsat
		WHERE date_format(lsat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20',"yyyyMMdd")
		) equi
	JOIN imprd001_rawvault.LNK_EQUIPMENT_DEVICE_MATERIAL lnk ON equi.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY = lnk.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY
	JOIN imprd001_rawvault.HUB_DEVICE hub_dev ON lnk.HUB_DEVICE_HSH_KEY = hub_dev.HUB_DEVICE_HSH_KEY
	WHERE equi.rec_seq = 1
		AND equi.CHANGE_TYPE <> 'D'
	),
-- get meter attrivutes and timelines
CIS_ZIPMTT_SYNC_TS_CV
AS (
	SELECT *
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY HUB_EQUIPMENT_HSH_KEY,
				FNAME,
				AB ORDER BY extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.SAT_EQUIPMENT_CIS_ZIPMTT_SYNC_TS
		)
	WHERE rec_seq = 1
		AND CHANGE_TYPE != 'D'
	),
READ_TYPE_CODE
AS (
	SELECT *
	FROM (
		SELECT HUB_EQUIPMENT_HSH_KEY,
			CASE 
				WHEN FNAME = 'READ_TYPE_CODE'
					THEN AB
				END AS READ_TYPE_CODE_FROM,
			CASE 
				WHEN FNAME = 'READ_TYPE_CODE'
					THEN BIS
				END AS READ_TYPE_CODE_TO,
			CASE 
				WHEN FNAME = 'READ_TYPE_CODE'
					THEN VALUE
				END AS READ_TYPE_CODE
		FROM CIS_ZIPMTT_SYNC_TS_CV
		)
	WHERE READ_TYPE_CODE IS NOT NULL
	),
INSTALL_TYPE_CODE
AS (
	SELECT *
	FROM (
		SELECT HUB_EQUIPMENT_HSH_KEY,
			CASE 
				WHEN FNAME = 'INSTALL_TYPE_CODE'
					THEN AB
				END AS INSTALL_TYPE_CODE_FROM,
			CASE 
				WHEN FNAME = 'INSTALL_TYPE_CODE'
					THEN BIS
				END AS INSTALL_TYPE_CODE_TO,
			CASE 
				WHEN FNAME = 'INSTALL_TYPE_CODE'
					THEN VALUE
				END AS INSTALL_TYPE_CODE
		FROM CIS_ZIPMTT_SYNC_TS_CV
		)
	WHERE INSTALL_TYPE_CODE IS NOT NULL
	),
	-- get EQUNR and LOGIK NR for Type 5-6 meters                             
EGERH_V3
AS (
	SELECT DISTINCT egerh.*,
		hub.EQUIPMENT_ID AS EQUNR,
		salnk.HUB_EQUIPMENT_HSH_KEY1 AS EQUNR_HSH_KEY,
		salnk.HUB_EQUIPMENT_HSH_KEY2 AS LOGIKNR_HSH_KEY,
		hub2.EQUIPMENT_ID AS LOGIKNR
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_EQUIPMENT2_HSH_KEY,
				sat.AB ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_EQUIPMENT2_CIS_EGERH sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20',"yyyyMMdd") and
			KOMBINAT = 'Z'-- filter for TYpe 5-6 meters
		) EGERH
	JOIN imprd001_rawvault.LNK_NMI_EQUIPMENT2 salnk ON EGERH.LNK_NMI_EQUIPMENT2_HSH_KEY = salnk.LNK_NMI_EQUIPMENT2_HSH_KEY
		AND EGERH.rec_seq = 1
		AND EGERH.CHANGE_TYPE <> 'D'
	JOIN imprd001_rawvault.HUB_EQUIPMENT hub ON salnk.HUB_EQUIPMENT_HSH_KEY1 = hub.HUB_EQUIPMENT_HSH_KEY
	JOIN imprd001_rawvault.HUB_EQUIPMENT hub2 ON salnk.HUB_EQUIPMENT_HSH_KEY2 = hub2.HUB_EQUIPMENT_HSH_KEY
	JOIN imprd001_rawvault.LNK_EQUIPMENT_DEVICE_MATERIAL lnk_equi ON salnk.HUB_EQUIPMENT_HSH_KEY1 = lnk_equi.HUB_EQUIPMENT_HSH_KEY
	),
	-- get installation key based on logiknr created   
EASTL_V4
AS (
	SELECT eastl.*,
		lnk.HUB_NMI_HSH_KEY AS ANLAGE_HSH_KEY,
		lnk.HUB_EQUIPMENT_HSH_KEY AS LOGIKNR_HSH_KEY,
		EASTL.AB AS INSTALL_DATE,
		EASTL.BIS AS REMOVAL_DATE
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.LNK_NMI_EQUIPMENT_HSH_KEY,
				sat.AB ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTL sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20',"yyyyMMdd")
		) EASTL
	JOIN imprd001_rawvault.LNK_NMI_EQUIPMENT lnk ON EASTL.LNK_NMI_EQUIPMENT_HSH_KEY = LNK.LNK_NMI_EQUIPMENT_HSH_KEY
		AND EASTL.CHANGE_TYPE <> 'D'
	JOIN imprd001_rawvault.HUB_EQUIPMENT hub ON lnk.HUB_EQUIPMENT_HSH_KEY = hub.HUB_EQUIPMENT_HSH_KEY
	),
	-- get int_ui from installation key     
EUIINSTLN_V5
AS (
	SELECT hub.nmi AS ANLAGE,
		salnk.HUB_NMI_HSH_KEY1 AS ANLAGE_HSH_KEY,
		salnk.HUB_NMI_HSH_KEY2 AS INT_UI_HSH_KEY
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.SALNK_NMI_HSH_KEY,
				sat.DATEFROM,
				sat.TIMEFROM ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20',"yyyyMMdd")
		) EUIINSTLN
	JOIN imprd001_rawvault.SALNK_NMI salnk ON EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUIINSTLN.rec_seq = 1
		AND EUIINSTLN.CHANGE_TYPE <> 'D'
	JOIN imprd001_rawvault.HUB_NMI hub ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
	),
	-- get NMI from EUITRANS and NMI_FROM and NMI_TO dates
EUITRANS_V6
AS (
	SELECT hub1_nmi.NMI AS EXT_UI,
		salnk.HUB_NMI_HSH_KEY1 AS EXT_UI_HSH_KEY,
		salnk.HUB_NMI_HSH_KEY2 AS INT_UI_HSH_KEY,
		EUITRANS.DATEFROM,
		EUITRANS.DATETO
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.SALNK_NMI_HSH_KEY,
				sat.DATEFROM,
				sat.TIMEFROM ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_CIS_EUITRANS sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20',"yyyyMMdd") AND
			 length(trim(sat.NMI_CHECKSUM)) > 0 
		) EUITRANS
	JOIN imprd001_rawvault.SALNK_NMI salnk ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUITRANS.rec_seq = 1
		AND EUITRANS.CHANGE_TYPE <> 'D'
	JOIN imprd001_rawvault.HUB_NMI hub1_nmi ON salnk.HUB_NMI_HSH_KEY1 = hub1_nmi.HUB_NMI_HSH_KEY
	LEFT OUTER JOIN imprd001_rawvault.HUB_NMI hub2_nmi ON salnk.HUB_NMI_HSH_KEY2 = hub2_nmi.HUB_NMI_HSH_KEY
  --   WHERE hub1_nmi.NMI='6305859889' 
	),
    
EGERR_V7 AS(
     select  egerr.*
           , hub.EQUIPMENT_ID as EQUNR
           , hub2.EQUIPMENT_ID as LOGIKNR
           , salnk.HUB_EQUIPMENT_HSH_KEY1
           , salnk.HUB_EQUIPMENT_HSH_KEY2
           , hub_dev.DEVICE_ID
       from (select sat.*
                  , ROW_NUMBER() OVER ( partition BY LNK_DEVICE_EQUIPMENT2_HSH_KEY, sat.AB ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               from imprd001_rawvault.LSAT_DEVICE_EQUIPMENT2_CIS_EGERR sat
               WHERE date_format(sat.EXTRACT_DATE_TIME,"yyyyMMdd") <= date_format('2021-09-20',"yyyyMMdd")
           )EGERR
       join imprd001_rawvault.LNK_DEVICE_EQUIPMENT2 salnk
         on EGERR.LNK_DEVICE_EQUIPMENT2_HSH_KEY = salnk.LNK_DEVICE_EQUIPMENT2_HSH_KEY
        and EGERR.rec_seq = 1
        and EGERR.CHANGE_TYPE <> 'D'
       join imprd001_rawvault.HUB_EQUIPMENT hub
         on salnk.HUB_EQUIPMENT_HSH_KEY1 = hub.HUB_EQUIPMENT_HSH_KEY
       join imprd001_rawvault.HUB_EQUIPMENT hub2
         on salnk.HUB_EQUIPMENT_HSH_KEY2 = hub2.HUB_EQUIPMENT_HSH_KEY
       join imprd001_rawvault.HUB_DEVICE hub_dev
         on salnk.HUB_DEVICE_HSH_KEY = hub_dev.HUB_DEVICE_HSH_KEY
     ),
     
NMI_INSTALLATION_V8 AS(
     select EUITRANS_V6.EXT_UI as NMI
          , EUIINSTLN_V5.ANLAGE
          , EUIINSTLN_V5.ANLAGE_HSH_KEY

      from EUITRANS_V6
       join EUIINSTLN_V5
         on EUITRANS_V6.INT_UI_HSH_KEY = EUIINSTLN_V5.INT_UI_HSH_KEY
     ),
     
METER_TYPE_1_4_V9 AS(
     select temp_meter.NMI
          , to_date(min(temp_meter.AB),'yyyyMMdd')    AS INSTALLED_ON_DT
          , to_date(max(temp_meter.BIS),'yyyyMMdd')   AS REMOVED_ON_DT
          , temp_meter.DEVICE_ID AS SERIAL_NUMBER
          , temp_meter.EQUNR     AS EQUIPMENT_NUMBER
          , if(trim(temp_meter.DEVICE_CATEGORY) ='',null,trim(temp_meter.DEVICE_CATEGORY)) AS DEVICE_CATEGORY

      from (
         select a.NMI
              , c.AB       AS AB -- changed this code(b.AB to C.AB) as part of issue found in nmi meter details
              , c.BIS      AS BIS
              , c.DEVICE_ID
              , NULL       AS EQUNR
              , c.ZWGRUPPE AS DEVICE_CATEGORY 

          from NMI_INSTALLATION_V8 a
          join EASTL_V4 b
            on a.ANLAGE_HSH_KEY = b.ANLAGE_HSH_KEY
          join EGERR_V7 c
            on b.LOGIKNR_HSH_KEY = c.HUB_EQUIPMENT_HSH_KEY2
       )temp_meter
       GROUP BY NMI, EQUNR, DEVICE_ID, DEVICE_CATEGORY    
     ),
	-- Consolidating NMI and METER install date or removal date
NMI_JOIN_View
AS (
	SELECT EQUNR_HSH_KEY,
		MIN(AB) AS INSTALL_DATE,
		MAX(BIS) AS REMOVAL_DATE,
		NMI,
		NMI_FROM,
		NMI_TO
	FROM (
		SELECT DISTINCT EGERH_V3.EQUNR_HSH_KEY,
			EASTL_V4.AB AS AB,
			EASTL_V4.BIS AS BIS,
			EUITRANS_V6.EXT_UI AS NMI,
			EUITRANS_V6.DATEFROM AS NMI_FROM,
			EUITRANS_V6.DATETO AS NMI_TO
		FROM EGERH_V3
		INNER JOIN EASTL_V4 ON EGERH_V3.LOGIKNR_HSH_KEY = EASTL_V4.LOGIKNR_HSH_KEY
		INNER JOIN EUIINSTLN_V5 ON EASTL_V4.ANLAGE_HSH_KEY = EUIINSTLN_V5.ANLAGE_HSH_KEY
		INNER JOIN EUITRANS_V6 ON EUIINSTLN_V5.INT_UI_HSH_KEY = EUITRANS_V6.INT_UI_HSH_KEY
		)
	GROUP BY EQUNR_HSH_KEY,
		NMI,
		NMI_FROM,
		NMI_TO
	),
	-- combining all attributes                             
JOINED_VIEW
AS (
	SELECT NMI_JOIN_View.NMI,
		NMI_JOIN_View.NMI_FROM AS NMI_FROM,
		NMI_JOIN_View.NMI_TO AS NMI_TO,
		EQUI_V1.DEVICE_ID AS METER_SERIAL_NUMBER,
		READ_TYPE_CODE.READ_TYPE_CODE,
		READ_TYPE_CODE.READ_TYPE_CODE_FROM,
		READ_TYPE_CODE.READ_TYPE_CODE_TO,
		INSTALL_TYPE_CODE.INSTALL_TYPE_CODE,
		INSTALL_TYPE_CODE.INSTALL_TYPE_CODE_FROM,
		INSTALL_TYPE_CODE.INSTALL_TYPE_CODE_TO,
		--               EUIINSTLN_V5.ANLAGE,
		NMI_JOIN_View.INSTALL_DATE,
		NMI_JOIN_View.REMOVAL_DATE,
		array_min(array(INSTALL_TYPE_CODE.INSTALL_TYPE_CODE_TO, READ_TYPE_CODE.READ_TYPE_CODE_TO, NMI_JOIN_View.REMOVAL_DATE)) AS MIN_TO_DATE
	FROM EQUI_V1
	LEFT OUTER JOIN READ_TYPE_CODE ON EQUI_V1.EQUI_HSH_KEY = READ_TYPE_CODE.HUB_EQUIPMENT_HSH_KEY
	LEFT OUTER JOIN INSTALL_TYPE_CODE ON EQUI_V1.EQUI_HSH_KEY = INSTALL_TYPE_CODE.HUB_EQUIPMENT_HSH_KEY
	INNER JOIN NMI_JOIN_View ON EQUI_V1.EQUI_HSH_KEY = NMI_JOIN_View.EQUNR_HSH_KEY
	),
	-- filtering dummy rows after join
JOINED_VIEW_Dup
AS (
	SELECT *
	FROM JOINED_VIEW
	WHERE nvl(READ_TYPE_CODE_FROM, 0) <= nvl(MIN_TO_DATE, 99999999)
		AND nvl(INSTALL_TYPE_CODE_FROM, 0) <= nvl(MIN_TO_DATE, 99999999)
		and  nvl(INSTALL_DATE,0) <= nvl(MIN_TO_DATE,99999999)
		--AND nvl(NMI_FROM, 0) <= nvl(MIN_TO_DATE, 99999999)
	),
	-- Logic for TIMELINE_FROM starts here
METER_EFFECTIVE_TIME_TEMP
AS (
	SELECT JOINED_VIEW_Dup.*,
		LAG(MIN_TO_DATE) OVER (
			PARTITION BY NMI,
			METER_SERIAL_NUMBER ORDER BY MIN_TO_DATE
			) AS LAST_ROW_MIN_TO_DATE,
		array_sort(array(READ_TYPE_CODE_FROM, INSTALL_TYPE_CODE_FROM,INSTALL_DATE)) AS SORTED_FROM_DATE_ARRAY
	FROM JOINED_VIEW_Dup
	),
	-- Create TIMELINE_FROM column for each row by checking FROM_DATE of each row from the SORTED_FROM_DATE_ARRAY column
	-- which is greater than MIN_TO_DATE of previous row or LAST_ROW_MIN_TO_DATE column value of same row
	-- For start TIMELINE
METER_EFFECTIVE_TIME
AS (
	SELECT METER_EFFECTIVE_TIME_TEMP.*,
		CASE 
			WHEN element_at(SORTED_FROM_DATE_ARRAY, 1) > LAST_ROW_MIN_TO_DATE
				THEN element_at(SORTED_FROM_DATE_ARRAY, 1)
			WHEN element_at(SORTED_FROM_DATE_ARRAY, 2) > LAST_ROW_MIN_TO_DATE
				THEN element_at(SORTED_FROM_DATE_ARRAY, 2)
			WHEN element_at(SORTED_FROM_DATE_ARRAY, 3) > LAST_ROW_MIN_TO_DATE
				THEN element_at(SORTED_FROM_DATE_ARRAY, 3)
			--when element_at(SORTED_FROM_DATE_ARRAY,4) > LAST_ROW_MIN_TO_DATE then element_at(SORTED_FROM_DATE_ARRAY,4)
			ELSE element_at(SORTED_FROM_DATE_ARRAY, 1)
			END AS TIMELINE_FROM
	FROM METER_EFFECTIVE_TIME_TEMP
	),
-- final view with all the timelines which can be used for further logic
final_view
AS (
	SELECT DISTINCT NMI,
		TIMELINE_FROM,
		IF (
			MIN_TO_DATE > REMOVAL_DATE,
			REMOVAL_DATE,
			MIN_TO_DATE
			) AS TIMELINE_TO,
		NMI_FROM,
		NMI_TO,
		METER_SERIAL_NUMBER,
		INSTALL_DATE,
		REMOVAL_DATE,
		READ_TYPE_CODE,
		READ_TYPE_CODE_FROM,
		READ_TYPE_CODE_TO,
		INSTALL_TYPE_CODE,
		INSTALL_TYPE_CODE_FROM,
		INSTALL_TYPE_CODE_TO FROM METER_EFFECTIVE_TIME ),
        
--- The view is added to get the timelines till the meter is installed on a NMI
  NMI_INSTALLED( select NMI,
                        0 as METER_NUM, 
                        null as INSTALLATION_TYP, 
                        null as READ_TYP_CD,
                        to_date(NMI_FROM, 'yyyyMMdd') as FROM_DT,
                        date_add(to_date(MIN(INSTALL_DATE),'yyyyMMdd'),-1) as TO_DT,
                        date_format(from_utc_timestamp(current_timestamp(),'GMT+10'),'yyyy-MM-dd HH:mm:ss') as ROW_INSERT_DTM
                        FROM final_view
                       -- where NMI='6305859889'
                        group by 
                        NMI,
                        NMI_FROM),
  ---------------- final output query for required columns --------------                    
Final_combined_view as (
		SELECT NMI, 
        METER_SERIAL_NUMBER AS METER_NUM,
		IF (
			trim(INSTALL_TYPE_CODE) = '',
			NULL,
			trim(INSTALL_TYPE_CODE)
			) AS INSTALLATION_TYP,
		IF (
				trim(READ_TYPE_CODE) = '',
				NULL,
				trim(READ_TYPE_CODE)
				) AS READ_TYP_CD,
			to_date(TIMELINE_FROM, 'yyyyMMdd') AS FROM_DT,
			to_date(TIMELINE_TO, 'yyyyMMdd') AS TO_DT,
			date_format(from_utc_timestamp(current_timestamp(),'GMT+10'),'yyyy-MM-dd HH:mm:ss') as ROW_INSERT_DTM
			FROM final_view
            UNION
            SELECT * FROM NMI_INSTALLED
            UNION
            -- added this logic to get type 1-4 meters
            SELECT NMI,
            SERIAL_NUMBER as METER_NUM,
            NULL as INSTALLATION_TYP,
            NULL as READ_TYP_CD,
            INSTALLED_ON_DT AS FROM_DT, 
            REMOVED_ON_DT AS TO_DT,
            date_format(from_utc_timestamp(current_timestamp(),'GMT+10'),'yyyy-MM-dd HH:mm:ss') as ROW_INSERT_DTM 
            FROM METER_TYPE_1_4_V9), 
DUP_REMOVAL 
as (
select 
  fq.*, 
  ROW_NUMBER() OVER (PARTITION BY fq.NMI, fq.METER_NUM, fq.FROM_DT ORDER BY TO_DT DESC) as RNO
  from Final_combined_view fq
  ) 
SELECT 
  NMI,
  METER_NUM	,
  INSTALLATION_TYP	,
  READ_TYP_CD	,
  FROM_DT	,
  TO_DT	,
  ROW_INSERT_DTM	
FROM DUP_REMOVAL 
where length(NMI) > 10 or length(METER_NUM) > 14

-- COMMAND ----------

WITH ADRC_V1
AS (
	SELECT hub_address.HUB_ADDRESS_HSH_KEY,
		hub_address.ADDRNUMBER AS ADDRNUMBER,
		addr.*
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.SAT_ADDRESS_CIS_ADRC sat
		-- WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") < date_format('2021-09-20', "yyyyMMdd")
		) addr
	INNER JOIN imprd001_rawvault.HUB_ADDRESS hub_address
		ON hub_address.HUB_ADDRESS_HSH_KEY = addr.HUB_ADDRESS_HSH_KEY
	WHERE addr.rec_seq = 1
		AND addr.CHANGE_TYPE != 'D' -- get the latest record
	),
	--- Get the TPLNR and joined with IFLOT to get TPLMA and map the same with EVBS and get the NMI     
ILOA_V2
AS (
	SELECT hub_addres.HUB_ADDRESS_HSH_KEY,
		hub_nmi.HUB_NMI_HSH_KEY,
		hub_addres.ADDRNUMBER AS ADDRNUMBER,
		hub_nmi.NMI AS TPLNR,
		Iloa.*
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_ADDRESS_HSH_KEY,
				ILOAN ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_ADDRESS_CIS_ILOA sat
		-- WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") < date_format('2021-09-20', "yyyyMMdd")
		) Iloa
	INNER JOIN imprd001_rawvault.LNK_NMI_ADDRESS lnk
		ON Iloa.LNK_NMI_ADDRESS_HSH_KEY = lnk.LNK_NMI_ADDRESS_HSH_KEY
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi
		ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	INNER JOIN imprd001_rawvault.HUB_ADDRESS hub_addres
		ON lnk.HUB_ADDRESS_HSH_KEY = hub_addres.HUB_ADDRESS_HSH_KEY
	WHERE Iloa.rec_seq = 1
		AND Iloa.CHANGE_TYPE != 'D'
	),
	-- Get the IFLOT TPLMA and join the EVBS HAUS and get the NMI from EUITRANS     
IFLOT_V3
AS (
	SELECT hub_nmi.HUB_NMI_HSH_KEY,
		hub_nmi.NMI AS NMI,
		iflot.*
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY HUB_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.SAT_NMI_CIS_IFLOT sat
		-- WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") < date_format('2021-09-20', "yyyyMMdd")
		) iflot
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi
		ON hub_nmi.HUB_NMI_HSH_KEY = iflot.HUB_NMI_HSH_KEY
	WHERE iflot.rec_seq = 1
		AND iflot.CHANGE_TYPE != 'D'
	),
EVBS_V4
AS (
	SELECT DISTINCT hub_nmi3.HUB_NMI_HSH_KEY,
		hub_nmi1.NMI AS VSTELLE,
		hub_nmi2.NMI AS HAUS,
		hub_nmi3.NMI AS EXT_UI, -- EUITRANS NMI
		evbs.LGZUSATZ,
		evbs.ZZFLATNO,
		evbs.ZZFLATTYPE,
		evbs.ZZSTRSUFFIX
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY SALNK_NMI_SUBSTATION_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_SUBSTATION_CIS_EVBS sat
		-- WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") < date_format('2021-09-20', "yyyyMMdd")
		) evbs
	INNER JOIN imprd001_rawvault.SALNK_NMI_SUBSTATION salnk
		ON evbs.SALNK_NMI_SUBSTATION_HSH_KEY = salnk.SALNK_NMI_SUBSTATION_HSH_KEY
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi2
		ON salnk.HUB_NMI_HSH_KEY2 = hub_nmi2.HUB_NMI_HSH_KEY -- HAUS join
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi1
		ON salnk.HUB_NMI_HSH_KEY1 = hub_nmi1.HUB_NMI_HSH_KEY -- get the vstelle from salnk_nmi_substation
	INNER JOIN imprd001_rawvault.SALNK_NMI salnk_nmi_eanl
		ON salnk_nmi_eanl.HUB_NMI_HSH_KEY2 = salnk.HUB_NMI_HSH_KEY1 -- join the vstelle to salnk_nmi and get the Anlage
	INNER JOIN imprd001_rawvault.SALNK_NMI salnk_nmi_euiinstln
		ON salnk_nmi_eanl.HUB_NMI_HSH_KEY1 = salnk_nmi_euiinstln.HUB_NMI_HSH_KEY1 -- join the Anlage to get the INT_UI
	INNER JOIN imprd001_rawvault.SALNK_NMI salnk_nmi_euitrans
		ON salnk_nmi_euiinstln.HUB_NMI_HSH_KEY2 = salnk_nmi_euitrans.HUB_NMI_HSH_KEY2 -- join the INT_UI to get the EXT_UI
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi3
		ON salnk_nmi_euitrans.HUB_NMI_HSH_KEY1 = hub_nmi3.HUB_NMI_HSH_KEY -- join the EXT_UI hash key from salnk-nmi euitrans to get NMI
	WHERE salnk_nmi_euitrans.HUB_NMI_KEY1_DEF = 'EXT_UI'
		AND evbs.rec_seq = 1
		AND evbs.CHANGE_TYPE != 'D'
	)
,final_view(
           SELECT 
              IF(trim(evbs.EXT_UI)='',null,trim(evbs.EXT_UI)) as NMI,
              IF(trim(adrc.ADDRNUMBER)='',null,trim(adrc.ADDRNUMBER)) as ADDRESS_NUMBER,
              IF(trim(adrc.COUNTRY)='',null,trim(adrc.COUNTRY)) as COUNTRY_CODE,
              IF(trim(adrc.REGIOGROUP)='',null,trim(adrc.REGIOGROUP)) as REGION_GROUP_CODE,
              IF(trim(adrc.REGION)='',null,trim(adrc.REGION)) as REGION_CODE,
              IF(trim(adrc.CITY_CODE)='',null,trim(adrc.CITY_CODE)) as CITY_CODE,
              IF(trim(adrc.CITY1)='',null,trim(adrc.CITY1)) as CITY_NAME,
              IF(trim(adrc.POST_CODE1)='',null,trim(adrc.POST_CODE1)) as POSTAL_CODE,
              IF(trim(adrc.STREETCODE)='',null,trim(adrc.STREETCODE)) as STREET_CODE,
              IF(trim(adrc.STREET)='',null,trim(adrc.STREET)) as STREET_NAME,
              IF(trim(adrc.STR_SUPPL1)='',null,trim(adrc.STR_SUPPL1)) as STREET_EXTRA_DETAIL1,
              IF(trim(adrc.STR_SUPPL2)='',null,trim(adrc.STR_SUPPL2)) as STREET_EXTRA_DETAIL2,
              IF(trim(adrc.STR_SUPPL3)='',null,trim(adrc.STR_SUPPL3)) as STREET_EXTRA_DETAIL3,
              IF(trim(evbs.ZZSTRSUFFIX)='',null,trim(evbs.ZZSTRSUFFIX)) as STREET_SUFFIX_CODE,
              IF(trim(adrc.LOCATION)='',null,trim(adrc.LOCATION)) as LOCATION_DETAIL,
              IF(trim(evbs.LGZUSATZ)='',null,trim(evbs.LGZUSATZ)) as LOCATION_EXTRA_DETAIL,
              IF(trim(adrc.HOUSE_NUM1)='',null,trim(adrc.HOUSE_NUM1)) as HOUSE_NUMBER1,
              IF(trim(adrc.HOUSE_NUM2)='',null,trim(adrc.HOUSE_NUM2)) as HOUSE_NUMBER2,
              IF(trim(adrc.BUILDING)='',null,trim(adrc.BUILDING)) as BUILDING_NUMBER,
              IF(trim(evbs.ZZFLATNO)='',null,trim(evbs.ZZFLATNO)) as FLAT_NUMBER,
              IF(trim(evbs.ZZFLATTYPE)='',null,trim(evbs.ZZFLATTYPE)) as FLAT_TYPE_CODE
FROM ADRC_V1 AS adrc
INNER JOIN ILOA_V2 AS iloa
	ON adrc.ADDRNUMBER = iloa.ADDRNUMBER
INNER JOIN IFLOT_V3 AS iflot
	ON iloa.TPLNR = iflot.NMI
		AND iloa.ILOAN = iflot.ILOAN
INNER JOIN EVBS_V4 AS evbs
	ON iflot.TPLMA = evbs.HAUS )
SELECT DISTINCT NMI,
	STREET_EXTRA_DETAIL1 AS BUILD_PROP_NM,
	LOCATION_EXTRA_DETAIL AS LOCATION_DESC,
	HOUSE_NUMBER2 AS LOT_NUM,
	FLAT_NUMBER AS UNIT_NUM,
	FLAT_TYPE_CODE AS UNIT_TYP,
	HOUSE_NUMBER1 AS HOUSE_NUM,
	LOCATION_DETAIL AS STREET_NM,
	STREET_SUFFIX_CODE AS STREET_SUFFIX,
	STREET_EXTRA_DETAIL3 AS STREET_TYP,
	CITY_NAME AS SUBURB,
	REGION_CODE AS STATE,
	POSTAL_CODE AS POST_CD,
	date_format(from_utc_timestamp(current_timestamp(),'GMT+10'),'yyyy-MM-dd HH:mm:ss') as ROW_INSERT_DTM
FROM final_view
where NMI = '6305110926'

-- COMMAND ----------

select * from (
SELECT DISTINCT hub_nmi3.HUB_NMI_HSH_KEY,
		hub_nmi1.NMI AS VSTELLE,
		hub_nmi2.NMI AS HAUS,
		hub_nmi3.NMI AS EXT_UI, -- EUITRANS NMI
		evbs.LGZUSATZ,
		evbs.ZZFLATNO,
		evbs.ZZFLATTYPE,
		evbs.ZZSTRSUFFIX,
        ROW_NUMBER() OVER (
				PARTITION BY hub_nmi3.NMI order by evbs.extract_date_time DESC ) as RNO
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY SALNK_NMI_SUBSTATION_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_SUBSTATION_CIS_EVBS sat
		-- WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") < date_format('2021-09-20', "yyyyMMdd")
		) evbs
	INNER JOIN imprd001_rawvault.SALNK_NMI_SUBSTATION salnk
		ON evbs.SALNK_NMI_SUBSTATION_HSH_KEY = salnk.SALNK_NMI_SUBSTATION_HSH_KEY
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi2
		ON salnk.HUB_NMI_HSH_KEY2 = hub_nmi2.HUB_NMI_HSH_KEY -- HAUS join
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi1
		ON salnk.HUB_NMI_HSH_KEY1 = hub_nmi1.HUB_NMI_HSH_KEY -- get the vstelle from salnk_nmi_substation
	INNER JOIN imprd001_rawvault.SALNK_NMI salnk_nmi_eanl
		ON salnk_nmi_eanl.HUB_NMI_HSH_KEY2 = salnk.HUB_NMI_HSH_KEY1 -- join the vstelle to salnk_nmi and get the Anlage
	INNER JOIN imprd001_rawvault.SALNK_NMI salnk_nmi_euiinstln
		ON salnk_nmi_eanl.HUB_NMI_HSH_KEY1 = salnk_nmi_euiinstln.HUB_NMI_HSH_KEY1 -- join the Anlage to get the INT_UI
	INNER JOIN imprd001_rawvault.SALNK_NMI salnk_nmi_euitrans
		ON salnk_nmi_euiinstln.HUB_NMI_HSH_KEY2 = salnk_nmi_euitrans.HUB_NMI_HSH_KEY2 -- join the INT_UI to get the EXT_UI
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi3
		ON salnk_nmi_euitrans.HUB_NMI_HSH_KEY1 = hub_nmi3.HUB_NMI_HSH_KEY -- join the EXT_UI hash key from salnk-nmi euitrans to get NMI
	WHERE salnk_nmi_euitrans.HUB_NMI_KEY1_DEF = 'EXT_UI'
		AND evbs.rec_seq = 1
		AND evbs.CHANGE_TYPE != 'D'
        and hub_nmi3.NMI = '6305110926'
) where RNO = 1

-- COMMAND ----------


with 
-- Get NMI 
EUITRANS_V1 as(
     select hub_nmi.NMI
          , salnk.HUB_NMI_HSH_KEY2
       from (select  sat.*
                    ,ROW_NUMBER() OVER ( partition BY sat.SALNK_NMI_HSH_KEY, sat.DATEFROM ORDER BY sat.extract_date_time DESC ) AS `rec_seq`
               from imprd001_rawvault.LSAT_NMI_CIS_EUITRANS sat
              --FOR Each valid NMI there must be a check-sum value   
			  where length(trim(sat.NMI_CHECKSUM)) > 0
            )EUITRANS
       join imprd001_rawvault.SALNK_NMI salnk
         on EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
       join imprd001_rawvault.HUB_NMI hub_nmi
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
          from imprd001_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
       )EUIINSTLN
  join imprd001_rawvault.SALNK_NMI salnk
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
            from imprd001_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTS sat
         )EASTS
    join imprd001_rawvault.LNK_NMI_EQUIPMENT lnk
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
      ,salnk.*
      ,ETDZ.*
   from (select sat.*
              , ROW_NUMBER() OVER ( partition BY sat.SALNK_EQUIPMENT_HSH_KEY, sat.ZWNUMMER ORDER BY sat.EXTRACT_DATE_TIME DESC) as `rec_seq`
           from imprd001_rawvault.LSAT_EQUIPMENT_CIS_ETDZ sat
          where ( Upper(KENNZIFF) like 'E%'
               or Upper(KENNZIFF) like 'B%'
                )
            -- Register type - C is for Current    
            and ZWART = 'C'     
         )ETDZ
   join imprd001_rawvault.SALNK_EQUIPMENT salnk
     on ETDZ.SALNK_EQUIPMENT_HSH_KEY = salnk.SALNK_EQUIPMENT_HSH_KEY   
  where ETDZ.rec_seq = 1
  AND ETDZ.AB < ETDZ.BIS
)
SELECT DISTINCT EUITRANS_V1.NMI AS NMI,
	ETDZ_V4.KENNZIFF AS NMI_SUFFIX,
	min(ETDZ_V4.AB) OVER (
		PARTITION BY EUITRANS_V1.NMI,
		ETDZ_V4.KENNZIFF,
		ETDZ_V4.BIS ORDER BY ETDZ_V4.AB
		) AS FROM_DT,
	max(ETDZ_V4.BIS) OVER (
		PARTITION BY EUITRANS_V1.NMI,
		ETDZ_V4.KENNZIFF,
		ETDZ_V4.AB ORDER BY ETDZ_V4.AB
		) AS TO_DT,
	date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM,
    ETDZ_V4.*
FROM EUITRANS_V1
INNER JOIN EUIINSTLN_V2
	ON EUITRANS_V1.HUB_NMI_HSH_KEY2 = EUIINSTLN_V2.HUB_NMI_HSH_KEY2
INNER JOIN EASTS_V3
	ON EUIINSTLN_V2.HUB_NMI_HSH_KEY1 = EASTS_V3.HUB_NMI_HSH_KEY
INNER JOIN ETDZ_V4
	ON EASTS_V3.HUB_EQUIPMENT_HSH_KEY = ETDZ_V4.HUB_EQUIPMENT_HSH_KEY2 
  where EUITRANS_V1.NMI = '6305566873'

-- COMMAND ----------



-- COMMAND ----------

select ETDZ.KENNZIFF
      , ETDZ.AB
      , ETDZ.BIS
      , ETDZ.AEDAT
      , salnk.HUB_EQUIPMENT_HSH_KEY2
   from (select sat.*
              , ROW_NUMBER() OVER ( partition BY sat.SALNK_EQUIPMENT_HSH_KEY, sat.ZWNUMMER ORDER BY sat.EXTRACT_DATE_TIME DESC) as `rec_seq`
           from imprd001_rawvault.LSAT_EQUIPMENT_CIS_ETDZ sat
          where ( Upper(KENNZIFF) like 'E%'
               or Upper(KENNZIFF) like 'B%'
                )
            -- Register type - C is for Current    
            and ZWART = 'C'     
         )ETDZ
   join imprd001_rawvault.SALNK_EQUIPMENT salnk
     on ETDZ.SALNK_EQUIPMENT_HSH_KEY = salnk.SALNK_EQUIPMENT_HSH_KEY   
  where ETDZ.rec_seq = 1
  and salnk.HUB_EQUIPMENT_HSH_KEY2 = ''
