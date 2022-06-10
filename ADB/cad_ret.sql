-- Databricks notebook source
CREATE WIDGET TEXT logicalenv DEFAULT "prd001";
CREATE WIDGET TEXT run_date DEFAULT "2021-09-14";

-- COMMAND ----------

WITH ESERVICE_V1 
-- Get Eservice Details
AS(
    SELECT hub_nmi2.NMI AS NMI,
	hub_mark.SRVID AS SRVID,
	eserv.VERTRAG,
	eserv.SERVICE_START,
	eserv.SERVICE_END,
	eserv.EXTRACT_DATE_TIME AS ESERVICE_ROW_EFF_DTM
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_NMI_PARTICIPANT_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_PARTICIPANT_CIS_ESERVICE sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND SERVICE = 'FRMP'
	) eserv
INNER JOIN im${logicalenv}_rawvault.LNK_NMI_PARTICIPANT lnk
	ON eserv.LNK_NMI_PARTICIPANT_HSH_KEY = lnk.LNK_NMI_PARTICIPANT_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
	ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_PARTICIPANT hub_mark
	ON lnk.HUB_PARTICIPANT_HSH_KEY = hub_mark.HUB_PARTICIPANT_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.salnk_nmi sn
	ON hub_nmi.HUB_NMI_HSH_KEY = sn.HUB_NMI_HSH_KEY2
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi2
	ON sn.HUB_NMI_HSH_KEY1 = hub_nmi2.HUB_NMI_HSH_KEY
WHERE eserv.rec_seq = 1
	AND eserv.CHANGE_TYPE != 'D'
     ),
-- get the ANLAGE and INT_UI from Installation      
EUIINSTLN_V3 as(
    SELECT hub1.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY1 AS ANLAGE_HSH_KEY,
	hub2.nmi AS INT_UI,
	salnk.HUB_NMI_HSH_KEY2 AS INT_UI_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) EUIINSTLN
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
	ON EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUIINSTLN.rec_seq = 1
		AND EUIINSTLN.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub1
	ON salnk.HUB_NMI_HSH_KEY2 = hub1.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
     ), 
-- get the ANLAGE and VKONTO from EVER       
 EVER_V4 as(
SELECT hub2.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY2 AS ANLAGE_HSH_KEY,
	hub3.nmi AS VKONTO,
	salnk.HUB_NMI_HSH_KEY3 AS VKONTO_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI2_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EVER sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) ever
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI2 salnk
	ON ever.SALNK_NMI2_HSH_KEY = salnk.SALNK_NMI2_HSH_KEY
		AND ever.rec_seq = 1
		AND ever.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub3
	ON salnk.HUB_NMI_HSH_KEY3 = hub3.HUB_NMI_HSH_KEY
     ), 
-- get the PARTNER and VKONTO  and BPART is taken in output from here   
FKKVKP_V5 AS(
SELECT hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_nmi.HUB_NMI_HSH_KEY AS VKONT_HSH_KEY,
	hub_bpart.PARTNER AS PARTNER,
	hub_nmi.NMI AS VKONT
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_FKKVKP sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) fkkvkp
INNER JOIN im${logicalenv}_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
	ON fkkvkp.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
	ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_bpart
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE fkkvkp.rec_seq = 1
	AND fkkvkp.CHANGE_TYPE != 'D'
     ) ,
-- NAMES of a business partner
BUT000_V6 AS(
SELECT HUB_BUSINESS_PARTNER_HSH_KEY,
	HUB_NMI_HSH_KEY,
	PARTNER,
	NMI,
	NMI_PARTNER_FROM,
	nvl(NMI_PARTNER_TO, '99991231') AS NMI_PARTNER_TO,
	NAME_ORG1,
	NAME_ORG2,
	NAME_ORG3,
	NAME_ORG4,
	TYPE,
	LNK_NMI_BUSINESS_PARTNER_HSH_KEY
FROM (
	SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
		hub_nmi.HUB_NMI_HSH_KEY,
		hub_part.PARTNER AS PARTNER,
		hub_nmi.NMI AS NMI,
		regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS NMI_PARTNER_FROM,
		LEAD(regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY but000.EXTRACT_DATE_TIME
			) AS NMI_PARTNER_TO,
		but000.NAME_ORG1,
		but000.NAME_ORG2,
		but000.NAME_ORG3,
		but000.NAME_ORG4,
		but000.TYPE,
		but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
		but000.extract_date_time,
		but000.rec_seq
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
				DATE (sat.extract_date_time) ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_BUT000 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) but000
	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
		ON but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
		ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
		ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
	WHERE but000.rec_seq = 1
		AND but000.CHANGE_TYPE != 'D'
	) a
),
-- Address number, Valid_from,valid_to , partner      
BUT021_FS_V7 AS(
SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_address.HUB_ADDRESS_HSH_KEY,
	hub_part.PARTNER AS PARTNER,
	hub_address.ADDRNUMBER AS ADDRNUMBER,
	SUBSTRING(but021.VALID_FROM, 1, 8) AS VALID_FROM,
	SUBSTRING(but021.VALID_TO, 1, 8) AS VALID_TO,
	but021.ADR_KIND,
	but021.EXTRACT_DATE_TIME AS BUT021_FS_ROW_EFF_DTM
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND sat.ADR_KIND = 'XXDEFAULT'
	) but021
INNER JOIN im${logicalenv}_rawvault.LNK_BUSINESS_PARTNER_ADDRESS lnk
	ON but021.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY = lnk.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
	ON lnk.HUB_ADDRESS_HSH_KEY = hub_address.HUB_ADDRESS_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE but021.rec_seq = 1
	AND but021.CHANGE_TYPE != 'D'
     ),
 -- telphone numbre, personal number, Date_from , consumber
ADR2_V8 AS(
SELECT HUB_ADDRESS_HSH_KEY,
	ADDRNUMBER,
	TEL_FROM,
	nvl(TEL_TO, '99991231') AS TEL_TO,
	TEL_NUMBER1,
	TEL_NUMBER2,
	PERSNUMBER,
	DATE_FROM,
	CONSNUMBER
FROM (
	SELECT hub_address.HUB_ADDRESS_HSH_KEY,
		hub_address.ADDRNUMBER AS ADDRNUMBER,
		regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS TEL_FROM,
		LEAD(regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY addr.HUB_ADDRESS_HSH_KEY ORDER BY addr.EXTRACT_DATE_TIME
			) AS TEL_TO,
		addr.TEL_NUMBER1 AS TEL_NUMBER1,
		addr.TEL_NUMBER2 AS TEL_NUMBER2,
		addr.PERSNUMBER,
		addr.DATE_FROM,
		addr.CONSNUMBER
	FROM (
		SELECT sat.HUB_ADDRESS_HSH_KEY,
			sat.TEL_NUMBER AS TEL_NUMBER1,
			LEAD(sat.TEL_NUMBER) OVER (
				PARTITION BY sat.HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,
					CONSNUMBER DESC
				) AS TEL_NUMBER2,
			sat.DATE_FROM,
			sat.CONSNUMBER,
			sat.PERSNUMBER,
			sat.EXTRACT_DATE_TIME,
			sat.CHANGE_TYPE,
			ROW_NUMBER() OVER (
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,CONSNUMBER DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.SAT_ADDRESS_CIS_ADR2 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) addr
	INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
		ON hub_address.HUB_ADDRESS_HSH_KEY = addr.HUB_ADDRESS_HSH_KEY
	WHERE addr.rec_seq = 1
		AND addr.CHANGE_TYPE != 'D'
	) a
 ),
-- get NMI from EUITRANS and NMI_FROM and NMI_TO dates
EUITRANS_V9 as(
SELECT hub1_nmi.NMI,
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
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUITRANS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND length(trim(sat.NMI_CHECKSUM)) > 0
	) EUITRANS
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
	ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUITRANS.rec_seq = 1
		AND EUITRANS.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub1_nmi
	ON salnk.HUB_NMI_HSH_KEY1 = hub1_nmi.HUB_NMI_HSH_KEY
     ),
-- Timeline framing start --
date_lst1
AS (
	SELECT DISTINCT EUITRANS_V9.NMI AS NMI,
		stack(99, NMI_PARTNER_TO, DATETO, TEL_TO, VALID_TO, SERVICE_END, BUT000_V6.NMI_PARTNER_FROM, EUITRANS_V9.DATEFROM, ADR2_V8.TEL_FROM, BUTF.VALID_FROM, ESERVICE_V1.SERVICE_START) AS DT_LST
	FROM EUITRANS_V9
	LEFT JOIN ESERVICE_V1
		ON ESERVICE_V1.NMI = EUITRANS_V9.NMI 
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = EUITRANS_V9.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6
		ON EUITRANS_V9.NMI=BUT000_V6.NMI
	LEFT JOIN BUT021_FS_V7 BUTF
		ON BUT000_V6.HUB_BUSINESS_PARTNER_HSH_KEY = BUTF.HUB_BUSINESS_PARTNER_HSH_KEY
	LEFT JOIN ADR2_V8
		ON BUTF.HUB_ADDRESS_HSH_KEY = ADR2_V8.HUB_ADDRESS_HSH_KEY
	WHERE EUITRANS_V9.NMI IN (
			'6305000619',
			'6306049812',
            '6305000386'
			)
	),
date_lst2
AS (
	SELECT NMI,
		DT_LST AS FROM_DT,
		LEAD(DT_LST, 1) OVER (
			PARTITION BY NMI ORDER BY DT_LST
			) TO_DT
	FROM date_lst1
	),
date_lst3
AS (
	SELECT NMI,
		to_date(FROM_DT,"yyyyMMdd") as FROM_DT,
        case when lead(to_date(FROM_DT,"yyyyMMdd")) over (PARTITION BY NMI ORDER BY FROM_DT) = to_date(TO_DT,"yyyyMMdd") then date_sub(to_date(TO_DT,"yyyyMMdd"),1) else to_date(TO_DT,"yyyyMMdd") end as TO_DT
	FROM date_lst2
	WHERE datediff(to_date(TO_DT,"yyyyMMdd"),to_date(FROM_DT,"yyyyMMdd")) >1 
    AND to_date(TO_DT,"yyyyMMdd") >= to_date('20210918','yyyyMMdd') -- Since IM don't have history for address, cut off date based on 5MS go-live is defined to take only latest data(History will be migrated from ADW CAD)
	),
-- Timeline framing end --    
date_lst4 
AS (
	SELECT distinct S1.NMI AS NMI,
		CASE 
			WHEN dl.FROM_DT < to_date('20150101', 'yyyyMMdd') -- ADW has cutoff date as 20150101, so allign with it
				THEN to_date('20150101', 'yyyyMMdd')
			ELSE dl.FROM_DT
			END AS START_DT,
		DL.TO_DT AS END_DT,
		S3.NAME_ORG1 AS BUT000_NAME_ORG1,
		S3.NAME_ORG2 AS BUT000_NAME_ORG2,
		S3.NAME_ORG3 AS BUT000_NAME_ORG3,
		S3.NAME_ORG4 AS BUT000_NAME_ORG4,
		S5.TEL_NUMBER1 AS ADR2_TEL_NUMBER1,
		S5.TEL_NUMBER2 AS ADR2_TEL_NUMBER2,
		S2.SRVID AS ESERVICE_SERVICE_ID,
		S5.PERSNUMBER AS ADR2_PERSNUMBER,
		S5.DATE_FROM AS ADR2_DATE_FROM,
		S5.CONSNUMBER AS CONSNUMBER,
		date_format(to_date(S5.TEL_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS ADR2_ROW_EFF_DTM,
		S3.TYPE AS BUT000_TYPE,
		date_format(to_date(S3.NMI_PARTNER_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS BUT000_ROW_EFF_DTM,
		S3.PARTNER AS BUT021_FS_PARTNER,
		to_date(S4.VALID_FROM, 'yyyyMMdd') AS BUT021_FS_VALID_FROM,
		to_date(S4.VALID_TO, 'yyyyMMdd') AS BUT021_FS_VALID_TO,
		S4.ADR_KIND AS BUT021_FS_ADR_KIND,
		S4.ADDRNUMBER AS BUT021_FS_ADDRNUMBER,
		date_format(S4.BUT021_FS_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS BUT021_FS_ROW_EFF_DTM,
		S2.VERTRAG AS ESERVICE_VERTRAG,
		date_format(S2.ESERVICE_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS ESERVICE_ROW_EFF_DTM,
		to_date(S2.SERVICE_START, "yyyyMMdd") AS ESERVICE_SERVICE_START,
		to_date(S2.SERVICE_END, "yyyyMMdd") AS ESERVICE_SERVICE_END,
		date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
	FROM EUITRANS_V9 S1
	INNER JOIN date_lst3 dl
		ON (
				S1.NMI = dl.NMI
				AND to_date(S1.DATETO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S1.DATEFROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ESERVICE_V1 S2
		ON (
				S2.NMI = S1.NMI
				AND to_date(S2.SERVICE_END, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S2.SERVICE_START, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = S1.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6 S3
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = FKKVKP_V5.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S3.NMI_PARTNER_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S3.NMI_PARTNER_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN BUT021_FS_V7 S4
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = S4.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S4.VALID_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S4.VALID_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ADR2_V8 S5
		ON (
				S4.HUB_ADDRESS_HSH_KEY = S5.HUB_ADDRESS_HSH_KEY
				AND to_date(S5.TEL_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S5.TEL_FROM, "yyyyMMdd") < dl.TO_DT
				)     
	)
SELECT NMI, START_DT, count(9)
FROM date_lst4
group by NMI, START_DT
having count(9) >1

-- COMMAND ----------

WITH ESERVICE_V1 
-- Get Eservice Details
-- Observed duplicate records in server, so added rank condition based on date column to keep the latest versions
AS(
    select * from (
    SELECT hub_nmi2.NMI AS NMI,
	hub_mark.SRVID AS SRVID,
	eserv.VERTRAG,
	eserv.SERVICE_START,
	eserv.SERVICE_END,
	eserv.EXTRACT_DATE_TIME AS ESERVICE_ROW_EFF_DTM,
    ROW_NUMBER() OVER (
			PARTITION BY hub_nmi2.NMI ORDER BY eserv.EXTRACT_DATE_TIME DESC, eserv.SERVICE_START DESC
            ) AS RNO
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_NMI_PARTICIPANT_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_PARTICIPANT_CIS_ESERVICE sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND SERVICE = 'FRMP'
	) eserv
INNER JOIN im${logicalenv}_rawvault.LNK_NMI_PARTICIPANT lnk
	ON eserv.LNK_NMI_PARTICIPANT_HSH_KEY = lnk.LNK_NMI_PARTICIPANT_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
	ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_PARTICIPANT hub_mark
	ON lnk.HUB_PARTICIPANT_HSH_KEY = hub_mark.HUB_PARTICIPANT_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.salnk_nmi sn
	ON hub_nmi.HUB_NMI_HSH_KEY = sn.HUB_NMI_HSH_KEY2
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi2
	ON sn.HUB_NMI_HSH_KEY1 = hub_nmi2.HUB_NMI_HSH_KEY
WHERE eserv.rec_seq = 1
	AND eserv.CHANGE_TYPE != 'D'
     ) where RNO = 1 ),
-- get the ANLAGE and INT_UI from Installation      
EUIINSTLN_V3 as(
    SELECT hub1.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY1 AS ANLAGE_HSH_KEY,
	hub2.nmi AS INT_UI,
	salnk.HUB_NMI_HSH_KEY2 AS INT_UI_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) EUIINSTLN
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
	ON EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUIINSTLN.rec_seq = 1
		AND EUIINSTLN.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub1
	ON salnk.HUB_NMI_HSH_KEY2 = hub1.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
     ), 
-- get the ANLAGE and VKONTO from EVER       
 EVER_V4 as(
SELECT hub2.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY2 AS ANLAGE_HSH_KEY,
	hub3.nmi AS VKONTO,
	salnk.HUB_NMI_HSH_KEY3 AS VKONTO_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI2_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EVER sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) ever
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI2 salnk
	ON ever.SALNK_NMI2_HSH_KEY = salnk.SALNK_NMI2_HSH_KEY
		AND ever.rec_seq = 1
		AND ever.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub3
	ON salnk.HUB_NMI_HSH_KEY3 = hub3.HUB_NMI_HSH_KEY
     ), 
-- get the PARTNER and VKONTO  and BPART is taken in output from here   
FKKVKP_V5 AS(
SELECT hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_nmi.HUB_NMI_HSH_KEY AS VKONT_HSH_KEY,
	hub_bpart.PARTNER AS PARTNER,
	hub_nmi.NMI AS VKONT
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_FKKVKP sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) fkkvkp
INNER JOIN im${logicalenv}_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
	ON fkkvkp.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
	ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_bpart
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE fkkvkp.rec_seq = 1
	AND fkkvkp.CHANGE_TYPE != 'D'
     ) ,
-- NAMES of a business partner
BUT000_V6 AS(
SELECT HUB_BUSINESS_PARTNER_HSH_KEY,
	HUB_NMI_HSH_KEY,
	PARTNER,
	NMI,
	NMI_PARTNER_FROM,
	nvl(NMI_PARTNER_TO, '99991231') AS NMI_PARTNER_TO,
	NAME_ORG1,
	NAME_ORG2,
	NAME_ORG3,
	NAME_ORG4,
	TYPE,
	LNK_NMI_BUSINESS_PARTNER_HSH_KEY
FROM (
	SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
		hub_nmi.HUB_NMI_HSH_KEY,
		hub_part.PARTNER AS PARTNER,
		hub_nmi.NMI AS NMI,
		regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS NMI_PARTNER_FROM,
		LEAD(regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY but000.EXTRACT_DATE_TIME
			) AS NMI_PARTNER_TO,
		but000.NAME_ORG1,
		but000.NAME_ORG2,
		but000.NAME_ORG3,
		but000.NAME_ORG4,
		but000.TYPE,
		but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
		but000.extract_date_time,
		but000.rec_seq
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
				DATE (sat.extract_date_time) ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_BUT000 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) but000
	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
		ON but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
		ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
		ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
	WHERE but000.rec_seq = 1
		AND but000.CHANGE_TYPE != 'D'
	) a
),
-- Address number, Valid_from,valid_to , partner      
BUT021_FS_V7 AS(
SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_address.HUB_ADDRESS_HSH_KEY,
	hub_part.PARTNER AS PARTNER,
	hub_address.ADDRNUMBER AS ADDRNUMBER,
	SUBSTRING(but021.VALID_FROM, 1, 8) AS VALID_FROM,
	SUBSTRING(but021.VALID_TO, 1, 8) AS VALID_TO,
	but021.ADR_KIND,
	but021.EXTRACT_DATE_TIME AS BUT021_FS_ROW_EFF_DTM
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND sat.ADR_KIND = 'XXDEFAULT'
	) but021
INNER JOIN im${logicalenv}_rawvault.LNK_BUSINESS_PARTNER_ADDRESS lnk
	ON but021.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY = lnk.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
	ON lnk.HUB_ADDRESS_HSH_KEY = hub_address.HUB_ADDRESS_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE but021.rec_seq = 1
	AND but021.CHANGE_TYPE != 'D'
     ),
 -- telphone numbre, personal number, Date_from , consumber
ADR2_V8 AS(
SELECT HUB_ADDRESS_HSH_KEY,
	ADDRNUMBER,
	TEL_FROM,
	nvl(TEL_TO, '99991231') AS TEL_TO,
	TEL_NUMBER1,
	TEL_NUMBER2,
	PERSNUMBER,
	DATE_FROM,
	CONSNUMBER
FROM (
	SELECT hub_address.HUB_ADDRESS_HSH_KEY,
		hub_address.ADDRNUMBER AS ADDRNUMBER,
		regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS TEL_FROM,
		LEAD(regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY addr.HUB_ADDRESS_HSH_KEY ORDER BY addr.EXTRACT_DATE_TIME
			) AS TEL_TO,
		addr.TEL_NUMBER1 AS TEL_NUMBER1,
		addr.TEL_NUMBER2 AS TEL_NUMBER2,
		addr.PERSNUMBER,
		addr.DATE_FROM,
		addr.CONSNUMBER
	FROM (
		SELECT sat.HUB_ADDRESS_HSH_KEY,
			sat.TEL_NUMBER AS TEL_NUMBER1,
			LEAD(sat.TEL_NUMBER) OVER (
				PARTITION BY sat.HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,
					CONSNUMBER DESC
				) AS TEL_NUMBER2,
			sat.DATE_FROM,
			sat.CONSNUMBER,
			sat.PERSNUMBER,
			sat.EXTRACT_DATE_TIME,
			sat.CHANGE_TYPE,
			ROW_NUMBER() OVER (
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,CONSNUMBER DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.SAT_ADDRESS_CIS_ADR2 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) addr
	INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
		ON hub_address.HUB_ADDRESS_HSH_KEY = addr.HUB_ADDRESS_HSH_KEY
	WHERE addr.rec_seq = 1
		AND addr.CHANGE_TYPE != 'D'
	) a
 ),
-- get NMI from EUITRANS and NMI_FROM and NMI_TO dates
EUITRANS_V9 as(
SELECT hub1_nmi.NMI,
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
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUITRANS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND length(trim(sat.NMI_CHECKSUM)) > 0
	) EUITRANS
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
	ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUITRANS.rec_seq = 1
		AND EUITRANS.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub1_nmi
	ON salnk.HUB_NMI_HSH_KEY1 = hub1_nmi.HUB_NMI_HSH_KEY
     ),
-- Timeline framing start --
date_lst1
AS (
	SELECT DISTINCT EUITRANS_V9.NMI AS NMI,
		stack(99, NMI_PARTNER_TO, DATETO, TEL_TO, VALID_TO, SERVICE_END, BUT000_V6.NMI_PARTNER_FROM, EUITRANS_V9.DATEFROM, ADR2_V8.TEL_FROM, BUTF.VALID_FROM, ESERVICE_V1.SERVICE_START) AS DT_LST
	FROM EUITRANS_V9
	LEFT JOIN ESERVICE_V1
		ON ESERVICE_V1.NMI = EUITRANS_V9.NMI 
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = EUITRANS_V9.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6
		ON EUITRANS_V9.NMI=BUT000_V6.NMI
	LEFT JOIN BUT021_FS_V7 BUTF
		ON BUT000_V6.HUB_BUSINESS_PARTNER_HSH_KEY = BUTF.HUB_BUSINESS_PARTNER_HSH_KEY
	LEFT JOIN ADR2_V8
		ON BUTF.HUB_ADDRESS_HSH_KEY = ADR2_V8.HUB_ADDRESS_HSH_KEY
	/*WHERE EUITRANS_V9.NMI IN (
			'6305010165',
			'6306049812',
            '6305000386'
			)*/
	),
date_lst2
AS (
	SELECT NMI,
		DT_LST AS FROM_DT,
		LEAD(DT_LST, 1) OVER (
			PARTITION BY NMI ORDER BY DT_LST
			) TO_DT
	FROM date_lst1
	),
date_lst3
AS (
	SELECT NMI,
		to_date(FROM_DT,"yyyyMMdd") as FROM_DT,
        case when lead(to_date(FROM_DT,"yyyyMMdd")) over (PARTITION BY NMI ORDER BY FROM_DT) = to_date(TO_DT,"yyyyMMdd") then date_sub(to_date(TO_DT,"yyyyMMdd"),1) else to_date(TO_DT,"yyyyMMdd") end as TO_DT
	FROM date_lst2
	WHERE datediff(to_date(TO_DT,"yyyyMMdd"),to_date(FROM_DT,"yyyyMMdd")) >1 
    AND to_date(TO_DT,"yyyyMMdd") >= to_date('20210918','yyyyMMdd') -- Since IM don't have history for address, cut off date based on 5MS go-live is defined to take only latest data(History will be migrated from ADW CAD)
	),
-- Timeline framing end --    
date_lst4 
AS (
	SELECT distinct S1.NMI AS NMI,
		CASE 
			WHEN dl.FROM_DT < to_date('20150101', 'yyyyMMdd') -- ADW has cutoff date as 20150101, so allign with it
				THEN to_date('20150101', 'yyyyMMdd')
			ELSE dl.FROM_DT
			END AS START_DT,
		DL.TO_DT AS END_DT,
		S3.NAME_ORG1 AS BUT000_NAME_ORG1,
		S3.NAME_ORG2 AS BUT000_NAME_ORG2,
		S3.NAME_ORG3 AS BUT000_NAME_ORG3,
		S3.NAME_ORG4 AS BUT000_NAME_ORG4,
		S5.TEL_NUMBER1 AS ADR2_TEL_NUMBER1,
		S5.TEL_NUMBER2 AS ADR2_TEL_NUMBER2,
		S2.SRVID AS ESERVICE_SERVICE_ID,
		S5.PERSNUMBER AS ADR2_PERSNUMBER,
		S5.DATE_FROM AS ADR2_DATE_FROM,
		S5.CONSNUMBER AS CONSNUMBER,
		date_format(to_date(S5.TEL_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS ADR2_ROW_EFF_DTM,
		S3.TYPE AS BUT000_TYPE,
		date_format(to_date(S3.NMI_PARTNER_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS BUT000_ROW_EFF_DTM,
		S3.PARTNER AS BUT021_FS_PARTNER,
		to_date(S4.VALID_FROM, 'yyyyMMdd') AS BUT021_FS_VALID_FROM,
		to_date(S4.VALID_TO, 'yyyyMMdd') AS BUT021_FS_VALID_TO,
		S4.ADR_KIND AS BUT021_FS_ADR_KIND,
		S4.ADDRNUMBER AS BUT021_FS_ADDRNUMBER,
		date_format(S4.BUT021_FS_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS BUT021_FS_ROW_EFF_DTM,
		S2.VERTRAG AS ESERVICE_VERTRAG,
		date_format(S2.ESERVICE_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS ESERVICE_ROW_EFF_DTM,
		to_date(S2.SERVICE_START, "yyyyMMdd") AS ESERVICE_SERVICE_START,
		to_date(S2.SERVICE_END, "yyyyMMdd") AS ESERVICE_SERVICE_END,
		date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
	FROM EUITRANS_V9 S1
	INNER JOIN date_lst3 dl
		ON (
				S1.NMI = dl.NMI
				AND to_date(S1.DATETO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S1.DATEFROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ESERVICE_V1 S2
		ON (
				S2.NMI = S1.NMI
				AND to_date(S2.SERVICE_END, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S2.SERVICE_START, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = S1.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6 S3
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = FKKVKP_V5.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S3.NMI_PARTNER_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S3.NMI_PARTNER_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN BUT021_FS_V7 S4
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = S4.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S4.VALID_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S4.VALID_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ADR2_V8 S5
		ON (
				S4.HUB_ADDRESS_HSH_KEY = S5.HUB_ADDRESS_HSH_KEY
				AND to_date(S5.TEL_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S5.TEL_FROM, "yyyyMMdd") < dl.TO_DT
				)     
	)
SELECT  count(9)
FROM date_lst4

-- COMMAND ----------

WITH ESERVICE_V1 
-- Get Eservice Details
AS(
    select * from (
    SELECT hub_nmi2.NMI AS NMI,
	hub_mark.SRVID AS SRVID,
	eserv.VERTRAG,
	eserv.SERVICE_START,
	eserv.SERVICE_END,
	eserv.EXTRACT_DATE_TIME AS ESERVICE_ROW_EFF_DTM,
    ROW_NUMBER() OVER (
			PARTITION BY hub_nmi2.NMI ORDER BY eserv.EXTRACT_DATE_TIME DESC, eserv.SERVICE_START DESC
            ) AS RNO
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_NMI_PARTICIPANT_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_PARTICIPANT_CIS_ESERVICE sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND SERVICE = 'FRMP'
	) eserv
INNER JOIN im${logicalenv}_rawvault.LNK_NMI_PARTICIPANT lnk
	ON eserv.LNK_NMI_PARTICIPANT_HSH_KEY = lnk.LNK_NMI_PARTICIPANT_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
	ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_PARTICIPANT hub_mark
	ON lnk.HUB_PARTICIPANT_HSH_KEY = hub_mark.HUB_PARTICIPANT_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.salnk_nmi sn
	ON hub_nmi.HUB_NMI_HSH_KEY = sn.HUB_NMI_HSH_KEY2
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi2
	ON sn.HUB_NMI_HSH_KEY1 = hub_nmi2.HUB_NMI_HSH_KEY
WHERE eserv.rec_seq = 1
	AND eserv.CHANGE_TYPE != 'D'
     ) where RNO = 1 ),
-- get the ANLAGE and INT_UI from Installation      
EUIINSTLN_V3 as(
    SELECT hub1.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY1 AS ANLAGE_HSH_KEY,
	hub2.nmi AS INT_UI,
	salnk.HUB_NMI_HSH_KEY2 AS INT_UI_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) EUIINSTLN
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
	ON EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUIINSTLN.rec_seq = 1
		AND EUIINSTLN.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub1
	ON salnk.HUB_NMI_HSH_KEY2 = hub1.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
     ), 
-- get the ANLAGE and VKONTO from EVER       
 EVER_V4 as(
SELECT hub2.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY2 AS ANLAGE_HSH_KEY,
	hub3.nmi AS VKONTO,
	salnk.HUB_NMI_HSH_KEY3 AS VKONTO_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI2_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EVER sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) ever
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI2 salnk
	ON ever.SALNK_NMI2_HSH_KEY = salnk.SALNK_NMI2_HSH_KEY
		AND ever.rec_seq = 1
		AND ever.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub3
	ON salnk.HUB_NMI_HSH_KEY3 = hub3.HUB_NMI_HSH_KEY
     ), 
-- get the PARTNER and VKONTO  and BPART is taken in output from here   
FKKVKP_V5 AS(
SELECT hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_nmi.HUB_NMI_HSH_KEY AS VKONT_HSH_KEY,
	hub_bpart.PARTNER AS PARTNER,
	hub_nmi.NMI AS VKONT
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_FKKVKP sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) fkkvkp
INNER JOIN im${logicalenv}_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
	ON fkkvkp.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
	ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_bpart
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE fkkvkp.rec_seq = 1
	AND fkkvkp.CHANGE_TYPE != 'D'
     ) ,
-- NAMES of a business partner
BUT000_V6 AS(
SELECT HUB_BUSINESS_PARTNER_HSH_KEY,
	HUB_NMI_HSH_KEY,
	PARTNER,
	NMI,
	NMI_PARTNER_FROM,
	nvl(NMI_PARTNER_TO, '99991231') AS NMI_PARTNER_TO,
	NAME_ORG1,
	NAME_ORG2,
	NAME_ORG3,
	NAME_ORG4,
	TYPE,
	LNK_NMI_BUSINESS_PARTNER_HSH_KEY
FROM (
	SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
		hub_nmi.HUB_NMI_HSH_KEY,
		hub_part.PARTNER AS PARTNER,
		hub_nmi.NMI AS NMI,
		regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS NMI_PARTNER_FROM,
		LEAD(regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY but000.EXTRACT_DATE_TIME
			) AS NMI_PARTNER_TO,
		but000.NAME_ORG1,
		but000.NAME_ORG2,
		but000.NAME_ORG3,
		but000.NAME_ORG4,
		but000.TYPE,
		but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
		but000.extract_date_time,
		but000.rec_seq
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
				DATE (sat.extract_date_time) ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_BUT000 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) but000
	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
		ON but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
		ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
		ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
	WHERE but000.rec_seq = 1
		AND but000.CHANGE_TYPE != 'D'
	) a
),
-- Address number, Valid_from,valid_to , partner      
BUT021_FS_V7 AS(
SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_address.HUB_ADDRESS_HSH_KEY,
	hub_part.PARTNER AS PARTNER,
	hub_address.ADDRNUMBER AS ADDRNUMBER,
	SUBSTRING(but021.VALID_FROM, 1, 8) AS VALID_FROM,
	SUBSTRING(but021.VALID_TO, 1, 8) AS VALID_TO,
	but021.ADR_KIND,
	but021.EXTRACT_DATE_TIME AS BUT021_FS_ROW_EFF_DTM
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND sat.ADR_KIND = 'XXDEFAULT'
	) but021
INNER JOIN im${logicalenv}_rawvault.LNK_BUSINESS_PARTNER_ADDRESS lnk
	ON but021.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY = lnk.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
	ON lnk.HUB_ADDRESS_HSH_KEY = hub_address.HUB_ADDRESS_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE but021.rec_seq = 1
	AND but021.CHANGE_TYPE != 'D'
     ),
 -- telphone numbre, personal number, Date_from , consumber
ADR2_V8 AS(
SELECT HUB_ADDRESS_HSH_KEY,
	ADDRNUMBER,
	TEL_FROM,
	nvl(TEL_TO, '99991231') AS TEL_TO,
	TEL_NUMBER1,
	TEL_NUMBER2,
	PERSNUMBER,
	DATE_FROM,
	CONSNUMBER
FROM (
	SELECT hub_address.HUB_ADDRESS_HSH_KEY,
		hub_address.ADDRNUMBER AS ADDRNUMBER,
		regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS TEL_FROM,
		LEAD(regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY addr.HUB_ADDRESS_HSH_KEY ORDER BY addr.EXTRACT_DATE_TIME
			) AS TEL_TO,
		addr.TEL_NUMBER1 AS TEL_NUMBER1,
		addr.TEL_NUMBER2 AS TEL_NUMBER2,
		addr.PERSNUMBER,
		addr.DATE_FROM,
		addr.CONSNUMBER
	FROM (
		SELECT sat.HUB_ADDRESS_HSH_KEY,
			sat.TEL_NUMBER AS TEL_NUMBER1,
			LEAD(sat.TEL_NUMBER) OVER (
				PARTITION BY sat.HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,
					CONSNUMBER DESC
				) AS TEL_NUMBER2,
			sat.DATE_FROM,
			sat.CONSNUMBER,
			sat.PERSNUMBER,
			sat.EXTRACT_DATE_TIME,
			sat.CHANGE_TYPE,
			ROW_NUMBER() OVER (
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,CONSNUMBER DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.SAT_ADDRESS_CIS_ADR2 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) addr
	INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
		ON hub_address.HUB_ADDRESS_HSH_KEY = addr.HUB_ADDRESS_HSH_KEY
	WHERE addr.rec_seq = 1
		AND addr.CHANGE_TYPE != 'D'
	) a
 ),
-- get NMI from EUITRANS and NMI_FROM and NMI_TO dates
EUITRANS_V9 as(
SELECT hub1_nmi.NMI,
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
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUITRANS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND length(trim(sat.NMI_CHECKSUM)) > 0
	) EUITRANS
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
	ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUITRANS.rec_seq = 1
		AND EUITRANS.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub1_nmi
	ON salnk.HUB_NMI_HSH_KEY1 = hub1_nmi.HUB_NMI_HSH_KEY
     ),
-- Timeline framing start --
date_lst1
AS (
	SELECT DISTINCT EUITRANS_V9.NMI AS NMI,
		stack(99, NMI_PARTNER_TO, DATETO, TEL_TO, VALID_TO, SERVICE_END, BUT000_V6.NMI_PARTNER_FROM, EUITRANS_V9.DATEFROM, ADR2_V8.TEL_FROM, BUTF.VALID_FROM, ESERVICE_V1.SERVICE_START) AS DT_LST
	FROM EUITRANS_V9
	LEFT JOIN ESERVICE_V1
		ON ESERVICE_V1.NMI = EUITRANS_V9.NMI 
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = EUITRANS_V9.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6
		ON EUITRANS_V9.NMI=BUT000_V6.NMI
	LEFT JOIN BUT021_FS_V7 BUTF
		ON BUT000_V6.HUB_BUSINESS_PARTNER_HSH_KEY = BUTF.HUB_BUSINESS_PARTNER_HSH_KEY
	LEFT JOIN ADR2_V8
		ON BUTF.HUB_ADDRESS_HSH_KEY = ADR2_V8.HUB_ADDRESS_HSH_KEY
	WHERE EUITRANS_V9.NMI IN (
			'6305000619',
			'6305741701',
            '6305874662'
			)
	),
date_lst2
AS (
	SELECT NMI,
		DT_LST AS FROM_DT,
		LEAD(DT_LST, 1) OVER (
			PARTITION BY NMI ORDER BY DT_LST
			) TO_DT
	FROM date_lst1
	),
date_lst3
AS (
	SELECT NMI,
		to_date(FROM_DT,"yyyyMMdd") as FROM_DT,
        case when lead(to_date(FROM_DT,"yyyyMMdd")) over (PARTITION BY NMI ORDER BY FROM_DT) = to_date(TO_DT,"yyyyMMdd") then date_sub(to_date(TO_DT,"yyyyMMdd"),1) else to_date(TO_DT,"yyyyMMdd") end as TO_DT
	FROM date_lst2
	WHERE datediff(to_date(TO_DT,"yyyyMMdd"),to_date(FROM_DT,"yyyyMMdd")) >1 
    AND to_date(TO_DT,"yyyyMMdd") >= to_date('20210918','yyyyMMdd') -- Since IM don't have history for address, cut off date based on 5MS go-live is defined to take only latest data(History will be migrated from ADW CAD)
	),
-- Timeline framing end --    
date_lst4 
AS (
	SELECT distinct S1.NMI AS NMI,
		CASE 
			WHEN dl.FROM_DT < to_date('20150101', 'yyyyMMdd') -- ADW has cutoff date as 20150101, so allign with it
				THEN to_date('20150101', 'yyyyMMdd')
			ELSE dl.FROM_DT
			END AS START_DT,
		DL.TO_DT AS END_DT,
		S3.NAME_ORG1 AS BUT000_NAME_ORG1,
		S3.NAME_ORG2 AS BUT000_NAME_ORG2,
		S3.NAME_ORG3 AS BUT000_NAME_ORG3,
		S3.NAME_ORG4 AS BUT000_NAME_ORG4,
		S5.TEL_NUMBER1 AS ADR2_TEL_NUMBER1,
		S5.TEL_NUMBER2 AS ADR2_TEL_NUMBER2,
		S2.SRVID AS ESERVICE_SERVICE_ID,
		S5.PERSNUMBER AS ADR2_PERSNUMBER,
		S5.DATE_FROM AS ADR2_DATE_FROM,
		S5.CONSNUMBER AS CONSNUMBER,
		date_format(to_date(S5.TEL_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS ADR2_ROW_EFF_DTM,
		S3.TYPE AS BUT000_TYPE,
		date_format(to_date(S3.NMI_PARTNER_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS BUT000_ROW_EFF_DTM,
		S3.PARTNER AS BUT021_FS_PARTNER,
		to_date(S4.VALID_FROM, 'yyyyMMdd') AS BUT021_FS_VALID_FROM,
		to_date(S4.VALID_TO, 'yyyyMMdd') AS BUT021_FS_VALID_TO,
		S4.ADR_KIND AS BUT021_FS_ADR_KIND,
		S4.ADDRNUMBER AS BUT021_FS_ADDRNUMBER,
		date_format(S4.BUT021_FS_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS BUT021_FS_ROW_EFF_DTM,
		S2.VERTRAG AS ESERVICE_VERTRAG,
		date_format(S2.ESERVICE_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS ESERVICE_ROW_EFF_DTM,
		to_date(S2.SERVICE_START, "yyyyMMdd") AS ESERVICE_SERVICE_START,
		to_date(S2.SERVICE_END, "yyyyMMdd") AS ESERVICE_SERVICE_END,
		date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
	FROM EUITRANS_V9 S1
	INNER JOIN date_lst3 dl
		ON (
				S1.NMI = dl.NMI
				AND to_date(S1.DATETO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S1.DATEFROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ESERVICE_V1 S2
		ON (
				S2.NMI = S1.NMI
				AND to_date(S2.SERVICE_END, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S2.SERVICE_START, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = S1.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6 S3
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = FKKVKP_V5.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S3.NMI_PARTNER_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S3.NMI_PARTNER_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN BUT021_FS_V7 S4
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = S4.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S4.VALID_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S4.VALID_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ADR2_V8 S5
		ON (
				S4.HUB_ADDRESS_HSH_KEY = S5.HUB_ADDRESS_HSH_KEY
				AND to_date(S5.TEL_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S5.TEL_FROM, "yyyyMMdd") < dl.TO_DT
				)     
	)
SELECT *
FROM date_lst4

-- COMMAND ----------



-- COMMAND ----------

WITH ESERVICE_V0
AS (
	SELECT *
	FROM (
		SELECT ESERVICE.*,
			lnk.HUB_NMI_HSH_KEY,
			lnk.HUB_PARTICIPANT_HSH_KEY,
			hubnmi.NMI AS INT_UI,
			hubpart.SRVID,
			ROW_NUMBER() OVER (
				PARTITION BY lnk.HUB_NMI_HSH_KEY,
				VERTRAG ORDER BY ESERVICE.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_PARTICIPANT_CIS_ESERVICE ESERVICE
		INNER JOIN imprd001_rawvault.LNK_NMI_PARTICIPANT lnk
			ON ESERVICE.LNK_NMI_PARTICIPANT_HSH_KEY = lnk.LNK_NMI_PARTICIPANT_HSH_KEY
		INNER JOIN imprd001_rawvault.HUB_NMI hubnmi
			ON lnk.HUB_NMI_HSH_KEY = hubnmi.HUB_NMI_HSH_KEY
		INNER JOIN imprd001_rawvault.HUB_PARTICIPANT hubpart
			ON lnk.HUB_PARTICIPANT_HSH_KEY = hubpart.HUB_PARTICIPANT_HSH_KEY
		WHERE SERVICE = 'FRMP'
			AND date_format(ESERVICE.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
		) ESERVICE
	WHERE ESERVICE.rec_seq = 1
		AND ESERVICE.CHANGE_TYPE != 'D'
	),
EUITRANS_V1
AS (
	SELECT hub.NMI,
		salnk.HUB_NMI_HSH_KEY1,
		salnk.HUB_NMI_HSH_KEY2
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.SALNK_NMI_HSH_KEY,
				sat.DATEFROM,
				sat.TIMEFROM ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_CIS_EUITRANS sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
			AND length(trim(sat.NMI_CHECKSUM)) > 0
		) EUITRANS
	INNER JOIN imprd001_rawvault.SALNK_NMI salnk
		ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
	INNER JOIN imprd001_rawvault.HUB_NMI hub
		ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
	WHERE EUITRANS.rec_seq = 1
		AND EUITRANS.CHANGE_TYPE <> 'D'
		AND salnk.HUB_NMI_KEY1_DEF = 'EXT_UI'
		AND salnk.HUB_NMI_KEY2_DEF = 'INT_UI'
	),
ESERVICE_V1
AS (
	SELECT *
	FROM (
		SELECT EUITRANS.NMI AS NMI,
			ESERVICE.SRVID AS SRVID,
			ESERVICE.VERTRAG,
			ESERVICE.SERVICE_START,
			ESERVICE.SERVICE_END,
			ESERVICE.EXTRACT_DATE_TIME AS ESERVICE_ROW_EFF_DTM,
            ROW_NUMBER() OVER (
				PARTITION BY EUITRANS.NMI, ESERVICE.SERVICE_END ORDER BY ESERVICE.EXTRACT_DATE_TIME DESC,ESERVICE.SERVICE_START DESC
				) AS RNO
		FROM ESERVICE_V0 AS ESERVICE
		INNER JOIN EUITRANS_V1 AS EUITRANS
			ON ESERVICE.HUB_NMI_HSH_KEY = EUITRANS.HUB_NMI_HSH_KEY2
		)
	WHERE RNO = 1 -- Observed duplicate records in non-prod, so added rank condition based on date column to keep the latest versions
	) ,
-- get the ANLAGE and INT_UI from Installation      
EUIINSTLN_V3 as(
    SELECT hub1.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY1 AS ANLAGE_HSH_KEY,
	hub2.nmi AS INT_UI,
	salnk.HUB_NMI_HSH_KEY2 AS INT_UI_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM imprd001_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
	) EUIINSTLN
INNER JOIN imprd001_rawvault.SALNK_NMI salnk
	ON EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUIINSTLN.rec_seq = 1
		AND EUIINSTLN.CHANGE_TYPE <> 'D'
INNER JOIN imprd001_rawvault.HUB_NMI hub1
	ON salnk.HUB_NMI_HSH_KEY2 = hub1.HUB_NMI_HSH_KEY
INNER JOIN imprd001_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
     ), 
-- get the ANLAGE and VKONTO from EVER       
 EVER_V4 as(
SELECT hub2.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY2 AS ANLAGE_HSH_KEY,
	hub3.nmi AS VKONTO,
	salnk.HUB_NMI_HSH_KEY3 AS VKONTO_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI2_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM imprd001_rawvault.LSAT_NMI_CIS_EVER sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
	) ever
INNER JOIN imprd001_rawvault.SALNK_NMI2 salnk
	ON ever.SALNK_NMI2_HSH_KEY = salnk.SALNK_NMI2_HSH_KEY
		AND ever.rec_seq = 1
		AND ever.CHANGE_TYPE <> 'D'
INNER JOIN imprd001_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
INNER JOIN imprd001_rawvault.HUB_NMI hub3
	ON salnk.HUB_NMI_HSH_KEY3 = hub3.HUB_NMI_HSH_KEY
     ), 
-- get the PARTNER and VKONTO  and BPART is taken in output from here   
FKKVKP_V5 AS(
SELECT hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_nmi.HUB_NMI_HSH_KEY AS VKONT_HSH_KEY,
	hub_bpart.PARTNER AS PARTNER,
	hub_nmi.NMI AS VKONT
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM imprd001_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_FKKVKP sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
	) fkkvkp
INNER JOIN imprd001_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
	ON fkkvkp.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi
	ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
INNER JOIN imprd001_rawvault.HUB_BUSINESS_PARTNER hub_bpart
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE fkkvkp.rec_seq = 1
	AND fkkvkp.CHANGE_TYPE != 'D'
     ) ,
-- NAMES of a business partner
BUT000_V6 AS(
SELECT HUB_BUSINESS_PARTNER_HSH_KEY,
	HUB_NMI_HSH_KEY,
	PARTNER,
	NMI,
	NMI_PARTNER_FROM,
	nvl(NMI_PARTNER_TO, '99991231') AS NMI_PARTNER_TO,
	NAME_ORG1,
	NAME_ORG2,
	NAME_ORG3,
	NAME_ORG4,
	TYPE,
	LNK_NMI_BUSINESS_PARTNER_HSH_KEY
FROM (
	SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
		hub_nmi.HUB_NMI_HSH_KEY,
		hub_part.PARTNER AS PARTNER,
		hub_nmi.NMI AS NMI,
		regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS NMI_PARTNER_FROM,
		LEAD(regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY but000.EXTRACT_DATE_TIME
			) AS NMI_PARTNER_TO,
		but000.NAME_ORG1,
		but000.NAME_ORG2,
		but000.NAME_ORG3,
		but000.NAME_ORG4,
		but000.TYPE,
		but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
		but000.extract_date_time,
		but000.rec_seq
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
				DATE (sat.extract_date_time) ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_BUT000 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
		) but000
	INNER JOIN imprd001_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
		ON but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
	INNER JOIN imprd001_rawvault.HUB_NMI hub_nmi
		ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	INNER JOIN imprd001_rawvault.HUB_BUSINESS_PARTNER hub_part
		ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
	WHERE but000.rec_seq = 1
		AND but000.CHANGE_TYPE != 'D'
	) a
),
-- Address number, Valid_from,valid_to , partner      
BUT021_FS_V7 AS(
SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_address.HUB_ADDRESS_HSH_KEY,
	hub_part.PARTNER AS PARTNER,
	hub_address.ADDRNUMBER AS ADDRNUMBER,
	SUBSTRING(but021.VALID_FROM, 1, 8) AS VALID_FROM,
	SUBSTRING(but021.VALID_TO, 1, 8) AS VALID_TO,
	but021.ADR_KIND,
	but021.EXTRACT_DATE_TIME AS BUT021_FS_ROW_EFF_DTM
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM imprd001_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
		AND sat.ADR_KIND = 'XXDEFAULT'
	) but021
INNER JOIN imprd001_rawvault.LNK_BUSINESS_PARTNER_ADDRESS lnk
	ON but021.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY = lnk.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY
INNER JOIN imprd001_rawvault.HUB_ADDRESS hub_address
	ON lnk.HUB_ADDRESS_HSH_KEY = hub_address.HUB_ADDRESS_HSH_KEY
INNER JOIN imprd001_rawvault.HUB_BUSINESS_PARTNER hub_part
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE but021.rec_seq = 1
	AND but021.CHANGE_TYPE != 'D'
     ),
 -- telphone numbre, personal number, Date_from , consumber
ADR2_V8 AS(
SELECT HUB_ADDRESS_HSH_KEY,
	ADDRNUMBER,
	TEL_FROM,
	nvl(TEL_TO, '99991231') AS TEL_TO,
	TEL_NUMBER1,
	TEL_NUMBER2,
	PERSNUMBER,
	DATE_FROM,
	CONSNUMBER
FROM (
	SELECT hub_address.HUB_ADDRESS_HSH_KEY,
		hub_address.ADDRNUMBER AS ADDRNUMBER,
		regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS TEL_FROM,
		LEAD(regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY addr.HUB_ADDRESS_HSH_KEY ORDER BY addr.EXTRACT_DATE_TIME
			) AS TEL_TO,
		addr.TEL_NUMBER1 AS TEL_NUMBER1,
		addr.TEL_NUMBER2 AS TEL_NUMBER2,
		addr.PERSNUMBER,
		addr.DATE_FROM,
		addr.CONSNUMBER
	FROM (
		SELECT sat.HUB_ADDRESS_HSH_KEY,
			sat.TEL_NUMBER AS TEL_NUMBER1,
			LEAD(sat.TEL_NUMBER) OVER (
				PARTITION BY sat.HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,
					CONSNUMBER DESC
				) AS TEL_NUMBER2,
			sat.DATE_FROM,
			sat.CONSNUMBER,
			sat.PERSNUMBER,
			sat.EXTRACT_DATE_TIME,
			sat.CHANGE_TYPE,
			ROW_NUMBER() OVER (
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,CONSNUMBER DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.SAT_ADDRESS_CIS_ADR2 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
		) addr
	INNER JOIN imprd001_rawvault.HUB_ADDRESS hub_address
		ON hub_address.HUB_ADDRESS_HSH_KEY = addr.HUB_ADDRESS_HSH_KEY
	WHERE addr.rec_seq = 1
		AND addr.CHANGE_TYPE != 'D'
	) a
 ),
-- get NMI from EUITRANS and NMI_FROM and NMI_TO dates
EUITRANS_V9 as(
SELECT hub1_nmi.NMI,
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
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('2021-09-20', "yyyyMMdd")
		AND length(trim(sat.NMI_CHECKSUM)) > 0
	) EUITRANS
INNER JOIN imprd001_rawvault.SALNK_NMI salnk
	ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUITRANS.rec_seq = 1
		AND EUITRANS.CHANGE_TYPE <> 'D'
INNER JOIN imprd001_rawvault.HUB_NMI hub1_nmi
	ON salnk.HUB_NMI_HSH_KEY1 = hub1_nmi.HUB_NMI_HSH_KEY
     ),
-- Timeline framing start --
date_lst1
AS (
	SELECT DISTINCT EUITRANS_V9.NMI AS NMI,
    NMI_PARTNER_TO, DATETO, TEL_TO, VALID_TO, SERVICE_END, BUT000_V6.NMI_PARTNER_FROM, EUITRANS_V9.DATEFROM, ADR2_V8.TEL_FROM, BUTF.VALID_FROM, ESERVICE_V1.SERVICE_START,
		stack(999, NMI_PARTNER_TO, DATETO, TEL_TO, VALID_TO, SERVICE_END, BUT000_V6.NMI_PARTNER_FROM, EUITRANS_V9.DATEFROM, ADR2_V8.TEL_FROM, BUTF.VALID_FROM, ESERVICE_V1.SERVICE_START) AS DT_LST
	FROM EUITRANS_V9
	LEFT JOIN ESERVICE_V1
		ON ESERVICE_V1.NMI = EUITRANS_V9.NMI 
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = EUITRANS_V9.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6 BUT000_V6
		ON BUT000_V6.HUB_BUSINESS_PARTNER_HSH_KEY = FKKVKP_V5.HUB_BUSINESS_PARTNER_HSH_KEY
	LEFT JOIN BUT021_FS_V7 BUTF
		ON BUT000_V6.HUB_BUSINESS_PARTNER_HSH_KEY = BUTF.HUB_BUSINESS_PARTNER_HSH_KEY
	LEFT JOIN ADR2_V8
		ON BUTF.HUB_ADDRESS_HSH_KEY = ADR2_V8.HUB_ADDRESS_HSH_KEY
	WHERE EUITRANS_V9.NMI IN (
			'6306056189'
			)
	)
    
  select * from date_lst1  
    /*
    
    
    ,
date_lst2
AS ( -- Frame FROM and TO Date
	SELECT NMI,
		DT_LST AS FROM_DT,
		LEAD(DT_LST, 1) OVER (
			PARTITION BY NMI ORDER BY DT_LST
			) TO_DT
	FROM date_lst1
	),
date_lst3
AS ( -- Exclude Nulls and invalid time slice
	SELECT NMI,
		to_date(FROM_DT,"yyyyMMdd") as FROM_DT,
        case when lead(to_date(FROM_DT,"yyyyMMdd")) over (PARTITION BY NMI ORDER BY FROM_DT) = to_date(TO_DT,"yyyyMMdd") then date_sub(to_date(TO_DT,"yyyyMMdd"),1) else to_date(TO_DT,"yyyyMMdd") end as TO_DT
	FROM date_lst2
	WHERE datediff(to_date(TO_DT,"yyyyMMdd"),to_date(FROM_DT,"yyyyMMdd")) >1 
    AND to_date(TO_DT,"yyyyMMdd") >= to_date('20210918','yyyyMMdd') -- Since IM don't have history for address, cut off date based on 5MS go-live is defined to take only latest data(History will be migrated from ADW CAD)
	),
-- Timeline framing end --    
date_lst4 
AS ( -- Join tables based on Key and Dates
	SELECT DISTINCT S1.NMI AS NMI,
		/*CASE 
			WHEN dl.FROM_DT < to_date('20150101', 'yyyyMMdd') -- ADW has cutoff date as 20150101, so allign with it
				THEN to_date('20150101', 'yyyyMMdd')
			ELSE dl.FROM_DT
			END AS START_DT,*/
        dl.FROM_DT AS START_DT,
		DL.TO_DT AS END_DT,
		S3.NAME_ORG1 AS BUT000_NAME_ORG1,
		S3.NAME_ORG2 AS BUT000_NAME_ORG2,
		S3.NAME_ORG3 AS BUT000_NAME_ORG3,
		S3.NAME_ORG4 AS BUT000_NAME_ORG4,
		S5.TEL_NUMBER1 AS ADR2_TEL_NUMBER1,
		S5.TEL_NUMBER2 AS ADR2_TEL_NUMBER2,
		S2.SRVID AS ESERVICE_SERVICE_ID,
		S5.PERSNUMBER AS ADR2_PERSNUMBER,
		S5.DATE_FROM AS ADR2_DATE_FROM,
		S5.CONSNUMBER AS ADR2_CONSNUMBER,
		date_format(to_date(S5.TEL_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS ADR2_ROW_EFF_DTM,
		S3.TYPE AS BUT000_TYPE,
		date_format(to_date(S3.NMI_PARTNER_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS BUT000_ROW_EFF_DTM,
		S3.PARTNER AS BUT021_FS_PARTNER,
		to_date(S4.VALID_FROM, 'yyyyMMdd') AS BUT021_FS_VALID_FROM,
		to_date(S4.VALID_TO, 'yyyyMMdd') AS BUT021_FS_VALID_TO,
		S4.ADR_KIND AS BUT021_FS_ADR_KIND,
		S4.ADDRNUMBER AS BUT021_FS_ADDRNUMBER,
		date_format(S4.BUT021_FS_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS BUT021_FS_ROW_EFF_DTM,
		S2.VERTRAG AS ESERVICE_VERTRAG,
		date_format(S2.ESERVICE_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS ESERVICE_ROW_EFF_DTM,
		to_date(S2.SERVICE_START, "yyyyMMdd") AS ESERVICE_SERVICE_START,
		to_date(S2.SERVICE_END, "yyyyMMdd") AS ESERVICE_SERVICE_END,
		date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
	FROM EUITRANS_V9 S1
	INNER JOIN date_lst3 dl
		ON (
				S1.NMI = dl.NMI
				AND to_date(S1.DATETO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S1.DATEFROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ESERVICE_V1 S2
		ON (
				S2.NMI = S1.NMI
				AND to_date(S2.SERVICE_END, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S2.SERVICE_START, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = S1.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6 S3
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = FKKVKP_V5.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S3.NMI_PARTNER_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S3.NMI_PARTNER_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN BUT021_FS_V7 S4
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = S4.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S4.VALID_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S4.VALID_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ADR2_V8 S5
		ON (
				S4.HUB_ADDRESS_HSH_KEY = S5.HUB_ADDRESS_HSH_KEY
				AND to_date(S5.TEL_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S5.TEL_FROM, "yyyyMMdd") < dl.TO_DT
				)     
	)
SELECT *
FROM date_lst4
where ESERVICE_VERTRAG is NOT NULL
AND START_DT >=to_date('20150101', 'yyyyMMdd')*/

-- COMMAND ----------



-- COMMAND ----------

-- =============================================
-- Author:      Sainath
-- Create Date: 19/08/2021
-- Description: Extracts Customer time slice data for MHE
-- =============================================

WITH ESERVICE_V0
AS (
	SELECT *
	FROM (
		SELECT ESERVICE.*,
			lnk.HUB_NMI_HSH_KEY,
			lnk.HUB_PARTICIPANT_HSH_KEY,
			hubnmi.NMI AS INT_UI,
			hubpart.SRVID,
			ROW_NUMBER() OVER (
				PARTITION BY lnk.HUB_NMI_HSH_KEY,
				VERTRAG ORDER BY ESERVICE.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_PARTICIPANT_CIS_ESERVICE ESERVICE
		INNER JOIN im${logicalenv}_rawvault.LNK_NMI_PARTICIPANT lnk
			ON ESERVICE.LNK_NMI_PARTICIPANT_HSH_KEY = lnk.LNK_NMI_PARTICIPANT_HSH_KEY
		INNER JOIN im${logicalenv}_rawvault.HUB_NMI hubnmi
			ON lnk.HUB_NMI_HSH_KEY = hubnmi.HUB_NMI_HSH_KEY
		INNER JOIN im${logicalenv}_rawvault.HUB_PARTICIPANT hubpart
			ON lnk.HUB_PARTICIPANT_HSH_KEY = hubpart.HUB_PARTICIPANT_HSH_KEY
		WHERE SERVICE = 'FRMP'
			AND date_format(ESERVICE.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) ESERVICE
	WHERE ESERVICE.rec_seq = 1
		AND ESERVICE.CHANGE_TYPE != 'D'
	),
EUITRANS_V1
AS (
	SELECT hub.NMI,
		salnk.HUB_NMI_HSH_KEY1,
		salnk.HUB_NMI_HSH_KEY2
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.SALNK_NMI_HSH_KEY,
				sat.DATEFROM,
				sat.TIMEFROM ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUITRANS sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
			AND length(trim(sat.NMI_CHECKSUM)) > 0
		) EUITRANS
	INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
		ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub
		ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
	WHERE EUITRANS.rec_seq = 1
		AND EUITRANS.CHANGE_TYPE <> 'D'
		AND salnk.HUB_NMI_KEY1_DEF = 'EXT_UI'
		AND salnk.HUB_NMI_KEY2_DEF = 'INT_UI'
	),
ESERVICE_V1
AS (
	SELECT *
	FROM (
		SELECT EUITRANS.NMI AS NMI,
			ESERVICE.SRVID AS SRVID,
			ESERVICE.VERTRAG,
			ESERVICE.SERVICE_START,
			ESERVICE.SERVICE_END,
			ESERVICE.EXTRACT_DATE_TIME AS ESERVICE_ROW_EFF_DTM,
            ROW_NUMBER() OVER (
				PARTITION BY EUITRANS.NMI, ESERVICE.SERVICE_END ORDER BY ESERVICE.EXTRACT_DATE_TIME DESC,ESERVICE.SERVICE_START DESC
				) AS RNO
		FROM ESERVICE_V0 AS ESERVICE
		INNER JOIN EUITRANS_V1 AS EUITRANS
			ON ESERVICE.HUB_NMI_HSH_KEY = EUITRANS.HUB_NMI_HSH_KEY2
		)
	WHERE RNO = 1 -- Observed duplicate records in non-prod, so added rank condition based on date column to keep the latest versions
	) ,
-- get the ANLAGE and INT_UI from Installation      
EUIINSTLN_V3 as(
    SELECT hub1.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY1 AS ANLAGE_HSH_KEY,
	hub2.nmi AS INT_UI,
	salnk.HUB_NMI_HSH_KEY2 AS INT_UI_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUIINSTLN sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) EUIINSTLN
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
	ON EUIINSTLN.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUIINSTLN.rec_seq = 1
		AND EUIINSTLN.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub1
	ON salnk.HUB_NMI_HSH_KEY2 = hub1.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
     ), 
-- get the ANLAGE and VKONTO from EVER       
 EVER_V4 as(
SELECT hub2.nmi AS ANLAGE,
	salnk.HUB_NMI_HSH_KEY2 AS ANLAGE_HSH_KEY,
	hub3.nmi AS VKONTO,
	salnk.HUB_NMI_HSH_KEY3 AS VKONTO_HSH_KEY
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY sat.SALNK_NMI2_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EVER sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) ever
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI2 salnk
	ON ever.SALNK_NMI2_HSH_KEY = salnk.SALNK_NMI2_HSH_KEY
		AND ever.rec_seq = 1
		AND ever.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub2
	ON salnk.HUB_NMI_HSH_KEY2 = hub2.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub3
	ON salnk.HUB_NMI_HSH_KEY3 = hub3.HUB_NMI_HSH_KEY
     ), 
-- get the PARTNER and VKONTO  and BPART is taken in output from here   
FKKVKP_V5 AS(
SELECT hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_nmi.HUB_NMI_HSH_KEY AS VKONT_HSH_KEY,
	hub_bpart.PARTNER AS PARTNER,
	hub_nmi.NMI AS VKONT
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_FKKVKP sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
	) fkkvkp
INNER JOIN im${logicalenv}_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
	ON fkkvkp.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
	ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_bpart
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_bpart.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE fkkvkp.rec_seq = 1
	AND fkkvkp.CHANGE_TYPE != 'D'
     ) ,
-- NAMES of a business partner
BUT000_V6 AS(
SELECT HUB_BUSINESS_PARTNER_HSH_KEY,
	HUB_NMI_HSH_KEY,
	PARTNER,
	NMI,
	NMI_PARTNER_FROM,
	nvl(NMI_PARTNER_TO, '99991231') AS NMI_PARTNER_TO,
	NAME_ORG1,
	NAME_ORG2,
	NAME_ORG3,
	NAME_ORG4,
	TYPE,
	LNK_NMI_BUSINESS_PARTNER_HSH_KEY
FROM (
	SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
		hub_nmi.HUB_NMI_HSH_KEY,
		hub_part.PARTNER AS PARTNER,
		hub_nmi.NMI AS NMI,
		regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS NMI_PARTNER_FROM,
		LEAD(regexp_replace(to_date(but000.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY ORDER BY but000.EXTRACT_DATE_TIME
			) AS NMI_PARTNER_TO,
		but000.NAME_ORG1,
		but000.NAME_ORG2,
		but000.NAME_ORG3,
		but000.NAME_ORG4,
		but000.TYPE,
		but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
		but000.extract_date_time,
		but000.rec_seq
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_BUSINESS_PARTNER_HSH_KEY,
				DATE (sat.extract_date_time) ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_BUT000 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) but000
	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_BUSINESS_PARTNER lnk
		ON but000.LNK_NMI_BUSINESS_PARTNER_HSH_KEY = lnk.LNK_NMI_BUSINESS_PARTNER_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi
		ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
		ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
	WHERE but000.rec_seq = 1
		AND but000.CHANGE_TYPE != 'D'
	) a
),
-- Address number, Valid_from,valid_to , partner      
BUT021_FS_V7 AS(
SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_address.HUB_ADDRESS_HSH_KEY,
	hub_part.PARTNER AS PARTNER,
	hub_address.ADDRNUMBER AS ADDRNUMBER,
	SUBSTRING(but021.VALID_FROM, 1, 8) AS VALID_FROM,
	SUBSTRING(but021.VALID_TO, 1, 8) AS VALID_TO,
	but021.ADR_KIND,
	but021.EXTRACT_DATE_TIME AS BUT021_FS_ROW_EFF_DTM
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY ,VALID_TO ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND sat.ADR_KIND = 'XXDEFAULT'
	) but021
INNER JOIN im${logicalenv}_rawvault.LNK_BUSINESS_PARTNER_ADDRESS lnk
	ON but021.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY = lnk.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
	ON lnk.HUB_ADDRESS_HSH_KEY = hub_address.HUB_ADDRESS_HSH_KEY
INNER JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE but021.rec_seq = 1
	AND but021.CHANGE_TYPE != 'D'
     ),
 -- telphone numbre, personal number, Date_from , consumber
ADR2_V8 AS(
SELECT HUB_ADDRESS_HSH_KEY,
	ADDRNUMBER,
	TEL_FROM,
	nvl(TEL_TO, '99991231') AS TEL_TO,
	TEL_NUMBER1,
	TEL_NUMBER2,
	PERSNUMBER,
	DATE_FROM,
	CONSNUMBER
FROM (
	SELECT hub_address.HUB_ADDRESS_HSH_KEY,
		hub_address.ADDRNUMBER AS ADDRNUMBER,
		regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '') AS TEL_FROM,
		LEAD(regexp_replace(to_date(addr.EXTRACT_DATE_TIME, 'yyyyMMdd'), '-', '')) OVER (
			PARTITION BY addr.HUB_ADDRESS_HSH_KEY ORDER BY addr.EXTRACT_DATE_TIME
			) AS TEL_TO,
		addr.TEL_NUMBER1 AS TEL_NUMBER1,
		addr.TEL_NUMBER2 AS TEL_NUMBER2,
		addr.PERSNUMBER,
		addr.DATE_FROM,
		addr.CONSNUMBER
	FROM (
		SELECT sat.HUB_ADDRESS_HSH_KEY,
			sat.TEL_NUMBER AS TEL_NUMBER1,
			LEAD(sat.TEL_NUMBER) OVER (
				PARTITION BY sat.HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,
					CONSNUMBER DESC
				) AS TEL_NUMBER2,
			sat.DATE_FROM,
			sat.CONSNUMBER,
			sat.PERSNUMBER,
			sat.EXTRACT_DATE_TIME,
			sat.CHANGE_TYPE,
			ROW_NUMBER() OVER (
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,CONSNUMBER DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.SAT_ADDRESS_CIS_ADR2 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) addr
	INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
		ON hub_address.HUB_ADDRESS_HSH_KEY = addr.HUB_ADDRESS_HSH_KEY
	WHERE /*addr.rec_seq = 1
		AND*/ addr.CHANGE_TYPE != 'D'
	) a
 ),
-- get NMI from EUITRANS and NMI_FROM and NMI_TO dates
EUITRANS_V9 as(
SELECT hub1_nmi.NMI,
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
	FROM im${logicalenv}_rawvault.LSAT_NMI_CIS_EUITRANS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		AND length(trim(sat.NMI_CHECKSUM)) > 0
	) EUITRANS
INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk
	ON EUITRANS.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EUITRANS.rec_seq = 1
		AND EUITRANS.CHANGE_TYPE <> 'D'
INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub1_nmi
	ON salnk.HUB_NMI_HSH_KEY1 = hub1_nmi.HUB_NMI_HSH_KEY
     ),
-- Timeline framing start --
date_lst1
AS (
	SELECT DISTINCT EUITRANS_V9.NMI AS NMI,
		stack(99, NMI_PARTNER_TO, DATETO, TEL_TO, VALID_TO, SERVICE_END, BUT000_V6.NMI_PARTNER_FROM, EUITRANS_V9.DATEFROM, ADR2_V8.TEL_FROM, BUTF.VALID_FROM, ESERVICE_V1.SERVICE_START) AS DT_LST
	FROM EUITRANS_V9
	LEFT JOIN ESERVICE_V1
		ON ESERVICE_V1.NMI = EUITRANS_V9.NMI 
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = EUITRANS_V9.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6 BUT000_V6
		ON BUT000_V6.HUB_BUSINESS_PARTNER_HSH_KEY = FKKVKP_V5.HUB_BUSINESS_PARTNER_HSH_KEY
	LEFT JOIN BUT021_FS_V7 BUTF
		ON BUT000_V6.HUB_BUSINESS_PARTNER_HSH_KEY = BUTF.HUB_BUSINESS_PARTNER_HSH_KEY
	LEFT JOIN ADR2_V8
		ON BUTF.HUB_ADDRESS_HSH_KEY = ADR2_V8.HUB_ADDRESS_HSH_KEY
	WHERE EUITRANS_V9.NMI IN (
			'6305638357'
			)
	),
date_lst2
AS ( -- Frame FROM and TO Date
	SELECT NMI,
		DT_LST AS FROM_DT,
		LEAD(DT_LST, 1) OVER (
			PARTITION BY NMI ORDER BY DT_LST
			) TO_DT
	FROM date_lst1
	),
date_lst3
AS ( -- Exclude Nulls and invalid time slice
	SELECT NMI,
		to_date(FROM_DT,"yyyyMMdd") as FROM_DT,
        case when lead(to_date(FROM_DT,"yyyyMMdd")) over (PARTITION BY NMI ORDER BY FROM_DT) = to_date(TO_DT,"yyyyMMdd") then date_sub(to_date(TO_DT,"yyyyMMdd"),1) else to_date(TO_DT,"yyyyMMdd") end as TO_DT
	FROM date_lst2
	WHERE datediff(to_date(TO_DT,"yyyyMMdd"),to_date(FROM_DT,"yyyyMMdd")) >1 
	),
-- Timeline framing end --    
date_lst4 
AS ( -- Join tables based on Key and Dates
	SELECT DISTINCT S1.NMI AS NMI,
        dl.FROM_DT AS START_DT,
		DL.TO_DT AS END_DT,
		S3.NAME_ORG1 AS BUT000_NAME_ORG1,
		S3.NAME_ORG2 AS BUT000_NAME_ORG2,
		S3.NAME_ORG3 AS BUT000_NAME_ORG3,
		S3.NAME_ORG4 AS BUT000_NAME_ORG4,
		S5.TEL_NUMBER1 AS ADR2_TEL_NUMBER1,
		S5.TEL_NUMBER2 AS ADR2_TEL_NUMBER2,
		S2.SRVID AS ESERVICE_SERVICE_ID,
		S5.PERSNUMBER AS ADR2_PERSNUMBER,
		S5.DATE_FROM AS ADR2_DATE_FROM,
		S5.CONSNUMBER AS ADR2_CONSNUMBER,
		date_format(to_date(S5.TEL_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS ADR2_ROW_EFF_DTM,
		S3.TYPE AS BUT000_TYPE,
		date_format(to_date(S3.NMI_PARTNER_FROM, 'yyyyMMdd'), 'yyyy-MM-dd HH:mm:ss') AS BUT000_ROW_EFF_DTM,
		S3.PARTNER AS BUT021_FS_PARTNER,
		to_date(S4.VALID_FROM, 'yyyyMMdd') AS BUT021_FS_VALID_FROM,
		to_date(S4.VALID_TO, 'yyyyMMdd') AS BUT021_FS_VALID_TO,
		S4.ADR_KIND AS BUT021_FS_ADR_KIND,
		S4.ADDRNUMBER AS BUT021_FS_ADDRNUMBER,
		date_format(S4.BUT021_FS_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS BUT021_FS_ROW_EFF_DTM,
		S2.VERTRAG AS ESERVICE_VERTRAG,
		date_format(S2.ESERVICE_ROW_EFF_DTM, 'yyyy-MM-dd HH:mm:ss') AS ESERVICE_ROW_EFF_DTM,
		to_date(S2.SERVICE_START, "yyyyMMdd") AS ESERVICE_SERVICE_START,
		to_date(S2.SERVICE_END, "yyyyMMdd") AS ESERVICE_SERVICE_END,
		date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM,
        rank() OVER (PARTITION BY S1.NMI,dl.FROM_DT order by to_date(NMI_PARTNER_FROM,'yyyyMMdd') desc, date_format(BUT021_FS_ROW_EFF_DTM,'yyyy-MM-dd HH:mm:ss') desc ) as RNO
	FROM EUITRANS_V9 S1
	INNER JOIN date_lst3 dl
		ON (
				S1.NMI = dl.NMI
				AND to_date(S1.DATETO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S1.DATEFROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ESERVICE_V1 S2
		ON (
				S2.NMI = S1.NMI
				AND to_date(S2.SERVICE_END, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S2.SERVICE_START, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN EUIINSTLN_V3
		ON EUIINSTLN_V3.INT_UI_HSH_KEY = S1.INT_UI_HSH_KEY
	LEFT JOIN EVER_V4
		ON EVER_V4.ANLAGE_HSH_KEY = EUIINSTLN_V3.ANLAGE_HSH_KEY
	LEFT JOIN FKKVKP_V5
		ON FKKVKP_V5.VKONT_HSH_KEY = EVER_V4.VKONTO_HSH_KEY
	LEFT JOIN BUT000_V6 S3
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = FKKVKP_V5.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S3.NMI_PARTNER_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S3.NMI_PARTNER_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN BUT021_FS_V7 S4
		ON (
				S3.HUB_BUSINESS_PARTNER_HSH_KEY = S4.HUB_BUSINESS_PARTNER_HSH_KEY
				AND to_date(S4.VALID_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S4.VALID_FROM, "yyyyMMdd") < dl.TO_DT
				)
	LEFT JOIN ADR2_V8 S5
		ON (
				S4.HUB_ADDRESS_HSH_KEY = S5.HUB_ADDRESS_HSH_KEY
				AND to_date(S5.TEL_TO, "yyyyMMdd") > dl.FROM_DT
				AND to_date(S5.TEL_FROM, "yyyyMMdd") < dl.TO_DT
				)     
	),
cat as (   
SELECT 
    NMI	,
    START_DT	,
    END_DT	,
    BUT000_NAME_ORG1	,
    BUT000_NAME_ORG2	,
    BUT000_NAME_ORG3	,
    BUT000_NAME_ORG4	,
    ADR2_TEL_NUMBER1	,
    ADR2_TEL_NUMBER2	,
    ESERVICE_SERVICE_ID	,
    ADR2_PERSNUMBER	,
    ADR2_DATE_FROM	,
    ADR2_CONSNUMBER	,
    ADR2_ROW_EFF_DTM	,
    BUT000_TYPE	,
    BUT000_ROW_EFF_DTM	,
    BUT021_FS_PARTNER	,
    BUT021_FS_VALID_FROM	,
    BUT021_FS_VALID_TO	,
    BUT021_FS_ADR_KIND	,
    BUT021_FS_ADDRNUMBER	,
    BUT021_FS_ROW_EFF_DTM	,
    ESERVICE_VERTRAG	,
    ESERVICE_ROW_EFF_DTM	,
    ESERVICE_SERVICE_START	,
    ESERVICE_SERVICE_END	,
    ROW_INSERT_DTM	,
    RNO
FROM date_lst4
where ESERVICE_VERTRAG is NOT NULL
-- AND NMI = '6305002138'
AND RNO = 1 -- During the SQL built there are data quality issues in DEV, to neglect it, we are using the rank on key columns to get only latest
)
select
  NMI	,
--  CASE 
-- 	WHEN START_DT < to_date('20150101', 'yyyyMMdd')  and  END_DT = to_date('99991231', 'yyyyMMdd') -- ADW has cutoff date as 20150101, so allign with it
-- 		THEN to_date('20150101', 'yyyyMMdd')
-- 		ELSE START_DT
-- 	END AS START_DT,
START_DT,
    END_DT	,
    BUT000_NAME_ORG1	,
    BUT000_NAME_ORG2	,
    BUT000_NAME_ORG3	,
    BUT000_NAME_ORG4	,
    ADR2_TEL_NUMBER1	,
    ADR2_TEL_NUMBER2	,
    ESERVICE_SERVICE_ID	,
    ADR2_PERSNUMBER	,
    ADR2_DATE_FROM	,
    ADR2_CONSNUMBER	,
    ADR2_ROW_EFF_DTM	,
    BUT000_TYPE	,
    BUT000_ROW_EFF_DTM	,
    BUT021_FS_PARTNER	,
    BUT021_FS_VALID_FROM	,
    BUT021_FS_VALID_TO	,
    BUT021_FS_ADR_KIND	,
    BUT021_FS_ADDRNUMBER	,
    BUT021_FS_ROW_EFF_DTM	,
    ESERVICE_VERTRAG	,
    ESERVICE_ROW_EFF_DTM	,
    ESERVICE_SERVICE_START	,
    ESERVICE_SERVICE_END	,
    ROW_INSERT_DTM            
from cat
-- where to_date(END_DT,"yyyyMMdd") >= to_date('20210918','yyyyMMdd') -- Since IM don't have history for address, cut off date based on 5MS go-live is defined to take only latest data(History will be migrated from ADW CAD)
