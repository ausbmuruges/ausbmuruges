-- Databricks notebook source
select * from imprd001_rawvault.SAT_DEVICE_PNT_AMI_DEVICE_EVENT where 
TZ_OFFSET IS NOT NULL

-- COMMAND ----------

CREATE WIDGET TEXT logicalenv DEFAULT "prd001";
CREATE WIDGET TEXT run_date DEFAULT "2021-09-27";

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
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,CONSNUMBER DESC, sat.EXTRACT_DATE_TIME DESC
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
     )
	SELECT *
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
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,CONSNUMBER DESC, sat.EXTRACT_DATE_TIME DESC
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
     )
	SELECT *
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
          

-- COMMAND ----------

select * from im${logicalenv}_rawvault.HUB_ADDRESS 
where ADDRNUMBER = '17436123'

-- COMMAND ----------

select * from im${logicalenv}_rawvault.SAT_ADDRESS_CIS_ADR2 where HUB_ADDRESS_HSH_KEY = 'ccd33b3a2f41fd0105665462681f462609a517aa'

-- COMMAND ----------

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
				PARTITION BY HUB_ADDRESS_HSH_KEY ORDER BY FLGDEFAULT DESC,CONSNUMBER DESC, sat.EXTRACT_DATE_TIME DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.SAT_ADDRESS_CIS_ADR2 sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) addr
	INNER JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
		ON hub_address.HUB_ADDRESS_HSH_KEY = addr.HUB_ADDRESS_HSH_KEY
	WHERE addr.rec_seq = 1
		AND addr.CHANGE_TYPE != 'D'
	) a
    where HUB_ADDRESS_HSH_KEY = 'ccd33b3a2f41fd0105665462681f462609a517aa'

-- COMMAND ----------

SELECT hub_part.HUB_BUSINESS_PARTNER_HSH_KEY,
	hub_address.HUB_ADDRESS_HSH_KEY,
	hub_part.PARTNER AS PARTNER,
	hub_address.ADDRNUMBER AS ADDRNUMBER,
	SUBSTRING(but021.VALID_FROM, 1, 8) AS VALID_FROM,
	SUBSTRING(but021.VALID_TO, 1, 8) AS VALID_TO,
	but021.ADR_KIND,
    rec_seq,
	but021.EXTRACT_DATE_TIME AS BUT021_FS_ROW_EFF_DTM,
    CHANGE_TYPE
FROM (
	SELECT sat.*,
		ROW_NUMBER() OVER (
			PARTITION BY LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY,VALID_TO ORDER BY sat.extract_date_time DESC
			) AS `rec_seq`
	FROM im${logicalenv}_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS sat
	WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd") and 
ADR_KIND = 'XXDEFAULT'
	) but021
left JOIN im${logicalenv}_rawvault.LNK_BUSINESS_PARTNER_ADDRESS lnk
	ON but021.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY = lnk.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY
left JOIN im${logicalenv}_rawvault.HUB_ADDRESS hub_address
	ON lnk.HUB_ADDRESS_HSH_KEY = hub_address.HUB_ADDRESS_HSH_KEY
left JOIN im${logicalenv}_rawvault.HUB_BUSINESS_PARTNER hub_part
	ON lnk.HUB_BUSINESS_PARTNER_HSH_KEY = hub_part.HUB_BUSINESS_PARTNER_HSH_KEY
WHERE but021.rec_seq = 1
	AND but021.CHANGE_TYPE != 'D'
and but021.LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY = 'ff10fc1ab7777401c888e0a4764be82c28fb1a0e'
--    AND ADR_KIND = 'XXDEFAULT'

-- COMMAND ----------

select * from im${logicalenv}_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS where LNK_BUSINESS_PARTNER_ADDRESS_HSH_KEY='ff10fc1ab7777401c888e0a4764be82c28fb1a0e'
