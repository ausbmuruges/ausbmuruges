-- Databricks notebook source
WITH ORG
	-- Only ANS org id required for MHE
AS (
	SELECT hro.HUB_REF_ORG_HSH_KEY,
		ORG_ID,
		NAME
	FROM imprd001_rawvault.hub_ref_org hro
	INNER JOIN (
		SELECT sor.*,
			ROW_NUMBER() OVER (
				PARTITION BY sor.HUB_REF_ORG_HSH_KEY ORDER BY sor.EXTRACT_DATE_TIME DESC,
					sor.LOAD_DATE_TIME DESC
				) AS rno
		FROM imprd001_rawvault.sat_ref_org_eip_org sor
		) sroeo ON sroeo.HUB_REF_ORG_HSH_KEY = hro.HUB_REF_ORG_HSH_KEY
		AND sroeo.NAME = 'ANS'
	WHERE rno = 1
	),
Channels
AS (
	-- Fillter only E and B channel to reduce data volume in Outbound
	SELECT HUB_CHANNEL_HSH_KEY,
		NAME,
		value,
		EFF_START_TIME,
		EFF_END_TIME
	FROM (
		SELECT sc.HUB_CHANNEL_HSH_KEY,
			sc.NAME,
			sc.value,
			DATE (sc.EFF_START_TIME),
			DATE (nvl(sc.EFF_END_TIME, '9999-12-31')) EFF_END_TIME,
			ROW_NUMBER() OVER (
				PARTITION BY sc.HUB_CHANNEL_HSH_KEY,
				sc.NAME,
				sc.EFF_START_TIME ORDER BY REC_VERSION_NUM DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.SAT_CHANNEL_EIP_CHANNEL_PARAM sc
		WHERE NAME = 'Data Stream Suffix'
			AND (
				upper(value) LIKE '%B%'
				OR upper(value) LIKE '%E%'
				)
			AND CHANGE_TYPE != 'D'
		)
	WHERE rec_seq = 1
	)
SELECT --HUB_CHANNEL_HSH_KEY,
	CHANNEL_ID,
	-- INTERVAL_END_TIME,
	CAST(REPLACE(READ_DT, '-') AS INT) AS READ_DT,
	cast(INTERVAL_SEQ_NUM AS INT),
	cast(CONSUMPTION AS DECIMAL(21, 6)) AS CONSUMPTION,
	VALIDATION_STATUS,
	CHANGE_METHOD,
	cast(INTERVAL_LEN AS INT),
	date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
FROM (
	SELECT hub.CHANNEL_ID,
		sat.HUB_CHANNEL_HSH_KEY,
		sat.INTERVAL_END_TIME,
		-- form read dt by subracting interval time by interval length (midnight read belongs to previous read date)
		DATE (sat.INTERVAL_END_TIME - make_interval(0, 0, 0, 0, 0, (sat.INTERVAL_LEN / 60), 0)) AS READ_DT,
		-- form interval sequence number based on interval time            
		(unix_timestamp(sat.INTERVAL_END_TIME) - unix_timestamp(DATE (sat.INTERVAL_END_TIME - make_interval(0, 0, 0, 0, 0, (sat.INTERVAL_LEN / 60), 0)))) / sat.INTERVAL_LEN AS INTERVAL_SEQ_NUM,
		sat.LP_VALUE AS CONSUMPTION,
		sat.VALIDATION_STATUS,
		sat.INTERVAL_LEN,
		sat.CHANGE_METHOD,
		row_number() OVER (
			PARTITION BY sat.HUB_CHANNEL_HSH_KEY,
			INTERVAL_END_TIME ORDER BY sat.EXTRACT_DATE_TIME DESC,
				sat.LOAD_DATE_TIME DESC
			) AS RNK
	FROM imprd001_rawvault.SAT_CHANNEL_EIP_LP_INTERVALS sat
	INNER JOIN org ON sat.org_id = org.org_id
	INNER JOIN imprd001_rawvault.HUB_CHANNEL hub ON sat.HUB_CHANNEL_HSH_KEY = hub.HUB_CHANNEL_HSH_KEY
	INNER JOIN Channels ch ON ch.HUB_CHANNEL_HSH_KEY = hub.HUB_CHANNEL_HSH_KEY
	WHERE date_format(sat.EXTRACT_DATE_PARTITION, "yyyyMMdd") >= date_format(date_sub('2021-11-08 03', 1), "yyyyMMdd")
		AND date_format(sat.EXTRACT_DATE_PARTITION, "yyyyMMdd") <= date_format(date_add('2021-11-08 03', 1), "yyyyMMdd")
		-- If filter hour parameter greater 0 then filter data based on extract hour else ignore
		AND (
			CASE 
				WHEN '8' != 0
					THEN (
							(
								sat.EXTRACT_DATE_TIME >= '2021-11-07 21' + MAKE_INTERVAL(0, 0, 0, 0, - '8', 0, 0)
								AND sat.EXTRACT_DATE_TIME <= '2021-11-08 03'
								-- ADF Schdule rundate is always AEST
								)
							OR (
								from_utc_timestamp(sat.LOAD_DATE_TIME, 'GMT+10') >= '2021-11-07 21' + MAKE_INTERVAL(0, 0, 0, 0, - '8', 0, 0) 
								AND from_utc_timestamp(sat.LOAD_DATE_TIME, 'GMT+10') <= '2021-11-08 03'
								-- ADF Schdule rundate is always AEST
								)
							)
				ELSE 1 = 1
				END
			)
		-- Only Valid and estimated reads required for MHE
		AND trim(upper(sat.VALIDATION_STATUS)) IN ('VAL', 'EST')
		-- Load only last 2 years interval data
		AND DATE (sat.INTERVAL_END_TIME - make_interval(0, 0, 0, 0, 0, (sat.INTERVAL_LEN / 60), 0)) >= (CURRENT_DATE () - 761)
	) e
WHERE e.rnk = 1

-- COMMAND ----------

-- OPTIMIZE  imprd001_rawvault.hub_channel
OPTIMIZE  imprd001_rawvault.SAT_CHANNEL_EIP_CHANNEL_PARAM

-- COMMAND ----------

WITH ORG
	-- Only ANS org id required for MHE
AS (
	SELECT hro.HUB_REF_ORG_HSH_KEY,
		ORG_ID,
		NAME
	FROM imprd001_rawvault.hub_ref_org hro
	INNER JOIN (
		SELECT sor.*,
			ROW_NUMBER() OVER (
				PARTITION BY sor.HUB_REF_ORG_HSH_KEY ORDER BY sor.EXTRACT_DATE_TIME DESC,
					sor.LOAD_DATE_TIME DESC
				) AS rno
		FROM imprd001_rawvault.sat_ref_org_eip_org sor
		) sroeo ON sroeo.HUB_REF_ORG_HSH_KEY = hro.HUB_REF_ORG_HSH_KEY
		AND sroeo.NAME = 'ANS'
	WHERE rno = 1
	),
Channels
AS (
	-- Fillter only E and B channel to reduce data volume in Outbound
	SELECT HUB_CHANNEL_HSH_KEY,
		NAME,
		value,
		EFF_START_TIME,
		EFF_END_TIME
	FROM (
		SELECT sc.HUB_CHANNEL_HSH_KEY,
			sc.NAME,
			sc.value,
			DATE (sc.EFF_START_TIME),
			DATE (nvl(sc.EFF_END_TIME, '9999-12-31')) EFF_END_TIME,
			ROW_NUMBER() OVER (
				PARTITION BY sc.HUB_CHANNEL_HSH_KEY,
				sc.NAME,
				sc.EFF_START_TIME ORDER BY REC_VERSION_NUM DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.SAT_CHANNEL_EIP_CHANNEL_PARAM sc
		WHERE NAME = 'Data Stream Suffix'
			AND (
				upper(value) LIKE '%B%'
				OR upper(value) LIKE '%E%'
				)
			AND CHANGE_TYPE != 'D'
		)
	WHERE rec_seq = 1
	)
SELECT --HUB_CHANNEL_HSH_KEY,
	CHANNEL_ID,
	-- INTERVAL_END_TIME,
	CAST(REPLACE(READ_DT, '-') AS INT) AS READ_DT,
	cast(INTERVAL_SEQ_NUM AS INT),
	cast(CONSUMPTION AS DECIMAL(21, 6)) AS CONSUMPTION,
	VALIDATION_STATUS,
	CHANGE_METHOD,
	cast(INTERVAL_LEN AS INT),
	date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
FROM (
	SELECT hub.CHANNEL_ID,
		sat.HUB_CHANNEL_HSH_KEY,
		sat.INTERVAL_END_TIME,
		-- form read dt by subracting interval time by interval length (midnight read belongs to previous read date)
		DATE (sat.INTERVAL_END_TIME - make_interval(0, 0, 0, 0, 0, (sat.INTERVAL_LEN / 60), 0)) AS READ_DT,
		-- form interval sequence number based on interval time            
		(unix_timestamp(sat.INTERVAL_END_TIME) - unix_timestamp(DATE (sat.INTERVAL_END_TIME - make_interval(0, 0, 0, 0, 0, (sat.INTERVAL_LEN / 60), 0)))) / sat.INTERVAL_LEN AS INTERVAL_SEQ_NUM,
		sat.LP_VALUE AS CONSUMPTION,
		sat.VALIDATION_STATUS,
		sat.INTERVAL_LEN,
		sat.CHANGE_METHOD,
		row_number() OVER (
			PARTITION BY sat.HUB_CHANNEL_HSH_KEY,
			INTERVAL_END_TIME ORDER BY sat.EXTRACT_DATE_TIME DESC,
				sat.LOAD_DATE_TIME DESC
			) AS RNK
	FROM imprd001_rawvault.SAT_CHANNEL_EIP_LP_INTERVALS sat
	INNER JOIN org ON sat.org_id = org.org_id
	INNER JOIN imprd001_rawvault.HUB_CHANNEL hub ON sat.HUB_CHANNEL_HSH_KEY = hub.HUB_CHANNEL_HSH_KEY
	INNER JOIN Channels ch ON ch.HUB_CHANNEL_HSH_KEY = hub.HUB_CHANNEL_HSH_KEY
	WHERE date_format(sat.EXTRACT_DATE_PARTITION, "yyyyMMdd") >= date_format(date_sub('2021-11-24 15', 1), "yyyyMMdd")
		AND date_format(sat.EXTRACT_DATE_PARTITION, "yyyyMMdd") <= date_format(date_add('2021-11-24 15', 1), "yyyyMMdd")
		-- If filter hour parameter greater 0 then filter data based on extract hour else ignore
		AND (
			CASE 
				WHEN '8' != 0
					THEN (
							(
								sat.EXTRACT_DATE_TIME >= '2021-11-24 15' + MAKE_INTERVAL(0, 0, 0, 0, - '8', 0, 0)
								AND sat.EXTRACT_DATE_TIME <= '2021-11-24 15'
								-- ADF Schdule rundate is always AEST
								)
							OR (
								from_utc_timestamp(sat.LOAD_DATE_TIME, 'GMT+10') >= '2021-11-24 15' + MAKE_INTERVAL(0, 0, 0, 0, - '8', 0, 0) 
								AND from_utc_timestamp(sat.LOAD_DATE_TIME, 'GMT+10') <= '2021-11-24 15'
								-- ADF Schdule rundate is always AEST
								)
							)
				ELSE 1 = 1
				END
			)
		-- Only Valid and estimated reads required for MHE
		AND trim(upper(sat.VALIDATION_STATUS)) IN ('VAL', 'EST')
		-- Load only last 2 years interval data
		AND DATE (sat.INTERVAL_END_TIME - make_interval(0, 0, 0, 0, 0, (sat.INTERVAL_LEN / 60), 0)) >= (CURRENT_DATE () - 761)
	) e
WHERE e.rnk = 1


-- COMMAND ----------

OPTIMIZE imprd001_rawvault.hub_channel
