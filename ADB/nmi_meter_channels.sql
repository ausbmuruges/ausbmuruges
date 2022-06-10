-- Databricks notebook source
CREATE WIDGET TEXT logicalenv DEFAULT "prd001";

-- COMMAND ----------

-- =============================================
-- Author:      Sainath 
-- Create Date: 23/08/2021
-- Description: Extracts EIP Standing Data for MHE feed
--              Time Slice based on NMI, Meter, Suffix, Channel
-- =============================================
WITH EIP_SVC_PT_V1
	-- Get Service Point Details
AS (
	SELECT hub1.HUB_NMI_HSH_KEY AS HUB_NMI_HSH_KEY,
		hub1.NMI AS SVC_PT_ID,
		hub.NMI AS NMI,
		SVC_PT.*
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_EIP_SVC_PT sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) SVC_PT
	INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk ON SVC_PT.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI AS hub ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI AS hub1 ON salnk.HUB_NMI_HSH_KEY2 = hub1.HUB_NMI_HSH_KEY
	WHERE SVC_PT.rec_seq = 1
		AND SVC_PT.CHANGE_TYPE != 'D'
		-- AND hub.NMI in ('6305106980','6305002485','6305002543')
	),
SVC_PT_DEVICE_REL_V2
AS (
	-- Get Service Point to Device Relationship 
	SELECT hub_nmi.HUB_NMI_HSH_KEY,
		hub_nmi.NMI,
		hub_dvc.DEVICE_ID,
		hub_dvc.HUB_DEVICE_HSH_KEY,
		eip_spd_rel.*
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_DEVICE_HSH_KEY,
				sat.EFF_START_TIME ORDER BY extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_DEVICE_EIP_SVC_PT_DEVICE_REL sat
		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) eip_spd_rel
	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_DEVICE lnk ON eip_spd_rel.LNK_NMI_DEVICE_HSH_KEY = lnk.LNK_NMI_DEVICE_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hub_dvc ON lnk.HUB_DEVICE_HSH_KEY = hub_dvc.HUB_DEVICE_HSH_KEY
	WHERE rec_seq = 1
		AND eip_spd_rel.CHANGE_TYPE != 'D'
	),
EIP_DEVICE_V3
AS (
	-- Get Device Details
	SELECT hubdev.HUB_DEVICE_HSH_KEY,
		hubdev.DEVICE_ID,
		eip_device.MFG_SERIAL_NUM,
		eip_device.STATUS_CD
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.HUB_DEVICE_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.SAT_DEVICE_EIP_DEVICE_DETAILS sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) eip_device
	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hubdev ON eip_device.HUB_DEVICE_HSH_KEY = hubdev.HUB_DEVICE_HSH_KEY
	WHERE eip_device.rec_seq = 1
	),
CHANNEL_DETAILS_V4
AS (
	-- Get channel Details
	SELECT hub_chan.HUB_CHANNEL_HSH_KEY,
		hub_chan.CHANNEL_ID,
		hub_nmi.HUB_NMI_HSH_KEY,
		eip_channel.TYPE AS CHANNEL_TYPE,
		eip_channel.NAME AS CHANNEL_NAME,
		eip_channel.INTERVAL_LEN,
		eip_channel.IS_VIRTUAL_FLG,
		eip_channel.STATUS_CD,
		hub_nmi.NMI AS SVC_PT_ID,
		eip_channel.UDC_ID
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_CHANNEL_HSH_KEY ORDER BY extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_CHANNEL_EIP_CHANNEL_DETAILS sat
		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
			AND TYPE = 'Interval Data'
		) eip_channel
	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_CHANNEL lnk ON eip_channel.LNK_NMI_CHANNEL_HSH_KEY = lnk.LNK_NMI_CHANNEL_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hub_chan ON lnk.HUB_CHANNEL_HSH_KEY = hub_chan.HUB_CHANNEL_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	WHERE rec_seq = 1
		AND eip_channel.CHANGE_TYPE != 'D'
	),
DEVICE_CHANNEL_REL_V5
AS (
	-- Get Device to Channel Details
	SELECT hub_chan.HUB_CHANNEL_HSH_KEY,
		hub_chan.CHANNEL_ID,
		hub_dvc.DEVICE_ID,
		hub_dvc.HUB_DEVICE_HSH_KEY,
		eip_channel_rel.EFF_START_TIME,
		eip_channel_rel.EFF_END_TIME
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_DEVICE_CHANNEL_HSH_KEY,
				sat.EFF_START_TIME ORDER BY extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_DEVICE_CHANNEL_EIP_DEVICE_CHANNEL_REL sat
		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) eip_channel_rel
	INNER JOIN im${logicalenv}_rawvault.LNK_DEVICE_CHANNEL lnk ON eip_channel_rel.LNK_DEVICE_CHANNEL_HSH_KEY = lnk.LNK_DEVICE_CHANNEL_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hub_chan ON lnk.HUB_CHANNEL_HSH_KEY = hub_chan.HUB_CHANNEL_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hub_dvc ON lnk.HUB_DEVICE_HSH_KEY = hub_dvc.HUB_DEVICE_HSH_KEY
	WHERE rec_seq = 1
		AND eip_channel_rel.CHANGE_TYPE != 'D'
	),
EIP_CHANEEL_PARAM_V6
AS (
	-- Get Channel Parameters
	SELECT hubchan.HUB_CHANNEL_HSH_KEY,
		hubchan.CHANNEL_ID,
		eip_channel_param.NAME AS NAME,
		eip_channel_param.VALUE AS VALUE,
		eip_channel_param.EFF_START_TIME AS EFF_START_TIME,
		nvl(eip_channel_param.EFF_END_TIME, to_timestamp('9999-12-31')) AS EFF_END_TIME
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.HUB_CHANNEL_HSH_KEY,
				sat.NAME,
				sat.EFF_START_TIME ORDER BY REC_VERSION_NUM DESC
				) AS `rec_seq`
		--used record version column from the data in sorting to get the latest records
		FROM im${logicalenv}_rawvault.SAT_CHANNEL_EIP_CHANNEL_PARAM sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
			AND upper(trim(sat.NAME)) = 'DATA STREAM SUFFIX'
			-- Take only Data Stream Suffix Channel which is required for MHE
		) eip_channel_param
	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hubchan ON eip_channel_param.HUB_CHANNEL_HSH_KEY = hubchan.HUB_CHANNEL_HSH_KEY
	WHERE eip_channel_param.rec_seq = 1
		AND eip_channel_param.CHANGE_TYPE != 'D'
	),
date_lst1
AS (
	SELECT DISTINCT espv.NMI,
		stack(99, cast(spdl.EFF_START_TIME AS DATE), CASE 
				WHEN cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
				END, cast(dcr.EFF_START_TIME AS DATE), CASE 
				WHEN cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
				END, cast(ecpv.EFF_START_TIME AS DATE), CASE 
				WHEN cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
				END) AS DT_LST
	FROM EIP_SVC_PT_V1 AS espv
	INNER JOIN SVC_PT_DEVICE_REL_V2 AS spdl ON spdl.NMI = espv.SVC_PT_ID
	INNER JOIN EIP_DEVICE_V3 AS edev ON spdl.DEVICE_ID = edev.DEVICE_ID
	INNER JOIN DEVICE_CHANNEL_REL_V5 dcr ON edev.DEVICE_ID = dcr.DEVICE_ID
	INNER JOIN CHANNEL_DETAILS_V4 cd ON dcr.CHANNEL_ID = cd.CHANNEL_ID
		AND spdl.NMI = cd.SVC_PT_ID
	INNER JOIN EIP_CHANEEL_PARAM_V6 ecpv ON cd.CHANNEL_ID = ecpv.CHANNEL_ID
		WHERE espv.NMI IN (
			'6305996642'
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
		to_date(FROM_DT, "yyyyMMdd") AS FROM_DT,
		CASE 
			WHEN lead(to_date(FROM_DT, "yyyyMMdd")) OVER (
					PARTITION BY NMI ORDER BY FROM_DT
					) = to_date(TO_DT, "yyyyMMdd")
				THEN date_sub(to_date(TO_DT, "yyyyMMdd"), 1)
			ELSE to_date(TO_DT, "yyyyMMdd")
			END AS TO_DT
	FROM date_lst2
	WHERE datediff(to_date(TO_DT, "yyyyMMdd"), to_date(FROM_DT, "yyyyMMdd")) > 1
	),
	-- Timeline framing end --    
date_lst4
AS (
	SELECT espv.NMI,
		edev.MFG_SERIAL_NUM AS METER_NUM,
		ecpv.VALUE AS NMI_SUFFIX,
		cd.CHANNEL_ID,
		cd.IS_VIRTUAL_FLG,
		dl.FROM_DT,
		dl.TO_DT,
		ROW_NUMBER() OVER (
			PARTITION BY espv.NMI,
			edev.MFG_SERIAL_NUM,
			ecpv.VALUE ORDER BY cd.CHANNEL_ID
			) AS RNO,
		date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
	FROM EIP_SVC_PT_V1 AS espv
	INNER JOIN date_lst3 dl ON (espv.NMI = dl.NMI)
	INNER JOIN SVC_PT_DEVICE_REL_V2 AS spdl ON (
			spdl.NMI = espv.SVC_PT_ID
			AND CASE 
				WHEN cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 0)
				END > dl.FROM_DT
			AND cast(spdl.EFF_START_TIME AS DATE) < dl.TO_DT
			)
	INNER JOIN EIP_DEVICE_V3 AS edev ON spdl.DEVICE_ID = edev.DEVICE_ID
	INNER JOIN DEVICE_CHANNEL_REL_V5 dcr ON (
			edev.DEVICE_ID = dcr.DEVICE_ID
			AND CASE 
				WHEN cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 0)
				END > dl.FROM_DT
			AND cast(dcr.EFF_START_TIME AS DATE) < dl.TO_DT
			)
	INNER JOIN CHANNEL_DETAILS_V4 cd ON dcr.CHANNEL_ID = cd.CHANNEL_ID
		AND spdl.NMI = cd.SVC_PT_ID
		AND upper(trim(cd.IS_VIRTUAL_FLG)) = 'N'
	INNER JOIN EIP_CHANEEL_PARAM_V6 ecpv ON (
			cd.CHANNEL_ID = ecpv.CHANNEL_ID
			AND CASE 
				WHEN cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 0)
				END > dl.FROM_DT
			AND cast(ecpv.EFF_START_TIME AS DATE) < dl.TO_DT
			)
	)
SELECT NMI,
	METER_NUM,
	NMI_SUFFIX,
	CHANNEL_ID,
	IS_VIRTUAL_FLG,
	FROM_DT,
	TO_DT,
	ROW_INSERT_DTM
FROM date_lst4
WHERE RNO = 1 -- Handle duplicate channel id for same nmi, meter, suffix


-- COMMAND ----------

-- =============================================
-- Author:      Sainath 
-- Create Date: 23/08/2021
-- Description: Extracts EIP Standing Data for MHE feed
--              Time Slice based on NMI, Meter, Suffix, Channel
-- =============================================
WITH EIP_SVC_PT_V1
	-- Get Service Point Details
AS (
	SELECT hub1.HUB_NMI_HSH_KEY AS HUB_NMI_HSH_KEY,
		hub1.NMI AS SVC_PT_ID,
		hub.NMI AS NMI,
		SVC_PT.*
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_EIP_SVC_PT sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) SVC_PT
	INNER JOIN im${logicalenv}_rawvault.SALNK_NMI salnk ON SVC_PT.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI AS hub ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI AS hub1 ON salnk.HUB_NMI_HSH_KEY2 = hub1.HUB_NMI_HSH_KEY
	WHERE SVC_PT.rec_seq = 1
		AND SVC_PT.CHANGE_TYPE != 'D'
		-- AND hub.NMI in ('6305106980','6305002485','6305002543')
	),
SVC_PT_DEVICE_REL_V2
AS (
	-- Get Service Point to Device Relationship 
	SELECT hub_nmi.HUB_NMI_HSH_KEY,
		hub_nmi.NMI,
		hub_dvc.DEVICE_ID,
		hub_dvc.HUB_DEVICE_HSH_KEY,
		eip_spd_rel.*
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_DEVICE_HSH_KEY,
				sat.EFF_START_TIME ORDER BY extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_DEVICE_EIP_SVC_PT_DEVICE_REL sat
		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) eip_spd_rel
	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_DEVICE lnk ON eip_spd_rel.LNK_NMI_DEVICE_HSH_KEY = lnk.LNK_NMI_DEVICE_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hub_dvc ON lnk.HUB_DEVICE_HSH_KEY = hub_dvc.HUB_DEVICE_HSH_KEY
	WHERE rec_seq = 1
		AND eip_spd_rel.CHANGE_TYPE != 'D'
	),
EIP_DEVICE_V3
AS (
	-- Get Device Details
	SELECT hubdev.HUB_DEVICE_HSH_KEY,
		hubdev.DEVICE_ID,
		eip_device.MFG_SERIAL_NUM,
		eip_device.STATUS_CD
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.HUB_DEVICE_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.SAT_DEVICE_EIP_DEVICE_DETAILS sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) eip_device
	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hubdev ON eip_device.HUB_DEVICE_HSH_KEY = hubdev.HUB_DEVICE_HSH_KEY
	WHERE eip_device.rec_seq = 1
	),
CHANNEL_DETAILS_V4
AS (
	-- Get channel Details
	SELECT hub_chan.HUB_CHANNEL_HSH_KEY,
		hub_chan.CHANNEL_ID,
		hub_nmi.HUB_NMI_HSH_KEY,
		eip_channel.TYPE AS CHANNEL_TYPE,
		eip_channel.NAME AS CHANNEL_NAME,
		eip_channel.INTERVAL_LEN,
		eip_channel.IS_VIRTUAL_FLG,
		eip_channel.STATUS_CD,
		hub_nmi.NMI AS SVC_PT_ID,
		eip_channel.UDC_ID
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_NMI_CHANNEL_HSH_KEY ORDER BY extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_NMI_CHANNEL_EIP_CHANNEL_DETAILS sat
		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
			AND TYPE = 'Interval Data'
		) eip_channel
	INNER JOIN im${logicalenv}_rawvault.LNK_NMI_CHANNEL lnk ON eip_channel.LNK_NMI_CHANNEL_HSH_KEY = lnk.LNK_NMI_CHANNEL_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hub_chan ON lnk.HUB_CHANNEL_HSH_KEY = hub_chan.HUB_CHANNEL_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_NMI hub_nmi ON lnk.HUB_NMI_HSH_KEY = hub_nmi.HUB_NMI_HSH_KEY
	WHERE rec_seq = 1
		AND eip_channel.CHANGE_TYPE != 'D'
	),
DEVICE_CHANNEL_REL_V5
AS (
	-- Get Device to Channel Details
	SELECT hub_chan.HUB_CHANNEL_HSH_KEY,
		hub_chan.CHANNEL_ID,
		hub_dvc.DEVICE_ID,
		hub_dvc.HUB_DEVICE_HSH_KEY,
		eip_channel_rel.EFF_START_TIME,
		eip_channel_rel.EFF_END_TIME
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY LNK_DEVICE_CHANNEL_HSH_KEY,
				sat.EFF_START_TIME ORDER BY extract_date_time DESC
				) AS `rec_seq`
		FROM im${logicalenv}_rawvault.LSAT_DEVICE_CHANNEL_EIP_DEVICE_CHANNEL_REL sat
		WHERE date_format(EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
		) eip_channel_rel
	INNER JOIN im${logicalenv}_rawvault.LNK_DEVICE_CHANNEL lnk ON eip_channel_rel.LNK_DEVICE_CHANNEL_HSH_KEY = lnk.LNK_DEVICE_CHANNEL_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hub_chan ON lnk.HUB_CHANNEL_HSH_KEY = hub_chan.HUB_CHANNEL_HSH_KEY
	INNER JOIN im${logicalenv}_rawvault.HUB_DEVICE hub_dvc ON lnk.HUB_DEVICE_HSH_KEY = hub_dvc.HUB_DEVICE_HSH_KEY
	WHERE rec_seq = 1
		AND eip_channel_rel.CHANGE_TYPE != 'D'
	),
EIP_CHANEEL_PARAM_V6
AS (
	-- Get Channel Parameters
	SELECT hubchan.HUB_CHANNEL_HSH_KEY,
		hubchan.CHANNEL_ID,
		eip_channel_param.NAME AS NAME,
		eip_channel_param.VALUE AS VALUE,
		eip_channel_param.EFF_START_TIME AS EFF_START_TIME,
		nvl(eip_channel_param.EFF_END_TIME, to_timestamp('9999-12-31')) AS EFF_END_TIME
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.HUB_CHANNEL_HSH_KEY,
				sat.ID,
                sat.NAME
				ORDER BY EXTRACT_DATE_TIME DESC
				) AS `rec_seq`
		--used record version column from the data in sorting to get the latest records
		FROM im${logicalenv}_rawvault.SAT_CHANNEL_EIP_CHANNEL_PARAM sat
		WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") <= date_format('${run_date}', "yyyyMMdd")
			AND upper(trim(sat.NAME)) = 'DATA STREAM SUFFIX'
			-- Take only Data Stream Suffix Channel which is required for MHE
		) eip_channel_param
	INNER JOIN im${logicalenv}_rawvault.HUB_CHANNEL hubchan ON eip_channel_param.HUB_CHANNEL_HSH_KEY = hubchan.HUB_CHANNEL_HSH_KEY
	WHERE eip_channel_param.rec_seq = 1
		AND eip_channel_param.CHANGE_TYPE != 'D'
	),
date_lst1
AS (
	SELECT DISTINCT espv.NMI,
		stack(99, cast(spdl.EFF_START_TIME AS DATE), CASE 
				WHEN cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
				END, cast(dcr.EFF_START_TIME AS DATE), CASE 
				WHEN cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
				END, cast(ecpv.EFF_START_TIME AS DATE), CASE 
				WHEN cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 1)
				END) AS DT_LST
	FROM EIP_SVC_PT_V1 AS espv
	INNER JOIN SVC_PT_DEVICE_REL_V2 AS spdl ON spdl.NMI = espv.SVC_PT_ID
	INNER JOIN EIP_DEVICE_V3 AS edev ON spdl.DEVICE_ID = edev.DEVICE_ID
	INNER JOIN DEVICE_CHANNEL_REL_V5 dcr ON edev.DEVICE_ID = dcr.DEVICE_ID
	INNER JOIN CHANNEL_DETAILS_V4 cd ON dcr.CHANNEL_ID = cd.CHANNEL_ID
		AND spdl.NMI = cd.SVC_PT_ID
	INNER JOIN EIP_CHANEEL_PARAM_V6 ecpv ON cd.CHANNEL_ID = ecpv.CHANNEL_ID
		WHERE espv.NMI IN (
			'6305996642', '6305499448'
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
		to_date(FROM_DT, "yyyyMMdd") AS FROM_DT,
		CASE 
			WHEN lead(to_date(FROM_DT, "yyyyMMdd")) OVER (
					PARTITION BY NMI ORDER BY FROM_DT
					) = to_date(TO_DT, "yyyyMMdd")
				THEN date_sub(to_date(TO_DT, "yyyyMMdd"), 1)
			ELSE to_date(TO_DT, "yyyyMMdd")
			END AS TO_DT
	FROM date_lst2
	WHERE datediff(to_date(TO_DT, "yyyyMMdd"), to_date(FROM_DT, "yyyyMMdd")) > 1
	),
	-- Timeline framing end --    
date_lst4
AS (
	SELECT espv.NMI,
		edev.MFG_SERIAL_NUM AS METER_NUM,
		ecpv.VALUE AS NMI_SUFFIX,
		cd.CHANNEL_ID,
		cd.IS_VIRTUAL_FLG,
		dl.FROM_DT,
		dl.TO_DT,
		ROW_NUMBER() OVER (
			PARTITION BY espv.NMI,
			edev.MFG_SERIAL_NUM,
			ecpv.VALUE,
            dl.FROM_DT
            ORDER BY cd.CHANNEL_ID
			) AS RNO,
		date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
	FROM EIP_SVC_PT_V1 AS espv
	INNER JOIN date_lst3 dl ON (espv.NMI = dl.NMI)
	INNER JOIN SVC_PT_DEVICE_REL_V2 AS spdl ON (
			spdl.NMI = espv.SVC_PT_ID
			AND CASE 
				WHEN cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(spdl.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 0)
				END > dl.FROM_DT
			AND cast(spdl.EFF_START_TIME AS DATE) < dl.TO_DT
			)
	INNER JOIN EIP_DEVICE_V3 AS edev ON spdl.DEVICE_ID = edev.DEVICE_ID
	INNER JOIN DEVICE_CHANNEL_REL_V5 dcr ON (
			edev.DEVICE_ID = dcr.DEVICE_ID
			AND CASE 
				WHEN cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(dcr.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 0)
				END > dl.FROM_DT
			AND cast(dcr.EFF_START_TIME AS DATE) < dl.TO_DT
			)
	INNER JOIN CHANNEL_DETAILS_V4 cd ON dcr.CHANNEL_ID = cd.CHANNEL_ID
		AND spdl.NMI = cd.SVC_PT_ID
		AND upper(trim(cd.IS_VIRTUAL_FLG)) = 'N'
	INNER JOIN EIP_CHANEEL_PARAM_V6 ecpv ON (
			cd.CHANNEL_ID = ecpv.CHANNEL_ID
			AND CASE 
				WHEN cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE) = cast('9999-12-31' AS DATE)
					THEN cast('9999-12-31' AS DATE)
				ELSE date_sub(cast(nvl(ecpv.EFF_END_TIME, to_timestamp('9999-12-31')) AS DATE), 0)
				END > dl.FROM_DT
			AND cast(ecpv.EFF_START_TIME AS DATE) < dl.TO_DT
			)
	)
SELECT NMI,
	METER_NUM,
	NMI_SUFFIX,
	CHANNEL_ID,
	IS_VIRTUAL_FLG,
	FROM_DT,
	TO_DT,
	ROW_INSERT_DTM
FROM date_lst4
WHERE RNO = 1 -- Handle duplicate channel id for same nmi, meter, suffix

