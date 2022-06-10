-- Databricks notebook source
WITH EANL_V1
AS (
	SELECT EANL.*,
		salnk.HUB_NMI_HSH_KEY1
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.SALNK_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.LSAT_NMI_CIS_EANL sat
			-- WHERE date_format(sat.EXTRACT_DATE_TIME, "yyyyMMdd") < date_format('2021-09-22', "yyyyMMdd")
		) EANL
	JOIN imprd001_rawvault.SALNK_NMI salnk ON EANL.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
		AND EANL.rec_seq = 1
		AND trim(EANL.NODISCONCT) = 'LS'
		AND EANL.CHANGE_TYPE <> 'D'
	),
EUIINSTLN_V2
AS (
	SELECT salnk.HUB_NMI_HSH_KEY1,
		salnk.HUB_NMI_HSH_KEY2
	FROM imprd001_rawvault.SALNK_NMI salnk
	JOIN imprd001_rawvault.LSAT_NMI_CIS_EUIINSTLN sat ON sat.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
	WHERE sat.CHANGE_TYPE <> 'D'
	),
EUITRANS_V3
AS (
	SELECT salnk.HUB_NMI_HSH_KEY1,
		salnk.HUB_NMI_HSH_KEY2,
		hub.NMI
	FROM imprd001_rawvault.LSAT_NMI_CIS_EUITRANS sat
	JOIN imprd001_rawvault.SALNK_NMI salnk ON sat.SALNK_NMI_HSH_KEY = salnk.SALNK_NMI_HSH_KEY
	JOIN imprd001_rawvault.HUB_NMI hub ON salnk.HUB_NMI_HSH_KEY1 = hub.HUB_NMI_HSH_KEY
	WHERE length(trim(sat.NMI_CHECKSUM)) > 0
		AND sat.CHANGE_TYPE <> 'D'
	),
ZIIDTT_NMI_STAT_V4
AS (
	SELECT *
	FROM (
		SELECT sat.*,
			ROW_NUMBER() OVER (
				PARTITION BY sat.HUB_NMI_HSH_KEY ORDER BY sat.extract_date_time DESC
				) AS `rec_seq`
		FROM imprd001_rawvault.SAT_NMI_CIS_ZIIDTT_NMI_STAT sat
		WHERE sat.TO_DT = '99991231'
			AND length(trim(sat.NMI_CHECKSUM)) > 0
		) a
	WHERE rec_seq = 1
	AND TRIM(NMI_STATUS) IN ('A', 'D', 'G')
	),
EASTL_V5
AS (
	SELECT lnk.HUB_NMI_HSH_KEY,
		lnk.HUB_EQUIPMENT_HSH_KEY
	FROM imprd001_rawvault.LNK_NMI_EQUIPMENT lnk
	JOIN imprd001_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTL sat ON sat.LNK_NMI_EQUIPMENT_HSH_KEY = lnk.LNK_NMI_EQUIPMENT_HSH_KEY
	WHERE sat.BIS = '99991231'
		AND sat.CHANGE_TYPE <> 'D'
	),
EGERH_V6
AS (
	SELECT salnk.HUB_EQUIPMENT_HSH_KEY2,
		salnk.HUB_EQUIPMENT_HSH_KEY1
	FROM imprd001_rawvault.LSAT_NMI_EQUIPMENT2_CIS_EGERH sat
	JOIN imprd001_rawvault.LNK_NMI_EQUIPMENT2 salnk ON sat.LNK_NMI_EQUIPMENT2_HSH_KEY = salnk.LNK_NMI_EQUIPMENT2_HSH_KEY
	WHERE sat.KOMBINAT = 'Z'
		AND sat.BIS = '99991231'
		AND sat.CHANGE_TYPE <> 'D'
	),
EQUI_V7
AS (
	SELECT hub.DEVICE_ID AS SERNR,
		lnk.HUB_EQUIPMENT_HSH_KEY
	FROM imprd001_rawvault.LSAT_EQUIPMENT_DEVICE_MATERIAL_CIS_EQUI sat
	JOIN imprd001_rawvault.LNK_EQUIPMENT_DEVICE_MATERIAL lnk ON sat.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY = lnk.LNK_EQUIPMENT_DEVICE_MATERIAL_HSH_KEY
	JOIN imprd001_rawvault.HUB_DEVICE hub ON lnk.HUB_DEVICE_HSH_KEY = hub.HUB_DEVICE_HSH_KEY
	WHERE sat.CHANGE_TYPE <> 'D'
	)
SELECT DISTINCT EUITRANS_V3.NMI,
	EANL_V1.NODISCONCT AS LIFE_SUPPORT_FLAG,
	ZIIDTT_NMI_STAT_V4.NMI_STATUS,
	TRIM(EQUI_V7.SERNR) AS METER_NUM,
	date_format(from_utc_timestamp(current_timestamp(), 'GMT+10'), 'yyyy-MM-dd HH:mm:ss') AS ROW_INSERT_DTM
FROM EANL_V1
JOIN EUIINSTLN_V2 ON EANL_V1.HUB_NMI_HSH_KEY1 = EUIINSTLN_V2.HUB_NMI_HSH_KEY1
JOIN EUITRANS_V3 ON EUIINSTLN_V2.HUB_NMI_HSH_KEY2 = EUITRANS_V3.HUB_NMI_HSH_KEY2
JOIN ZIIDTT_NMI_STAT_V4 ON EUITRANS_V3.HUB_NMI_HSH_KEY1 = ZIIDTT_NMI_STAT_V4.HUB_NMI_HSH_KEY
LEFT OUTER JOIN EASTL_V5 ON EANL_V1.HUB_NMI_HSH_KEY1 = EASTL_V5.HUB_NMI_HSH_KEY
JOIN EGERH_V6 ON EASTL_V5.HUB_EQUIPMENT_HSH_KEY = EGERH_V6.HUB_EQUIPMENT_HSH_KEY2
JOIN EQUI_V7 ON EGERH_V6.HUB_EQUIPMENT_HSH_KEY1 = EQUI_V7.HUB_EQUIPMENT_HSH_KEY;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import concat, col, lit
-- MAGIC 
-- MAGIC 
-- MAGIC df = spark.sql("show tables in {}".format("imprd001_rawvault"))
-- MAGIC df.select(concat(col("database"), col("tableName")))
-- MAGIC df.show()

-- COMMAND ----------

show tables in imprd001_rawvault like '*ESERVICE*'

-- COMMAND ----------

select distinct change_type from imprd001_rawvault.sat_address_cis_adr2_cv

-- COMMAND ----------

select * from imprd001_rawvault.sat_address_cis_adr2_cv where change_type = 'D'

-- COMMAND ----------

select * from imprd001_rawvault.sat_address_cis_adr2 where HUB_ADDRESS_HSH_KEY = '00f1e87e5ab21c940c2875500296a2a7a5d4de52'

-- COMMAND ----------

select * from imprd001_rawvault.sat_address_cis_adr2_cv where change_type = 'A' limit 10

-- COMMAND ----------

select * from imprd001_rawvault.sat_address_cis_adr2 where HUB_ADDRESS_HSH_KEY = '01033e73d532f4f2eaacab7625ece1481847b298'

-- COMMAND ----------

select * from imprd001_rawvault.sat_address_cis_adr2_cv where HUB_ADDRESS_HSH_KEY = '01033e73d532f4f2eaacab7625ece1481847b298'

-- COMMAND ----------

select 'ADR2', count(*) from imprd001_rawvault.sat_address_cis_adr2_cv where change_type <> 'D' union all
select 'BUT000', count(*) from imprd001_rawvault.lsat_nmi_business_partner_cis_but000_cv where change_type <> 'D' union all
select 'ESERVICE', count(*) from imprd001_rawvault.lsat_nmi_participant_cis_eservice_cv where change_type <> 'D' union all
select 'BUT021_FS', count(*) from imprd001_rawvault.lsat_business_partner_address_cis_but021_fs_cv where change_type <> 'D'
order by 1

-- COMMAND ----------

-- Databricks notebook source
select 'ADR2' as TableName, count(*) as Total_Record_Count from imprd001_rawvault.SAT_ADDRESS_CIS_ADR2_CV where change_type <> 'D' union all
select 'ADR6', count(*) from imprd001_rawvault.SAT_ADDRESS_CIS_ADR6_CV where change_type <> 'D' union all
select 'ADR8', count(*) from imprd001_rawvault.SAT_ADDRESS_CIS_ADR8_CV where change_type <> 'D' union all
select 'ADRC', count(*) from imprd001_rawvault.SAT_ADDRESS_CIS_ADRC_CV where change_type <> 'D' union all
select 'ADRCITY', count(*) from imprd001_rawvault.SAT_REF_ADRCITY_CIS_ADRCITY_CV where change_type <> 'D' union all
select 'ADRCITYT', count(*) from imprd001_rawvault.SAT_REF_ADRCITY_CIS_ADRCITYT_CV where change_type <> 'D' union all
select 'ADRPSTCODE', count(*) from imprd001_rawvault.SAT_REF_POSTCODE_CIS_ADRPSTCODE_CV where change_type <> 'D' union all
select 'ADRSTREET', count(*) from imprd001_rawvault.SAT_REF_STREET_CIS_ADRSTREET_CV where change_type <> 'D' union all
select 'ADRSTREETT', count(*) from imprd001_rawvault.SAT_REF_STREET_CIS_ADRSTREETT_CV where change_type <> 'D' union all
select 'ADRSTRTMRU', count(*) from imprd001_rawvault.SAT_REF_ADRSTRTMRU_CIS_ADRSTRTMRU_CV where change_type <> 'D' union all
select 'AFIH', count(*) from imprd001_rawvault.LSAT_SRVORDER_MATERIAL_EQUIPMENT_DEVICE_BUSINESS_PARTNER_CIS_AFIH_CV where change_type <> 'D' union all
select 'AFKO', count(*) from imprd001_rawvault.LSAT_SRVORDER_CIS_AFKO_CV where change_type <> 'D' union all
select 'AFVC', count(*) from imprd001_rawvault.LSAT_SRVORDER_CIS_AFVC_CV where change_type <> 'D' union all
select 'AFVV', count(*) from imprd001_rawvault.SAT_SRVORDER_CIS_AFVV_CV where change_type <> 'D' union all
select 'AUFK', count(*) from imprd001_rawvault.LSAT_SRVORDER_SWITCHDOC_NMI_CIS_AUFK_CV where change_type <> 'D' union all
-- select 'AUSP', count(*) from imprd001_rawvault.AUSP_HIST_VW_CV where change_type <> 'D' union all
select 'BCONT', count(*) from imprd001_rawvault.SAT_BUSINESS_PARTNER_CIS_BCONT_CV where change_type <> 'D' union all
select 'BGMKOBJ', count(*) from imprd001_rawvault.SAT_EQUIPMENT_CIS_BGMKOBJ_CV where change_type <> 'D' union all
select 'BUT000', count(*) from imprd001_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_BUT000_CV where change_type <> 'D' union all
select 'BUT021_FS', count(*) from imprd001_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS_CV where change_type <> 'D' union all
select 'COMMON_REFERENCE', count(*) from imprd001_rawvault.SAT_REF_COMMON_REFERENCE_CIS_COMMON_REFERENCE_CV where change_type <> 'D' union all
select 'CRHD', count(*) from imprd001_rawvault.SAT_REF_WORKCENTER_CIS_CRHD_CV where change_type <> 'D' union all
select 'CRTX', count(*) from imprd001_rawvault.SAT_REF_WORKCENTER_CIS_CRTX_CV where change_type <> 'D' union all
select 'DBERCHZ1', count(*) from imprd001_rawvault.SAT_BILLING_CIS_DBERCHZ1_CV where change_type <> 'D' union all
select 'DBERCHZ2', count(*) from imprd001_rawvault.LSAT_EQUIPMENT_MATERIAL_DEVICE_MTRREAD_BILLING_CIS_DBERCHZ2_CV where change_type <> 'D' union all
select 'DBERCHZ3', count(*) from imprd001_rawvault.SAT_BILLING_CIS_DBERCHZ3_CV where change_type <> 'D' union all
select 'DBERCHZ4', count(*) from imprd001_rawvault.SAT_BILLING_CIS_DBERCHZ4_CV where change_type <> 'D' union all
select 'DD07L', count(*) from imprd001_rawvault.SAT_REF_DOMAIN_CIS_DD07L_CV where change_type <> 'D' union all
select 'DD07T', count(*) from imprd001_rawvault.SAT_REF_DOMAIN_CIS_DD07T_CV where change_type <> 'D' union all
select 'EABL', count(*) from imprd001_rawvault.LSAT_EQUIPMENT_MTRREAD_DEVICE_CIS_EABL_CV where change_type <> 'D' union all
select 'EABLG', count(*) from imprd001_rawvault.LSAT_NMI_MTRREAD_CIS_EABLG_CV where change_type <> 'D' union all
select 'EANL', count(*) from imprd001_rawvault.LSAT_NMI_CIS_EANL_CV where change_type <> 'D' union all
select 'EANLH', count(*) from imprd001_rawvault.SAT_NMI_CIS_EANLH_CV where change_type <> 'D' union all
select 'EASTE', count(*) from imprd001_rawvault.LSAT_DEVICE_EQUIPMENT_CIS_EASTE_CV where change_type <> 'D' union all
select 'EASTL', count(*) from imprd001_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTL_CV where change_type <> 'D' union all
select 'EASTS', count(*) from imprd001_rawvault.LSAT_NMI_EQUIPMENT_CIS_EASTS_CV where change_type <> 'D' union all
select 'EBAN', count(*) from imprd001_rawvault.SAT_MATERIAL_CIS_EBAN_CV where change_type <> 'D' union all
select 'EDISCACT', count(*) from imprd001_rawvault.SAT_DISCONNECTION_CIS_EDISCACT_CV where change_type <> 'D' union all
select 'EDISCOBJ', count(*) from imprd001_rawvault.LSAT_DISCONNECTION_NMI_EQUIPMENT_CIS_EDISCOBJ_CV where change_type <> 'D' union all
select 'EDISCPOS', count(*) from imprd001_rawvault.SAT_DISCONNECTION_CIS_EDISCPOS_CV where change_type <> 'D' union all
select 'EDISCTYPET', count(*) from imprd001_rawvault.SAT_REF_DISCON_TYPE_CIS_EDISCTYPET_CV where change_type <> 'D' union all
select 'EDSAPPL', count(*) from imprd001_rawvault.SAT_REF_IDOC_STRUCT_CIS_EDSAPPL_CV where change_type <> 'D' union all
select 'EEIN', count(*) from imprd001_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_EEIN_CV where change_type <> 'D' union all
select 'EEINV', count(*) from imprd001_rawvault.LSAT_NMI_CIS_EEINV_CV where change_type <> 'D' union all
select 'EGERH', count(*) from imprd001_rawvault.LSAT_NMI_EQUIPMENT2_CIS_EGERH_CV where change_type <> 'D' union all
select 'EGERR', count(*) from imprd001_rawvault.LSAT_DEVICE_EQUIPMENT2_CIS_EGERR_CV where change_type <> 'D' union all
select 'EGERS', count(*) from imprd001_rawvault.SAT_EQUIPMENT_CIS_EGERS_CV where change_type <> 'D' union all
select 'EGPLTX', count(*) from imprd001_rawvault.SAT_NMI_CIS_EGPLTX_CV where change_type <> 'D' union all
select 'EGRIDH', count(*) from imprd001_rawvault.SAT_REF_GRID_ALLOC_CIS_EGRIDH_CV where change_type <> 'D' union all
select 'EHAUISU', count(*) from imprd001_rawvault.SAT_NMI_CIS_EHAUISU_CV where change_type <> 'D' union all
select 'EIDESWTDOC', count(*) from imprd001_rawvault.LSAT_NMI_SWITCHDOC_CIS_EIDESWITDOC_CV where change_type <> 'D' union all
select 'EIDESWTDOCATTR', count(*) from imprd001_rawvault.SAT_SWITCHSTEP_CIS_EIDESWTDOCATTR _CV where change_type <> 'D' union all
select 'EIDESWTDOCSTEP', count(*) from imprd001_rawvault.LSAT_SWITCHDOC_SWITCHSTEP_CIS_EIDESWTDOCSTEP_CV where change_type <> 'D' union all
select 'ELWEG', count(*) from imprd001_rawvault.SAT_EQUIPMENT_CIS_ELWEG_CV where change_type <> 'D' union all
select 'EMMA_CASE', count(*) from imprd001_rawvault.SAT_BPEM_CIS_EMMA_CASE _CV where change_type <> 'D' union all
select 'ENOTE', count(*) from imprd001_rawvault.SAT_NMI_CIS_ENOTE_CV where change_type <> 'D' union all
select 'EQKT', count(*) from imprd001_rawvault.SAT_EQUIPMENT_CIS_EQKT_CV where change_type <> 'D' union all
select 'EQUI', count(*) from imprd001_rawvault.LSAT_EQUIPMENT_DEVICE_MATERIAL_CIS_EQUI_CV where change_type <> 'D' union all
select 'EQUZ', count(*) from imprd001_rawvault.LSAT_EQUIPMENT_MATERIAL_CIS_EQUZ_CV where change_type <> 'D' union all
select 'ERCH', count(*) from imprd001_rawvault.LSAT_NMI_BILLING_CIS_ERCH_CV where change_type <> 'D' union all
select 'ESERVICE', count(*) from imprd001_rawvault.LSAT_NMI_PARTICIPANT_CIS_ESERVICE_CV where change_type <> 'D' union all
select 'ESERVPROV', count(*) from imprd001_rawvault.SAT_PARTICIPANT_CIS_ESERVPROV_CV where change_type <> 'D' union all
select 'ESERVPROVP', count(*) from imprd001_rawvault.SAT_PARTICIPANT_CIS_ESERVPROVP_CV where change_type <> 'D' union all
select 'ESERVPROVT', count(*) from imprd001_rawvault.SAT_PARTICIPANT_CIS_ESERVPROVT_CV where change_type <> 'D' union all
select 'ETDZ', count(*) from imprd001_rawvault.LSAT_EQUIPMENT_CIS_ETDZ_CV where change_type <> 'D' union all
select 'ETTA', count(*) from imprd001_rawvault.SAT_REF_RATE_CAT_CIS_ETTA_CV where change_type <> 'D' union all
select 'ETTAT', count(*) from imprd001_rawvault.SAT_REF_RATE_CAT_CIS_ETTAT_CV where change_type <> 'D' union all
select 'ETYP', count(*) from imprd001_rawvault.SAT_MATERIAL_CIS_ETYP_CV where change_type <> 'D' union all
select 'EUIGRID', count(*) from imprd001_rawvault.SAT_NMI_CIS_EUIGRID_CV where change_type <> 'D' union all
select 'EUIHEAD', count(*) from imprd001_rawvault.SAT_NMI_CIS_EUIHEAD_CV where change_type <> 'D' union all
select 'EUIHEADT', count(*) from imprd001_rawvault.SAT_NMI_CIS_EUIHEADT_CV where change_type <> 'D' union all
select 'EUIINSTLN', count(*) from imprd001_rawvault.LSAT_NMI_CIS_EUIINSTLN_CV where change_type <> 'D' union all
select 'EUITRANS', count(*) from imprd001_rawvault.LSAT_NMI_CIS_EUITRANS_CV where change_type <> 'D' union all
select 'EVBS', count(*) from imprd001_rawvault.LSAT_NMI_SUBSTATION_CIS_EVBS_CV where change_type <> 'D' union all
select 'EVER', count(*) from imprd001_rawvault.LSAT_NMI_CIS_EVER_CV where change_type <> 'D' union all
select 'EWIK', count(*) from imprd001_rawvault.SAT_REF_WINDING_GRP_CIS_EWIK_CV where change_type <> 'D' union all
select 'EZUG', count(*) from imprd001_rawvault.LSAT_EQUIPMENT_COMMS_CIS_EZUG_CV where change_type <> 'D' union all
select 'EZWG', count(*) from imprd001_rawvault.SAT_REF_REGISTER_GRP_CIS_EZWG_CV where change_type <> 'D' union all
select 'EZWG_HEAD', count(*) from imprd001_rawvault.SAT_REF_REGISTER_GRP_CIS_EZWG_HEAD_CV where change_type <> 'D' union all
select 'FKKVK', count(*) from imprd001_rawvault.SAT_NMI_CIS_FKKVK_CV where change_type <> 'D' union all
select 'FKKVKP', count(*) from imprd001_rawvault.LSAT_NMI_BUSINESS_PARTNER_CIS_FKKVKP_CV where change_type <> 'D' union all
select 'IFLOT', count(*) from imprd001_rawvault.SAT_NMI_CIS_IFLOT_CV where change_type <> 'D' union all
select 'IFLOTX', count(*) from imprd001_rawvault.SAT_NMI_CIS_IFLOTX_CV where change_type <> 'D' union all
select 'IHPA', count(*) from imprd001_rawvault.LSAT_SRVORDER_BUSINESS_PARTNER_ADDRESS_CIS_IHPA_CV where change_type <> 'D' union all
select 'ILOA', count(*) from imprd001_rawvault.LSAT_NMI_ADDRESS_CIS_ILOA_CV where change_type <> 'D' union all
select 'IMPTT', count(*) from imprd001_rawvault.LSAT_EQUIPMENT_MEASURING_POINT_CIS_IMPTT_CV where change_type <> 'D' union all
select 'IMRG', count(*) from imprd001_rawvault.SAT_MEASURING_POINT_CIS_IMRG_CV where change_type <> 'D' union all
select 'JEST', count(*) from imprd001_rawvault.SAT_SRVORDER_CIS_JEST_CV where change_type <> 'D' union all
select 'JSTO', count(*) from imprd001_rawvault.SAT_SRVORDER_CIS_JSTO_CV where change_type <> 'D' union all
-- select 'KSSK', count(*) from imprd001_rawvault.KSSK_HIST_VW_CV where change_type <> 'D' union all
select 'MAKT', count(*) from imprd001_rawvault.SAT_MATERIAL_CIS_MAKT_CV where change_type <> 'D' union all
select 'MARA', count(*) from imprd001_rawvault.SAT_MATERIAL_CIS_MARA_CV where change_type <> 'D' union all
select 'MARC', count(*) from imprd001_rawvault.SAT_MATERIAL_CIS_MARC_CV where change_type <> 'D' union all
select 'MARD', count(*) from imprd001_rawvault.SAT_MATERIAL_CIS_MARD_CV where change_type <> 'D' union all
select 'MCH1', count(*) from imprd001_rawvault.SAT_MATERIAL_CIS_MCH1_CV where change_type <> 'D' union all
select 'MCHA', count(*) from imprd001_rawvault.SAT_MATERIAL_CIS_MCHA_CV where change_type <> 'D' union all
select 'MKPF', count(*) from imprd001_rawvault.SAT_PURCHASE_CIS_MKPF_CV where change_type <> 'D' union all
select 'MSEG', count(*) from imprd001_rawvault.LSAT_PURCHASE_MATERIAL_CIS_MSEG_CV where change_type <> 'D' union all
select 'OBJK', count(*) from imprd001_rawvault.LSAT_SRVORDER_MATERIAL_EQUIPMENT_DEVICE_NOTIFICATION_CIS_OBJK_CV where change_type <> 'D' union all
select 'QMEL', count(*) from imprd001_rawvault.LSAT_NOTIFICATION_DEVICE_MATERIAL_SRVORDER_CIS_QMEL_CV where change_type <> 'D' union all
select 'RESB', count(*) from imprd001_rawvault.LSAT_SRVORDER_MATERIAL_CIS_RESB_CV where change_type <> 'D' union all
select 'SWWUSERWI', count(*) from imprd001_rawvault.SAT_WORKITEM_CIS_SWWUSERWI_CV where change_type <> 'D' union all
select 'SWWWIHEAD', count(*) from imprd001_rawvault.SAT_WORKITEM_CIS_SWWWIHEAD_CV where change_type <> 'D' union all
select 'T003O', count(*) from imprd001_rawvault.SAT_REF_ORDER_TYP_CIS_T003O_CV where change_type <> 'D' union all
select 'T003P', count(*) from imprd001_rawvault.SAT_REF_ORDER_TYP_CIS_T003P_CV where change_type <> 'D' union all
select 'TE418', count(*) from imprd001_rawvault.SAT_MRU_CIS_TE418_CV where change_type <> 'D' union all
select 'TE422', count(*) from imprd001_rawvault.SAT_MRU_CIS_TE422_CV where change_type <> 'D' union all
select 'TFACS', count(*) from imprd001_rawvault.SAT_REF_CALENDAR_CIS_TFACS_CV where change_type <> 'D' union all
select 'ZICATT_CRTVAL', count(*) from imprd001_rawvault.SAT_REF_CROSSREF_CIS_ZICATT_CRTVAL_CV where change_type <> 'D' union all
select 'ZIIDTT_DATASTRM', count(*) from imprd001_rawvault.LSAT_NMI_DEVICE_CIS_ZIIDTT_DATASTRM_CV where change_type <> 'D' union all
select 'ZIIDTT_DLF', count(*) from imprd001_rawvault.SAT_NMI_CIS_ZIIDTT_DLF_CV where change_type <> 'D' union all
select 'ZIIDTT_LIFESUP', count(*) from imprd001_rawvault.LSAT_NMI_ADDRESS_CIS_ZIIDTT_LIFSUP_CV where change_type <> 'D' union all
select 'ZIIDTT_MDNO_ITEM', count(*) from imprd001_rawvault.LSAT_NMI_DEVICE_BILLING_CIS_ZIIDTT_MDNO_ITEM_CV where change_type <> 'D' union all
select 'ZIIDTT_MR_EXCH', count(*) from imprd001_rawvault.SAT_NMI_CIS_ZIIDTT_MR_EXCH_CV where change_type <> 'D' union all
select 'ZIIDTT_MRDATA', count(*) from imprd001_rawvault.LSAT_NMI_EQUIPMENT_DEVICE_SRVORDER_MR_CIS_ZIIDTT_MRDATA_CV where change_type <> 'D' union all
select 'ZIIDTT_MTRCFGHDR', count(*) from imprd001_rawvault.SAT_REF_MTR_CONFIG_CIS_ZIIDTT_MTRCFGHDR_CV where change_type <> 'D' union all
select 'ZIIDTT_MTRCFGITM', count(*) from imprd001_rawvault.SAT_REF_MTR_CONFIG_CIS_ZIIDTT_MTRCFGITM_CV where change_type <> 'D' union all
select 'ZIIDTT_NMI_CLASS', count(*) from imprd001_rawvault.SAT_NMI_CIS_ZIIDTT_NMI_CLASS_CV where change_type <> 'D' union all
select 'ZIIDTT_NMI_STAT', count(*) from imprd001_rawvault.SAT_NMI_CIS_ZIIDTT_NMI_STAT_CV where change_type <> 'D' union all
select 'ZIIDTT_SUB_TNI', count(*) from imprd001_rawvault.SAT_REF_SUBSTATION_CIS_ZIIDTT_SUB_TNI_CV where change_type <> 'D' union all
select 'ZIPMTT_SYNC_TS', count(*) from imprd001_rawvault.SAT_EQUIPMENT_CIS_ZIPMTT_SYNC_TS_CV where change_type <> 'D' union all
select 'ZIPMTTWORKCENTER', count(*) from imprd001_rawvault.SAT_REF_ZWORKCENTRE_CIS_ZIPMTTWORKCENTER_CV where change_type <> 'D' union all
select 'EZUZ', count(*) from imprd001_rawvault.LSAT_EQUIPMENT_REGISTER_CIS_EZUZ_CV where change_type <> 'D' union all
select 'EIDESWTACTIVITYT', count(*) from imprd001_rawvault.SAT_REF_EIDESWTACTIVITYT_CIS_EIDESWTACTIVITYT_CV where change_type <> 'D' union all
select 'EIDESWTDOCREF', count(*) from imprd001_rawvault.LSAT_SWITCHSTEP_DOC_CIS_EIDESWTDOCREF_CV where change_type <> 'D' union all
select 'ZIIDTT_STAGEIDOC', count(*) from imprd001_rawvault.SAT_DOC_CIS_ZIIDTT_STAGEIDOC _CV where change_type <> 'D' union all
select 'EIDESWTTYPEST', count(*) from imprd001_rawvault.SAT_REF_EIDESWTTYPEST_CIS_EIDESWTTYPEST_CV where change_type <> 'D' union all
select 'EIDESWTSTATUST', count(*) from imprd001_rawvault.SAT_REF_EIDESWTSTATUST_CIS_EIDESWTSTATUST where change_type <> 'D'
order by 1

-- COMMAND ----------

describe table EXTENDED  im${logicalenv}_rawvault.HUB_NMI

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def droptable(tbl_name):
-- MAGIC #     dbutils = DBUtils(spark)
-- MAGIC     env='imprd001'
-- MAGIC     spark.sql(f"drop table if exists {env}_rawvault.{tbl_name}")
-- MAGIC     for i in dbutils.fs.ls(f'/mnt/imrv/deltalake/{env}/rawvault/{tbl_name}'):
-- MAGIC         print(f'/mnt/imrv/deltalake/{env}/rawvault/{tbl_name}/{i.name}')
-- MAGIC         dbutils.fs.rm(f'/mnt/imrv/deltalake/{env}/rawvault/{tbl_name}/{i.name}',True)
-- MAGIC     dbutils.fs.rm(f'/mnt/imrv/deltalake/{env}/rawvault/{tbl_name}/',True)
-- MAGIC     print("dropped table ", tbl_name)
-- MAGIC     
-- MAGIC # droptable('SAT_NMI_SDME_NMA_SITE_INFO') ;
-- MAGIC # droptable('SAT_NMI_SDME_NMA_SITE_INFO_GEOM') ;
-- MAGIC # droptable('SAT_EQUIPMENT_SDME_EO_LIGHT') ;
-- MAGIC # droptable('SAT_EQUIPMENT_SDME_EO_LIGHT_GEOM') ;
-- MAGIC # droptable('SAT_EQUIPMENT_SDME_NMA_PUBLIC_LIGHT_LOG') ;
-- MAGIC # droptable('SAT_EQUIPMENT_SDME_NMA_PUBLIC_LIGHT_LOG_GEOM') ;
-- MAGIC # droptable('SAT_SPECIFICATION_SDE_LIGHT_SPEC') ;
-- MAGIC # droptable('LSAT_NMI_EQUIPMENT_AUTHORITY_SDME_INT_NMA_NMI_INFO_EO_LIGHT') ;
-- MAGIC # droptable('LSAT_NMI_AUTHORITY_SDME_NMA_NMI_INFO') ;
-- MAGIC # droptable('SAT_AUTHORITY_SDME_NMA_OTHER_AUTHORITY') ;
-- MAGIC # droptable('SAT_AUTHORITY_SDME_NMA_OTHER_AUTHORITY_GEOM') ;
-- MAGIC # droptable('LSAT_EQUIPMENT_SPECIFICATION_SDME_INT_HETERO145027') ;

-- COMMAND ----------

select CAST(to_date(date_add('${run_date}',-1)) AS timestamp) + MAKE_INTERVAL(0, 0, 0, 0, -4, 0, 0), 
CAST(to_date('${run_date}') AS timestamp) + MAKE_INTERVAL(0, 0, 0, 0, -4, 0, 0),
date_format(date_add('${run_date}',-4),"yyyyMMdd") 
