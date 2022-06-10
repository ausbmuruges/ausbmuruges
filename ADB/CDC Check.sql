-- Databricks notebook source
CREATE WIDGET TEXT logicalenv DEFAULT "prd001";

-- COMMAND ----------

select max(extract_date_time) from im${logicalenv}_rawvault.SAT_ADDRESS_CIS_ADR2

-- COMMAND ----------

select max(extract_date_time) from im${logicalenv}_rawvault.LSAT_BUSINESS_PARTNER_ADDRESS_CIS_BUT021_FS
