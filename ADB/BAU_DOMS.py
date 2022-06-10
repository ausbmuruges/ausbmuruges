# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC SELECT `src`.hub_doms_alarm_hsh_key, `hub`.alarm_id, `src`.extract_date_time, `src`.load_date_time, alarm_reference,alarm_value,alarm_message,alarm_status,alarm_priority,alarm_type,alarm_ack_class,alarm_ack_action,alarm_zone_id,superseded_id,alarm_component_id,alarm_display_class,domain,changetype,alarm_time_dtm,alarm_ack_time_dtm,event_insert_time_dtm FROM ( SELECT ROW_NUMBER() OVER ( PARTITION BY hub_doms_alarm_hsh_key, alarm_reference ORDER BY extract_date_time DESC ) AS `REC_SEQ` , hub_doms_alarm_hsh_key, extract_date_time, load_date_time, alarm_reference,alarm_value,alarm_message,alarm_status,alarm_priority,alarm_type,alarm_ack_class,alarm_ack_action,alarm_zone_id,superseded_id,alarm_component_id,alarm_display_class,domain,changetype,alarm_time_dtm,alarm_ack_time_dtm,event_insert_time_dtm FROM imprd001_transform.SAT_DOMS_HISTORIC_ALARM_LOG_HV WHERE event_insert_time_dtm >= '2022-04-29 14:00:00.000000' and event_insert_time_dtm <= '2022-06-01 14:00:00.000000' and alarm_plant_attribute != 'ICCP_FEP' ) `src` INNER JOIN imprd001_rawvault.HUB_DOMS_ALARM `hub` ON `src`.hub_doms_alarm_hsh_key = `hub`.hub_doms_alarm_hsh_key WHERE rec_seq = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended imprd001_transform.SAT_DOMS_HISTORIC_ALARM_LOG_HV

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended imprd001_rawvault.SAT_DOMS_HISTORIC_ALARM_LOG_DETAILS

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize imprd001_rawvault.SAT_DOMS_HISTORIC_ALARM_LOG_DETAILS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_rawvault.SAT_DOMS_HISTORIC_ALARM_LOG_DETAILS WHERE event_insert_time_dtm >= '2022-04-29 14:00:00.000000' and event_insert_time_dtm <= '2022-06-01 14:00:00.000000' 
