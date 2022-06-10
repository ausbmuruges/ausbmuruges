# Databricks notebook source
tab = ["imprd001_rawvault.SAT_CHANNEL_EIP_CHANNEL_PARAM"]
for x in tab:
  x = "OPTIMIZE " + x
  print(x)
  sqlContext.sql(x)

# COMMAND ----------

# SDO Group Sat

tab = ["imprd001_rawvault.LSAT_SRVORDER_MATERIAL_EQUIPMENT_DEVICE_BUSINESS_PARTNER_CIS_AFIH",
"imprd001_rawvault.LSAT_NMI_SWITCHDOC_CIS_EIDESWITDOC",
"imprd001_rawvault.SAT_SWITCHSTEP_CIS_EIDESWTDOCATTR",
"imprd001_rawvault.LSAT_SRVORDER_CIS_AFKO",
"imprd001_rawvault.LSAT_SWITCHDOC_SWITCHSTEP_CIS_EIDESWTDOCSTEP",
"imprd001_rawvault.LSAT_SRVORDER_MATERIAL_EQUIPMENT_DEVICE_NOTIFICATION_CIS_OBJK",
"imprd001_rawvault.SAT_DOC_CIS_ZIIDTT_STAGEIDOC",
"imprd001_rawvault.SAT_SRVORDER_CIS_JSTO",
"imprd001_rawvault.SAT_REF_ZWORKCENTRE_CIS_ZIPMTTWORKCENTER",
"imprd001_rawvault.SAT_REF_EIDESWTSTATUST_CIS_EIDESWTSTATUST",
"imprd001_rawvault.SAT_REF_EIDESWTTYPEST_CIS_EIDESWTTYPEST",
"imprd001_rawvault.SAT_REF_RATE_CAT_CIS_ETTA",
"imprd001_rawvault.LSAT_SRVORDER_BUSINESS_PARTNER_ADDRESS_CIS_IHPA",
"imprd001_rawvault.SAT_REF_WORKCENTER_CIS_CRHD",
"imprd001_rawvault.SAT_REF_WORKCENTER_CIS_CRTX",
"imprd001_rawvault.LSAT_SRVORDER_MATERIAL_CIS_RESB",
"imprd001_rawvault.SAT_REF_GRID_ALLOC_CIS_EGRIDH",
"imprd001_rawvault.SAT_REF_EIDESWTACTIVITYT_CIS_EIDESWTACTIVITYT",
"imprd001_rawvault.LSAT_SRVORDER_CIS_AFVC",
"imprd001_rawvault.LSAT_SWITCHSTEP_DOC_CIS_EIDESWTDOCREF",
"imprd001_rawvault.SAT_SRVORDER_CIS_AFVV",
"imprd001_rawvault.SAT_REF_ORDER_TYP_CIS_T003P",
"imprd001_rawvault.SAT_REF_ORDER_TYP_CIS_T003O",
"imprd001_rawvault.LSAT_SRVORDER_SWITCHDOC_NMI_CIS_AUFK"]
for x in tab:
  x = "OPTIMIZE " + x
  print(x)
  sqlContext.sql(x)

# COMMAND ----------

#  NMI Meter Details Tables
tab = ["imprd001_busvault.bsat_meter_details_mesh", "imprd001_rawvault.lsat_device_uiq_device_data_timestamps"
       ]
for x in tab:
  x = "OPTIMIZE " + x
  print(x)
  sqlContext.sql(x)

# COMMAND ----------

#  Comms Fault Tables
tab = ["imprd001_rawvault.lsat_nmi_cis_euiinstln", 
       "imprd001_rawvault.lsat_nmi_cis_euitrans", 
       "imprd001_rawvault.lsat_nmi_equipment_cis_eastl", 
       "imprd001_rawvault.sat_nmi_cis_ziidtt_nmi_stat",
       "imprd001_rawvault.lsat_equipment_comms_cis_ezug", 
       "imprd001_rawvault.lsat_equipment_device_material_cis_equi", 
       "imprd001_rawvault.lsat_nmi_equipment2_cis_egerh", 
       "imprd001_rawvault.sat_equipment_cis_zipmtt_sync_ts",
       "imprd001_rawvault.sat_srvorder_cis_jest",
       "imprd001_rawvault.lsat_nmi_device_cis_ziidtt_datastrm",
       "imprd001_busvault.bsat_meter_details_wimax"
       ]
for x in tab:
  x = "OPTIMIZE " + x
  print(x)
  sqlContext.sql(x)       
