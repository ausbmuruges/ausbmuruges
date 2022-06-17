# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT logicalenv DEFAULT "dev01"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS im${logicalenv}_staging.EAI_GAS_BIZDOC;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE im${logicalenv}_staging.EAI_GAS_BIZDOC (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    DOCID STRING ,
# MAGIC    DOCTYPEID STRING ,
# MAGIC    SENDERID STRING ,
# MAGIC    RECEIVERID STRING ,
# MAGIC    NATIVEID STRING ,
# MAGIC    DOCTIMESTAMP TIMESTAMP ,
# MAGIC    ROUTINGSTATUS STRING ,
# MAGIC    GROUPID STRING ,
# MAGIC    CONVERSATIONID STRING ,
# MAGIC    USERSTATUS STRING ,
# MAGIC    RECEIVESVC STRING ,
# MAGIC    ORIGSENDERID STRING ,
# MAGIC    ORIGRECEIVERID STRING ,
# MAGIC    LASTMODIFIED TIMESTAMP ,
# MAGIC    COMMENTS STRING ,
# MAGIC    REPEATNUM DOUBLE ,
# MAGIC    DATE_DIM_ID DECIMAL(10,0) ,
# MAGIC    TIME_DIM_ID DOUBLE )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/staging/EAI/EAI_GAS_BIZDOC'
# MAGIC COMMENT 'Staging table for EAI_GAS_BIZDOC'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program im${logicalenv}_staging.EAI_GAS_BIZDOCCONTENT
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure im${logicalenv}_staging.EAI_GAS_BIZDOCCONTENT.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-15 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS im${logicalenv}_staging.EAI_GAS_BIZDOCCONTENT;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE im${logicalenv}_staging.EAI_GAS_BIZDOCCONTENT (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    DOCID STRING ,
# MAGIC    PARTNAME STRING ,
# MAGIC    MIMETYPE STRING ,
# MAGIC    CONTENTLENGTH DOUBLE ,
# MAGIC    CONTENT STRING ,
# MAGIC    PARTINDEX DOUBLE ,
# MAGIC    STORAGETYPE STRING ,
# MAGIC    STORAGEREF STRING )
# MAGIC USING delta
# MAGIC PARTITIONED BY (ROW_INSERT_DTM)
# MAGIC LOCATION '/mnt/staging/EAI/EAI_GAS_BIZDOCCONTENT'
# MAGIC COMMENT 'Staging table for EAI_GAS_BIZDOCCONTENT'
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE im${logicalenv}_staging.EAI_GAS_BIZDOC SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended im${logicalenv}_staging.EAI_GAS_BIZDOC

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_timestamp(ROW_INSERT_DTM,'dd/MM/yyyy HH:mm:ss.SSSSS') from im${logicalenv}_staging.EAI_GAS_BIZDOC

# COMMAND ----------

dbutils.fs.rm("/mnt/staging/EAI/EAI_GAS_BIZDOCCONTENT",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from im${logicalenv}_staging.EAI_GAS_BIZDOC;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from im${logicalenv}_staging.EAI_GAS_BIZDOC;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from im${logicalenv}_staging.EAI_GAS_BIZDOC;
# MAGIC select * from im${logicalenv}_staging.EAI_GAS_BIZDOCATTRIBUTE;
# MAGIC select * from im${logicalenv}_staging.EAI_GAS_BIZDOCCONTENT;
# MAGIC select * from im${logicalenv}_staging.EAI_GAS_BIZDOCRELATIONSHIP;
# MAGIC select * from im${logicalenv}_staging.EAI_GAS_BIZDOCTYPEDEF;
# MAGIC select * from im${logicalenv}_staging.EAI_GAS_PARTNER;
# MAGIC select * from im${logicalenv}_staging.HAN_CONNECTION_POINT;
# MAGIC select * from im${logicalenv}_recon.EAI_GAS_HDL_DET;
# MAGIC select * from im${logicalenv}_recon.EAI_GAS_HDL_HDR;
# MAGIC select * from im${logicalenv}_recon.EAI_GAS_HUL_DET;
# MAGIC select * from im${logicalenv}_recon.EAI_GAS_HUL_HDR;
# MAGIC select * from im${logicalenv}_recon.EAI_GAS_MDN_BIZDOC;
# MAGIC select * from im${logicalenv}_recon.EAI_GAS_MDN_METER_READS;
# MAGIC select * from im${logicalenv}_recon.HAN_CONNECTION_POINT;
# MAGIC select * from im${logicalenv}_recon.HAN_ROUTE_DIVISION;
# MAGIC select * from im${logicalenv}_recon.HAN_PART_RETAILER;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'EAI_GAS_BIZDOC' as TAB, count(*) as CNT from im${logicalenv}_staging.EAI_GAS_BIZDOC union all 
# MAGIC select 'EAI_GAS_BIZDOCATTRIBUTE' as TAB, count(*) as CNT from im${logicalenv}_staging.EAI_GAS_BIZDOCATTRIBUTE union all 
# MAGIC select 'EAI_GAS_BIZDOCCONTENT' as TAB, count(*) as CNT from im${logicalenv}_staging.EAI_GAS_BIZDOCCONTENT union all 
# MAGIC select 'EAI_GAS_BIZDOCRELATIONSHIP' as TAB, count(*) as CNT from im${logicalenv}_staging.EAI_GAS_BIZDOCRELATIONSHIP union all 
# MAGIC select 'EAI_GAS_BIZDOCTYPEDEF' as TAB, count(*) as CNT from im${logicalenv}_staging.EAI_GAS_BIZDOCTYPEDEF union all 
# MAGIC select 'EAI_GAS_PARTNER' as TAB, count(*) as CNT from im${logicalenv}_staging.EAI_GAS_PARTNER union all 
# MAGIC select 'HAN_CONNECTION_POINT' as TAB, count(*) as CNT from im${logicalenv}_staging.HAN_CONNECTION_POINT union all 
# MAGIC select 'EAI_GAS_HDL_DET' as TAB, count(*) as CNT from im${logicalenv}_recon.EAI_GAS_HDL_DET union all 
# MAGIC select 'EAI_GAS_HDL_HDR' as TAB, count(*) as CNT from im${logicalenv}_recon.EAI_GAS_HDL_HDR union all 
# MAGIC select 'EAI_GAS_HUL_DET' as TAB, count(*) as CNT from im${logicalenv}_recon.EAI_GAS_HUL_DET union all 
# MAGIC select 'EAI_GAS_HUL_HDR' as TAB, count(*) as CNT from im${logicalenv}_recon.EAI_GAS_HUL_HDR union all 
# MAGIC select 'EAI_GAS_MDN_BIZDOC' as TAB, count(*) as CNT from im${logicalenv}_recon.EAI_GAS_MDN_BIZDOC union all 
# MAGIC select 'EAI_GAS_MDN_METER_READS' as TAB, count(*) as CNT from im${logicalenv}_recon.EAI_GAS_MDN_METER_READS union all 
# MAGIC select 'HAN_CONNECTION_POINT' as TAB, count(*) as CNT from im${logicalenv}_recon.HAN_CONNECTION_POINT union all 
# MAGIC select 'HAN_ROUTE_DIVISION' as TAB, count(*) as CNT from im${logicalenv}_recon.HAN_ROUTE_DIVISION union all 
# MAGIC select 'HAN_PART_RETAILER' as TAB, count(*) as CNT from im${logicalenv}_recon.HAN_PART_RETAILER ;

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -lhrt /dbfs/mnt/staging/EAI/EAI_GAS_BIZDOC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/staging/EAI/

# COMMAND ----------

file_paths ={'/mnt/staging/EAI/EAI_GAS_BIZDOC',
'/mnt/staging/EAI/EAI_GAS_BIZDOCATTRIBUTE',
'/mnt/staging/EAI/EAI_GAS_BIZDOCCONTENT',
'/mnt/staging/EAI/EAI_GAS_BIZDOCRELATIONSHIP',
'/mnt/staging/EAI/EAI_GAS_BIZDOCTYPEDEF',
'/mnt/staging/EAI/EAI_GAS_PARTNER',
'/mnt/staging/EAI/HAN_CONNECTION_POINT',
'/mnt/imrv/deltalake/im${logicalenv}/recon/EAI_GAS_HDL_DET',
'/mnt/imrv/deltalake/im${logicalenv}/recon/EAI_GAS_HDL_HDR',
'/mnt/imrv/deltalake/im${logicalenv}/recon/EAI_GAS_HUL_DET',
'/mnt/imrv/deltalake/im${logicalenv}/recon/EAI_GAS_HUL_HDR',
'/mnt/imrv/deltalake/im${logicalenv}/recon/EAI_GAS_MDN_BIZDOC',
'/mnt/imrv/deltalake/im${logicalenv}/recon/EAI_GAS_MDN_METER_READS',
'/mnt/imrv/deltalake/im${logicalenv}/recon/HAN_CONNECTION_POINT',
'/mnt/imrv/deltalake/im${logicalenv}/recon/HAN_ROUTE_DIVISION',
'/mnt/imrv/deltalake/im${logicalenv}/recon/HAN_PART_RETAILER',
}

for file_path in file_paths:
  file_path = file_path.replace('${', '{')  
  Params = {}
  Params["logicalenv"]=dbutils.widgets.get("logicalenv")
  file_path = file_path.format(**Params)
  for filepath in dbutils.fs.ls(file_path):
    file = str(filepath).split('\'')[1]
    print('removing: '+file)
    dbutils.fs.rm(file, True)

# COMMAND ----------

file_paths ={'/mnt/staging/EAI/EAI_GAS_BIZDOC','/mnt/staging/EAI/EAI_GAS_BIZDOCCONTENT'}

for file_path in file_paths:
  file_path = file_path.replace('${', '{')  
  Params = {}
  Params["logicalenv"]=dbutils.widgets.get("logicalenv")
  file_path = file_path.format(**Params)
  for filepath in dbutils.fs.ls(file_path):
    file = str(filepath).split('\'')[1]
    print('removing: '+file)
    dbutils.fs.rm(file, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.EAI_GAS_HDL_DET
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.EAI_GAS_HDL_DET.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.EAI_GAS_HDL_DET;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.EAI_GAS_HDL_DET (
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    ETL_RUN_ID DECIMAL(38,0) ,
# MAGIC    ACCOUNT_NUMBER STRING ,
# MAGIC    ROUTE_NUMBER STRING ,
# MAGIC    CYCLE_DATE STRING ,
# MAGIC    METER_NUMBER STRING ,
# MAGIC    METER_READ_SEQUENCE STRING ,
# MAGIC    READ_METHOD STRING ,
# MAGIC    PREVIOUS_READING STRING ,
# MAGIC    HIGH1 STRING ,
# MAGIC    LOW1 STRING ,
# MAGIC    NUM_OF_CONSECUTIVE_EST STRING ,
# MAGIC    TOTAL_NUM_OF_CUST_RECORDS STRING ,
# MAGIC    FILE_NAME STRING ,
# MAGIC    MESSAGEID STRING )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/EAI_GAS_HDL_DET'
# MAGIC COMMENT 'Recon table for EAI_GAS_HDL_DET'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.EAI_GAS_HDL_HDR
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.EAI_GAS_HDL_HDR.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.EAI_GAS_HDL_HDR;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.EAI_GAS_HDL_HDR (
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    ETL_RUN_ID DECIMAL(38,0) ,
# MAGIC    FROM1 STRING ,
# MAGIC    TO1 STRING ,
# MAGIC    MESSAGEID STRING ,
# MAGIC    MESSAGEDATE STRING ,
# MAGIC    PRIORITY STRING ,
# MAGIC    MARKET STRING ,
# MAGIC    TRANSACTIONID STRING ,
# MAGIC    TRANSACTIONDATE STRING ,
# MAGIC    VERSION1 STRING ,
# MAGIC    DOCID STRING ,
# MAGIC    DOCTYPEID STRING ,
# MAGIC    SENDERID STRING ,
# MAGIC    RECEIVERID STRING ,
# MAGIC    NATIVEID STRING ,
# MAGIC    DOCTIMESTAMP TIMESTAMP ,
# MAGIC    ROUTINGSTATUS STRING ,
# MAGIC    GROUPID STRING ,
# MAGIC    CONVERSATIONID STRING ,
# MAGIC    USERSTATUS STRING ,
# MAGIC    RECEIVESVC STRING ,
# MAGIC    LASTMODIFIED TIMESTAMP ,
# MAGIC    COMMENTS STRING ,
# MAGIC    REPEATNUM DECIMAL(38,0) ,
# MAGIC    DATE_DIM_ID DECIMAL(10,0) ,
# MAGIC    TIME_DIM_ID DECIMAL(38,0) ,
# MAGIC    CONTENTLENGTH DECIMAL(38,0) )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/EAI_GAS_HDL_HDR'
# MAGIC COMMENT 'Recon table for EAI_GAS_HDL_HDR'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.EAI_GAS_HUL_DET
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.EAI_GAS_HUL_DET.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.EAI_GAS_HUL_DET;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.EAI_GAS_HUL_DET (
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    ETL_RUN_ID DECIMAL(38,0) ,
# MAGIC    ACCOUNT_NUMBER STRING ,
# MAGIC    METER_NUMBER STRING ,
# MAGIC    ROUTE_NUMBER STRING ,
# MAGIC    SCHEDULED_READ_DATE STRING ,
# MAGIC    FORCE_COMPLETE_FLAG STRING ,
# MAGIC    CHANNEL_NUMBER STRING ,
# MAGIC    READ_METHOD STRING ,
# MAGIC    METER_CONSTANT STRING ,
# MAGIC    CONSTANT_FLAG STRING ,
# MAGIC    READING STRING ,
# MAGIC    READ_ORDER STRING ,
# MAGIC    READ_DATE STRING ,
# MAGIC    READ_TIME STRING ,
# MAGIC    READ_CODE STRING ,
# MAGIC    READ_REASON STRING ,
# MAGIC    READ_AUDIT_REASON STRING ,
# MAGIC    TROUBLE_CODE_1 STRING ,
# MAGIC    TROUBLE_CODE_2 STRING ,
# MAGIC    AUDIT_COUNTER STRING ,
# MAGIC    CLEAR_COUNTER STRING ,
# MAGIC    UNPROCESSED_READ_FLAG STRING ,
# MAGIC    METER_READER_ID STRING ,
# MAGIC    ELAPSED_TIME STRING ,
# MAGIC    NEW_NUMBER_OF_DIALS STRING ,
# MAGIC    NEW_NUMBER_OF_DECIMALS STRING ,
# MAGIC    NEW_READ_METHOD STRING ,
# MAGIC    METER_CONSTANT_VERIFIED STRING ,
# MAGIC    HHF_POSITION STRING ,
# MAGIC    NEW_METER_CONSTANT STRING ,
# MAGIC    READ_TYPE_CODE STRING ,
# MAGIC    PREVIOUS_READING STRING ,
# MAGIC    FILE_NAME STRING ,
# MAGIC    MESSAGEID STRING )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/EAI_GAS_HUL_DET'
# MAGIC COMMENT 'Recon table for EAI_GAS_HUL_DET'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.EAI_GAS_HUL_HDR
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.EAI_GAS_HUL_HDR.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.EAI_GAS_HUL_HDR;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.EAI_GAS_HUL_HDR (
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    ETL_RUN_ID DECIMAL(38,0) ,
# MAGIC    FROM1 STRING ,
# MAGIC    TO1 STRING ,
# MAGIC    MESSAGEID STRING ,
# MAGIC    MESSAGEDATE STRING ,
# MAGIC    PRIORITY STRING ,
# MAGIC    MARKET STRING ,
# MAGIC    TRANSACTIONID STRING ,
# MAGIC    TRANSACTIONDATE STRING ,
# MAGIC    VERSION1 STRING ,
# MAGIC    DOCID STRING ,
# MAGIC    DOCTYPEID STRING ,
# MAGIC    SENDERID STRING ,
# MAGIC    RECEIVERID STRING ,
# MAGIC    NATIVEID STRING ,
# MAGIC    DOCTIMESTAMP TIMESTAMP ,
# MAGIC    ROUTINGSTATUS STRING ,
# MAGIC    GROUPID STRING ,
# MAGIC    CONVERSATIONID STRING ,
# MAGIC    USERSTATUS STRING ,
# MAGIC    RECEIVESVC STRING ,
# MAGIC    LASTMODIFIED TIMESTAMP ,
# MAGIC    COMMENTS STRING ,
# MAGIC    REPEATNUM DECIMAL(38,0) ,
# MAGIC    DATE_DIM_ID DECIMAL(10,0) ,
# MAGIC    TIME_DIM_ID DECIMAL(38,0) ,
# MAGIC    CONTENTLENGTH DECIMAL(38,0) )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/EAI_GAS_HUL_HDR'
# MAGIC COMMENT 'Recon table for EAI_GAS_HUL_HDR'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.EAI_GAS_MDN_BIZDOC
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.EAI_GAS_MDN_BIZDOC.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.EAI_GAS_MDN_BIZDOC;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.EAI_GAS_MDN_BIZDOC (
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    ETL_RUN_ID DECIMAL(38,0) ,
# MAGIC    FROM1 STRING ,
# MAGIC    TO1 STRING ,
# MAGIC    MESSAGEID STRING ,
# MAGIC    MESSAGEDATE STRING ,
# MAGIC    PRIORITY STRING ,
# MAGIC    MARKET STRING ,
# MAGIC    TRANSACTIONID STRING ,
# MAGIC    TRANSACTIONDATE STRING ,
# MAGIC    VERSION1 STRING ,
# MAGIC    RECORDCOUNT DECIMAL(10,0) ,
# MAGIC    DOCID STRING ,
# MAGIC    DOCTYPEID STRING ,
# MAGIC    SENDERID STRING ,
# MAGIC    RECEIVERID STRING ,
# MAGIC    NATIVEID STRING ,
# MAGIC    DOCTIMESTAMP TIMESTAMP ,
# MAGIC    ROUTINGSTATUS STRING ,
# MAGIC    GROUPID STRING ,
# MAGIC    CONVERSATIONID STRING ,
# MAGIC    USERSTATUS STRING ,
# MAGIC    RECEIVESVC STRING ,
# MAGIC    LASTMODIFIED TIMESTAMP ,
# MAGIC    COMMENTS STRING ,
# MAGIC    REPEATNUM DECIMAL(38,0) ,
# MAGIC    DATE_DIM_ID DECIMAL(10,0) ,
# MAGIC    TIME_DIM_ID DECIMAL(38,0) ,
# MAGIC    CONTENTLENGTH DECIMAL(38,0) )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/EAI_GAS_MDN_BIZDOC'
# MAGIC COMMENT 'Recon table for EAI_GAS_MDN_BIZDOC'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.EAI_GAS_MDN_METER_READS
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.EAI_GAS_MDN_METER_READS.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.EAI_GAS_MDN_METER_READS;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.EAI_GAS_MDN_METER_READS (
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    ETL_RUN_ID DECIMAL(38,0) ,
# MAGIC    NMI STRING ,
# MAGIC    NMI_CHECKSUM STRING ,
# MAGIC    RB_REFERENCE_NUMBER STRING ,
# MAGIC    REASON_FOR_READ STRING ,
# MAGIC    GAS_METER_NUMBER STRING ,
# MAGIC    GAS_METER_UNITS STRING ,
# MAGIC    PREVIOUS_INDEX_VALUE STRING ,
# MAGIC    PREVIOUS_READ_DATE STRING ,
# MAGIC    CURRENT_INDEX_VALUE STRING ,
# MAGIC    CURRENT_READ_DATE STRING ,
# MAGIC    VOLUME_FLOW STRING ,
# MAGIC    AVERAGE_HEATING_VALUE STRING ,
# MAGIC    PRESSURE_CORRECTION_FACTOR STRING ,
# MAGIC    CONSUMED_ENERGY STRING ,
# MAGIC    TYPE_OF_READ STRING ,
# MAGIC    ESTIMATION_SUBSTITUTION_TYPE STRING ,
# MAGIC    EST_SUBS_REASON_CODE STRING ,
# MAGIC    METER_STATUS STRING ,
# MAGIC    NEXT_SCHEDULED_READ_DATE STRING ,
# MAGIC    HI_LOW_FAILURE STRING ,
# MAGIC    METER_CAPACITY_FAILURE STRING ,
# MAGIC    ADJUSTMENT_REASON_CODE STRING ,
# MAGIC    ENERGY_CALCULATION_DATE_STAMP STRING ,
# MAGIC    ENERGY_CALCULATION_TIME_STAMP STRING ,
# MAGIC    MESSAGEID STRING ,
# MAGIC    TRANSACTIONID STRING )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/EAI_GAS_MDN_METER_READS'
# MAGIC COMMENT 'Recon table for EAI_GAS_MDN_METER_READS'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.HAN_CONNECTION_POINT
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.HAN_CONNECTION_POINT.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.HAN_CONNECTION_POINT;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.HAN_CONNECTION_POINT (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_EFFECTIVE_DTM TIMESTAMP ,
# MAGIC    ROW_EXPIRATION_DTM TIMESTAMP ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    ROW_UPDATE_DTM TIMESTAMP ,
# MAGIC    MIRN STRING ,
# MAGIC    MIRN_START_DT TIMESTAMP ,
# MAGIC    MIRN_STATUS STRING ,
# MAGIC    METER STRING ,
# MAGIC    METER_START_DT TIMESTAMP ,
# MAGIC    METER_END_DT TIMESTAMP ,
# MAGIC    METER_STATUS STRING ,
# MAGIC    METER_TYPE STRING ,
# MAGIC    METER_CLASS STRING ,
# MAGIC    ROUTE_ID STRING ,
# MAGIC    RETAILER_ID DECIMAL(10,0) ,
# MAGIC    TIER DOUBLE ,
# MAGIC    SITE_ADDRESS STRING )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/HAN_CONNECTION_POINT'
# MAGIC COMMENT 'Recon table for HAN_CONNECTION_POINT'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.HAN_PART_RETAILER
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.HAN_PART_RETAILER.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.HAN_PART_RETAILER;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.HAN_PART_RETAILER (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    RETAILER_ID DOUBLE ,
# MAGIC    RETAILER_NAME STRING ,
# MAGIC    PART_ID STRING ,
# MAGIC    TIER STRING )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/HAN_PART_RETAILER'
# MAGIC COMMENT 'Recon table for HAN_PART_RETAILER'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_recon.HAN_ROUTE_DIVISION
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_recon.HAN_ROUTE_DIVISION.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_recon.HAN_ROUTE_DIVISION;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_recon.HAN_ROUTE_DIVISION (
# MAGIC    ROUTE_ID STRING ,
# MAGIC    SUBURB STRING ,
# MAGIC    DIVISION STRING ,
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/imrv/deltalake/imdev01/recon/HAN_ROUTE_DIVISION'
# MAGIC COMMENT 'Recon table for HAN_ROUTE_DIVISION'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_staging.EAI_GAS_BIZDOCATTRIBUTE
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_staging.EAI_GAS_BIZDOCATTRIBUTE.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_staging.EAI_GAS_BIZDOCATTRIBUTE;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_staging.EAI_GAS_BIZDOCATTRIBUTE (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    DOCID STRING ,
# MAGIC    ATTRIBUTEID STRING ,
# MAGIC    STRINGVALUE STRING ,
# MAGIC    NUMBERVALUE FLOAT ,
# MAGIC    DATEVALUE TIMESTAMP )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/staging/EAI/EAI_GAS_BIZDOCATTRIBUTE'
# MAGIC COMMENT 'Staging table for EAI_GAS_BIZDOCATTRIBUTE'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_staging.EAI_GAS_BIZDOCCONTENT
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_staging.EAI_GAS_BIZDOCCONTENT.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_staging.EAI_GAS_BIZDOCCONTENT;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_staging.EAI_GAS_BIZDOCCONTENT (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    DOCID STRING ,
# MAGIC    PARTNAME STRING ,
# MAGIC    MIMETYPE STRING ,
# MAGIC    CONTENTLENGTH DOUBLE ,
# MAGIC    CONTENT STRING ,
# MAGIC    PARTINDEX DOUBLE ,
# MAGIC    STORAGETYPE STRING ,
# MAGIC    STORAGEREF STRING )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/staging/EAI/EAI_GAS_BIZDOCCONTENT'
# MAGIC COMMENT 'Staging table for EAI_GAS_BIZDOCCONTENT'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_staging.EAI_GAS_BIZDOCRELATIONSHIP
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_staging.EAI_GAS_BIZDOCRELATIONSHIP.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_staging.EAI_GAS_BIZDOCRELATIONSHIP;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_staging.EAI_GAS_BIZDOCRELATIONSHIP (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    DOCID STRING ,
# MAGIC    RELATEDDOCID STRING ,
# MAGIC    RELATIONSHIP STRING )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/staging/EAI/EAI_GAS_BIZDOCRELATIONSHIP'
# MAGIC COMMENT 'Staging table for EAI_GAS_BIZDOCRELATIONSHIP'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_staging.EAI_GAS_BIZDOC
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_staging.EAI_GAS_BIZDOC.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_staging.EAI_GAS_BIZDOC;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_staging.EAI_GAS_BIZDOC (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    DOCID STRING ,
# MAGIC    DOCTYPEID STRING ,
# MAGIC    SENDERID STRING ,
# MAGIC    RECEIVERID STRING ,
# MAGIC    NATIVEID STRING ,
# MAGIC    DOCTIMESTAMP TIMESTAMP ,
# MAGIC    ROUTINGSTATUS STRING ,
# MAGIC    GROUPID STRING ,
# MAGIC    CONVERSATIONID STRING ,
# MAGIC    USERSTATUS STRING ,
# MAGIC    RECEIVESVC STRING ,
# MAGIC    ORIGSENDERID STRING ,
# MAGIC    ORIGRECEIVERID STRING ,
# MAGIC    LASTMODIFIED TIMESTAMP ,
# MAGIC    COMMENTS STRING ,
# MAGIC    REPEATNUM DOUBLE ,
# MAGIC    DATE_DIM_ID DECIMAL(10,0) ,
# MAGIC    TIME_DIM_ID DOUBLE )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/staging/EAI/EAI_GAS_BIZDOC'
# MAGIC COMMENT 'Staging table for EAI_GAS_BIZDOC'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_staging.EAI_GAS_BIZDOCTYPEDEF
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_staging.EAI_GAS_BIZDOCTYPEDEF.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_staging.EAI_GAS_BIZDOCTYPEDEF;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_staging.EAI_GAS_BIZDOCTYPEDEF (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    TYPENAME STRING ,
# MAGIC    TYPEID STRING ,
# MAGIC    TYPEDESCRIPTION STRING ,
# MAGIC    TYPEDATA STRING ,
# MAGIC    DELETED DOUBLE ,
# MAGIC    LASTMODIFIED TIMESTAMP ,
# MAGIC    SUBTYPEID DOUBLE ,
# MAGIC    BDTYPE DOUBLE )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/staging/EAI/EAI_GAS_BIZDOCTYPEDEF'
# MAGIC COMMENT 'Staging table for EAI_GAS_BIZDOCTYPEDEF'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_staging.EAI_GAS_PARTNER
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_staging.EAI_GAS_PARTNER.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_staging.EAI_GAS_PARTNER;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_staging.EAI_GAS_PARTNER (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    PARENTPARTNERID STRING ,
# MAGIC    PARTNERID STRING ,
# MAGIC    CORPORATIONNAME STRING ,
# MAGIC    ORGUNITNAME STRING ,
# MAGIC    DELETED DOUBLE ,
# MAGIC    STATUS STRING ,
# MAGIC    SUBSTATUS STRING ,
# MAGIC    TYPE STRING ,
# MAGIC    SELF DOUBLE ,
# MAGIC    TNVERSION STRING ,
# MAGIC    B2BCOMMEMBER DOUBLE ,
# MAGIC    PREFERREDLOCALE STRING ,
# MAGIC    ROUTINGOFF DOUBLE ,
# MAGIC    LASTMODIFIED TIMESTAMP )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/staging/EAI/EAI_GAS_PARTNER'
# MAGIC COMMENT 'Staging table for EAI_GAS_PARTNER'
# MAGIC ;
# MAGIC /*
# MAGIC ------------------------------------------------------------------------------- 
# MAGIC -- Program imdev01_staging.HAN_CONNECTION_POINT
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Purpose:
# MAGIC --   Creates the ADB table structure imdev01_staging.HAN_CONNECTION_POINT.
# MAGIC --
# MAGIC --   Run by administrative user with permissions to create tables in ADB
# MAGIC --   database. 
# MAGIC --
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- Date       | Author                        | Description
# MAGIC -------------------------------------------------------------------------------
# MAGIC -- 2022-06-16 |  Automation Script            | Initial Version
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC --            |                               |
# MAGIC -------------------------------------------------------------------------------
# MAGIC */
# MAGIC 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS imdev01_staging.HAN_CONNECTION_POINT;
# MAGIC 
# MAGIC 
# MAGIC CREATE TABLE imdev01_staging.HAN_CONNECTION_POINT (
# MAGIC    ETL_RUN_ID DOUBLE ,
# MAGIC    ROW_INSERT_DTM TIMESTAMP ,
# MAGIC    FILE_DT TIMESTAMP ,
# MAGIC    MIRN STRING ,
# MAGIC    MIRN_START_DT STRING ,
# MAGIC    MIRN_STATUS STRING ,
# MAGIC    METER STRING ,
# MAGIC    METER_START_DT STRING ,
# MAGIC    METER_END_DT STRING ,
# MAGIC    METER_STATUS STRING ,
# MAGIC    METERT_TYPE STRING ,
# MAGIC    METER_CLASS STRING ,
# MAGIC    ROUTE_ID STRING ,
# MAGIC    RETAILER_ID DECIMAL(10,0) ,
# MAGIC    TIER DOUBLE ,
# MAGIC    SITE_ADDRESS STRING )
# MAGIC USING delta
# MAGIC LOCATION '/mnt/staging/EAI/HAN_CONNECTION_POINT'
# MAGIC COMMENT 'Staging table for HAN_CONNECTION_POINT'
# MAGIC ;
