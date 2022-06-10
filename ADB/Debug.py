# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE WIDGET TEXT logicalenv DEFAULT "prd001";

# COMMAND ----------

import logging
import os
import sys
from functools import partial

import pytz
import time
import argparse
import pandas as pd
import numpy as np
import urllib
import uuid

from datetime import datetime, timedelta, date
from dateutil import parser

from pandas.io.sql import SQLTable
from collections import OrderedDict


from sklearn.preprocessing import LabelEncoder

# from azure.storage.blob import BlockBlobService

# ------------------------------------------------------
# Edit these constants if required
# ------------------------------------------------------

JDBC_FILEPATH = "../../ausnet-logical-app/linux/edge/app/lib"

os.environ["CLASSPATH"] = "{0}/hive-jdbc-uber-2.6.3.0-235.jar:{0}/mssql-jdbc-7.0.0.jre8.jar".format(
    JDBC_FILEPATH)

# Has to be after the os.environ import

# Determines whether you are scheduling the code or running manually.
# Set this to False when added to control framework scheduling
MANUAL_EXECUTION = True

# Determines if the application (live production) server is in use
DEV_SERVER = True

# If True, we write to a CSV file to be added to blob storage for the Hive database,
# otherwise we write to SQL DW
WRITE_TO_HIVE = False

if not MANUAL_EXECUTION:
    from ControlFramework import Job, JobInstance, Client

ALARM_PATTERN_ID = 15
COMPONENT_PATTERN_ID = 16

CONFIG_FILE_PATH = '../../ausnet-logical-app/linux/edge/app/conf'

SRC_SCHEMA = "imprd01_rawvault"
DEST_SCHEMA = "imprd01_analytics"

# ------------------------------------------------------
# Constants
# ------------------------------------------------------

# Track connection attempts if they fail
MAX_ATTEMPTS = 5

BATCH_INSERT_SIZE = 5000

# Association rules
TRANSACTION_SIZE = 2
PERCENTAGE_SIDE = 10

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

NOW = datetime.utcnow()


TOGGLING_STATES = [("OPERATED", "RESET"), ("ABNORMAL", "OK"), ("INACTIVE", "ACTIVE"),
                   ("FAILED", "OK"), ("NOT READY", "OK"), ("ON", "OFF"), ("ENABLED", "DISABLED"),
                   ("RUNNING", "STOPPED"), ("LOW", "NORMAL"), ("OPEN", "CLOSED")]

# Functions called to process and save alarm/event data in the order it is executed.
ALARM_FUNCTIONS = ["get_historic_alarm_log",
                   "clean_alarms",
                   "transform_alarms",
                   "judge_if_pheripheral",
                   "create_statistics",
                   "create_stale_abnormal_alarms",
                   "create_acknowledged_alarms",
                   "update_old_alarm_data",
                   "create_incorrect_field_time",
                   "create_monthly_alarm_counts",
                   "create_monthly_statistics_drilldown",
                   "create_daily_volume_drilldown",
                   "create_monthly_region_alarm_rates",
                   "create_monthly_noisy_alarm_counts",
                   "create_daily_noise"]

# Functions called to process and save component related data in the order it is executed.
COMPONENT_FUNCTIONS = ["create_alarm_definitions_components", "create_component_attributes",
                       "create_time_delayed_components", "create_limit_components"]

# ------------------------------------------------------
# Global Variables
# ------------------------------------------------------

logger = None

hivedb_dict = None
sqldw_dict = None

hive_conn_timeout = 300
sqldw_timeout = 300

hive_last_operation_time = None
sqldw_last_operation_time = None

hivedb_conn = None

# Set the configuration for blob storage if saving to the Hive database
blob_config_dict = None

# This needs to be declared earlier since I need to call it in update_time_id
def generate_id():
    """
    Generate a unique ID, useful as "primary keys" for tables
    :return: A string representing a unique ID (36 characters)
    :rtype: str
    """
    return str(uuid.uuid4())


# update_time_id should only be altered in create_update_time_id()
update_time_id = generate_id()

# ------------------------------------------------------
# Tables to save into SQL DW
# ------------------------------------------------------

TABLE_PREFIX = "DOMS_"

UPDATE_TABLE = TABLE_PREFIX + "UPDATE_DATE_TIME"

# Overview Page
TEN_MIN_STATS_TABLE = TABLE_PREFIX + "TEN_MIN_STATISTIC"

# Treemap page
FILTER_COMBO_TABLE = TABLE_PREFIX + "FILTER_COMBINATION"
DAILY_VOLUME_DRILLDOWN_TABLE = TABLE_PREFIX + "DAILY_VOLUME_DRILLDOWN"
MONTHLY_STATS_TABLE = TABLE_PREFIX + "MONTHLY_STATISTIC_DRILLDOWN"
MONTHLY_ALARM_COUNT_TABLE = TABLE_PREFIX + "MONTHLY_ALARM_COUNT"
MONTHLY_REGION_ALARM_RATE_TABLE = TABLE_PREFIX + "MONTHLY_REGION_ALARM_RATE"

# Alarm Noise Analytics page
MONTHLY_NOISY_ALARM_COUNTS_TABLE = TABLE_PREFIX + "MONTHLY_NOISY_ALARM_COUNT"
DAILY_VOLUME_TABLE = TABLE_PREFIX + "DAILY_VOLUME"

# Responsiveness Page
ACKNOWLEDGED_ALARM_TABLE = TABLE_PREFIX + "ACKNOWLEDGED_ALARM"
ABNORMAL_ALARM_TABLE = TABLE_PREFIX + "STALE_ABNORMAL_ALARM"
ABNORMAL_ALARM_COUNT_TABLE = TABLE_PREFIX + "STALE_ABNORMAL_ALARM_COUNT"

# Device Alarm Consistency Page
COMPONENT_TABLE = TABLE_PREFIX + "DQ_COMPONENT"
ATTRIBUTE_TABLE = TABLE_PREFIX + "DQ_COMPONENT_ATTRIBUTE"

TIME_DELAY_COMP_TABLE = TABLE_PREFIX + "TIME_DELAYED_COMPONENT"
LIMIT_COMP_TABLE = TABLE_PREFIX + "LIMIT_COMPONENT"

# Alarm Data Quality Page
COMPONENT_INCORRECT_TIME_TABLE = TABLE_PREFIX + "INCORRECT_TIME_COMPONENT"
COMPONENT_INCORRECT_TIME_COUNT_TABLE = TABLE_PREFIX + "INCORRECT_TIME_COMPONENT_COUNT"
ALARM_DEFINITION_COUNTS_TABLE = TABLE_PREFIX + "ALARM_DEFINITION_COUNT"

# ------------------------------------------------------
# Some common Hive queries
# ------------------------------------------------------

ALARM_QUERY = """
SELECT
    `src`.hub_doms_alarm_hsh_key,
    `hub`.alarm_id,
    `src`.extract_date_time,
    `src`.load_date_time,
    {0}
FROM
    (
    SELECT
        ROW_NUMBER() OVER ( PARTITION BY hub_doms_alarm_hsh_key,
        alarm_reference ORDER BY extract_date_time DESC ) AS `REC_SEQ` ,
        hub_doms_alarm_hsh_key,
        extract_date_time,
        load_date_time,
        {0}
    FROM
        {1}.SAT_DOMS_HISTORIC_ALARM_LOG_DETAILS
    WHERE event_insert_time_dtm >= '{2}' and event_insert_time_dtm <= '{3}' 
    and alarm_plant_attribute != 'ICCP_FEP'
    ) `src`
    INNER JOIN {1}.HUB_DOMS_ALARM `hub`
    ON `src`.hub_doms_alarm_hsh_key = `hub`.hub_doms_alarm_hsh_key
WHERE
    rec_seq = 1
"""

ALARM_UPDATE_QUERY = """
SELECT
    `src`.hub_doms_alarm_hsh_key,
    `hub`.alarm_id,
    `src`.extract_date_time,
    `src`.load_date_time,
    {0}
FROM
    (
    SELECT
        ROW_NUMBER() OVER ( PARTITION BY hub_doms_alarm_hsh_key,
        alarm_reference ORDER BY extract_date_time DESC ) AS `REC_SEQ` ,
        hub_doms_alarm_hsh_key,
        extract_date_time,
        load_date_time,
        {0}
    FROM
        {1}.SAT_DOMS_HISTORIC_ALARM_LOG_DETAILS
    WHERE load_date_time > '{2}' and event_insert_time_dtm <= '{3}'
    and alarm_plant_attribute != 'ICCP_FEP'
    ) `src`
    INNER JOIN {1}.HUB_DOMS_ALARM `hub`
    ON `src`.hub_doms_alarm_hsh_key = `hub`.hub_doms_alarm_hsh_key
WHERE
    rec_seq = 1
"""

COMPONENT_QUERY = """
SELECT
    `src`.hub_doms_component_hsh_key,
    component_id,
    {0}
FROM
    (
    SELECT
        ROW_NUMBER() OVER ( PARTITION BY hub_doms_component_hsh_key
        ORDER BY extract_date_time DESC ) AS `REC_SEQ` ,
        hub_doms_component_hsh_key,
        {0}
    FROM
        {1}.SAT_DOMS_COMPONENT_HEADER_DETAILS
    ) `src` left join {1}.HUB_DOMS_COMPONENT `b` 
    on `src`.hub_doms_component_hsh_key = `b`.hub_doms_component_hsh_key
WHERE
    rec_seq = 1
"""

# ------------------------------------------------------
# Exception handling classes
# ------------------------------------------------------

class Error(Exception):
    '''Base class for exceptions in this module.'''


class DatabaseException(Error):
    '''Control Framework Database Exception
    Attributes:
        type     -- Type of error
        message  -- Message describing the error
    '''

    def __init__(self, type, message):
        self.error_type = type
        self.message = message


class SubmitException(Error):
    '''Job Submission Exception
    Attributes:
        type     -- Type of errror
        message  -- Message describing the error
    '''

    def __init__(self, type, message):
        self.error_type = type
        self.message = message


class MetadataException(Error):
    '''Control Framework Metadata Exception
    Attributes:
        type     -- Type of errror
        message  -- Message describing the error
    '''

    def __init__(self, type, message):
        self.error_type = type
        self.message = message


# ------------------------------------------------------
# Setup the logger
# ------------------------------------------------------
def setup_logger(logfile_name):
    # Handle relative paths
    if logfile_name[0] != '/':
        # Path is absolute
        logfile = logfile_name
    else:
        # Path is relative to script
        script_path = os.path.dirname(os.path.realpath(__file__))
        logfile = script_path + "/" + logfile_name

    # Configure the file base logger
    logging.basicConfig(filename=logfile,
                        format='%(asctime)s %(levelname)s %(module)s.%(funcName)s %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Add sysout
    sysout_logger = logging.StreamHandler(sys.stdout)
    sysout_logger.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(sysout_logger)

    return logger


# ------------------------------------------------------
# Control Framework Functions - slight modifications from controL_framework.py
# ------------------------------------------------------

def connect_to_hive():
    """
    Connect to the Hive database, setting the global variable hivedb_conn.
    It is a :class:`jaydebeapi.Connection`
    """
    global hive_last_operation_time
    global hivedb_conn
    # Populate local connection variables from the config
    driver = hivedb_dict['driver_class']
    connection_prefix = hivedb_dict['jdbc_connection_prefix']
    connection_suffix = hivedb_dict['jdbc_connection_suffix']
    server = hivedb_dict['server_name']
    port = hivedb_dict['server_port']
    dbname = hivedb_dict['database_name']
    user = hivedb_dict['database_user']
    pwd = hivedb_dict['database_password']
    lib = hivedb_dict['driver_library']

    connection_string = "{}{}:{}/{}{}".format(connection_prefix, server, port, dbname,
                                              connection_suffix)

    # Connect to data quality database
    num_conn_attempts = 0
    while (num_conn_attempts < MAX_ATTEMPTS):
        try:
            logger.info('Connecting to hive server ' + server + ' as ' + user + '...')
            hivedb_conn = jaydebeapi.connect(driver, connection_string, [user, pwd], lib)
        except DatabaseException as err:
            if num_conn_attempts < MAX_ATTEMPTS:
                logger.error("Database Exception %s connecting - %s ... reconnecting" % (
                    err.error_type, err.message))
                num_conn_attempts += 1
            else:
                logger.error("Database Exception %s connecting - %s ... terminating" % (
                    err.error_type, err.message))
                raise
        else:
            logger.info("Connected to Hive")
            hive_last_operation_time = time.time()
            return


def execute_hive_query(sql_string, print_string):
    """
    Execute the Hive SQL query.
    :return: Open cursor based on the SQL query,  call cursor.fetchall() or cursor.fetchone() to
    actually get the data in it.
    :rtype: :class:`jaydebeapi.Cursor`
    """
    global hive_last_operation_time
    num_conn_attempts = 0

    # Refresh hive connection if timeout has elapsed
    recycle_hive_connection()

    cursor = hivedb_conn.cursor()
    while (num_conn_attempts < MAX_ATTEMPTS):
        try:
            logger.info("%s" % print_string)
            logger.debug("SQL:\n%s" % sql_string)
            cursor = hivedb_conn.cursor()
            cursor.execute(sql_string)
        except DatabaseException as err:
            if (num_conn_attempts < MAX_ATTEMPTS):
                logger.error(
                    "Database Exception %s - %s ... retrying" % (err.error_type, err.message))
                num_conn_attempts += 1
            else:
                logger.error("Database Exception executing query ... terminating")
                raise
        else:
            break
    hive_last_operation_time = time.time()
    return cursor


def recycle_hive_connection():
    """
    Reconnect to the Hive database if the connection has timed out.
    :return: Open connection to the Hive database
    :rtype: :class:`jaydebeapi.Connection`
    """
    current_time = time.time()
    time_since_last_op = current_time - hive_last_operation_time
    if time_since_last_op > hive_conn_timeout:
        logger.info("Recycling connection (time since last operation was %s secs)." % str(
            int(time_since_last_op)))
        if not hivedb_conn._closed:
            hivedb_conn.close()
        connect_to_hive()


# ------------------------------------------------------
# Database & pandas I/O
# ------------------------------------------------------

def create_engine_to_sqldw():
    """
    Create SQLAlchemy engine, which is used to create connections to the SQL DW
    :return: an engine which can create connections to the database
    :rtype: :class:`~sqlalchemy.engine.base.Engine`
    """
    global sqldw_last_operation_time
    # Populate local connection variables from the config
    try:
        odbc_dsn = sqldw_dict['odbc_dsn']
        dbname = sqldw_dict['database_name']
        # Glet resource group if database user does not exist
        if "database_user" not in sqldw_dict:
            grp = sqldw_dict["default_resource_group"]
            account_details = sqldw_dict["resource_group"][grp]
            user = account_details['database_user']
            pwd = account_details['database_password']
        else:
            user = sqldw_dict['database_user']
            pwd = sqldw_dict['database_password']
    except KeyError as e:
        raise MetadataException(
            "MissingConfigValue",
            "Missing configuration key %s when creating control framework class." % e.args[0])

    pyodbc_string = "DSN={};UID={};PWD={};DATABASE={}".format(odbc_dsn, user, pwd, dbname)

    num_conn_attempts = 0
    while num_conn_attempts < MAX_ATTEMPTS:
        try:
            params = urllib.parse.quote_plus(pyodbc_string)
            connection_string = "mssql+pyodbc:///?autocommit=true&odbc_connect=%s" % params

            logger.info('Connection string to SQL DW: {}'.format(connection_string))
            engine = create_engine(connection_string, pool_recycle=3600, poolclass=NullPool)
        except DatabaseException as err:
            if (num_conn_attempts < MAX_ATTEMPTS):
                logger.error("Database Exception %s connecting - %s ... reconnecting" % (
                    err.error_type, err.message))
                num_conn_attempts += 1
            else:
                logger.error("Database Exception %s connecting - %s ... terminating" % (
                    err.error_type, err.message))
                raise
        else:
            logger.info("Engine created for SQL DW")
            sqldw_last_operation_time = time.time()
            return engine


def check_sqldw_connection(job_instance=None):
    """
    Check we can connect to SQL Data Warehouse.
    :param job_instance: The job instance so we can record if an error occurred during the process
    :type job_instance: :class:`ControlFramework.JobInstance`
    :return: an engine which can create connections to the database
    :rtype: :class:`~sqlalchemy.engine.base.Engine`
    """
    try:
        sqldw_engine = create_engine_to_sqldw()
        conn = sqldw_engine.connect()
        conn.close()
        sqldw_engine.dispose()
    except Exception as e:
        logger.error('Could not connect to SQL DW')
        if job_instance:
            job_instance.fail(str(e))
        exit(1)
    return sqldw_engine


def dataframe_from_database(sql_string, table, col_names=None):
    """
    Create a pandas dataframe from the data in the Hive database.
    A raw SQL query is used to get the data.

    Uses an active hive database connection.

    :param sql_string: SQL Query to execute to retrieve data from the database
    :type sql_string: str
    :param table: Table you are retrieving. This will also require the table's schema.
    :type table: str
    :param col_names: Names of the columns to set on the dataframe.
    If this is set to None, all columns will be retrieved from the database
    and used as the column names for the dataframe.
    :type col_names: List of str
    :return: Pandas dataframe generated from the SQL string provided to the database.
    Note that the dataframe's types may be incorrect.
    :rtype: :class:`pandas.DataFrame`
    """
    start_time = datetime.now()
    if col_names is None:
        cursor = execute_hive_query("describe {}".format(table), "Get columns for {}".format(table))
        columns = [row[0] for row in cursor.fetchall()]
    else:
        columns = col_names
    cursor = execute_hive_query(sql_string, "Get data from {}".format(table))
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    time_taken = datetime.now() - start_time
    logger.info("Retrieving {} took {} seconds".format(table, time_taken.total_seconds()))
    return df

# COMMAND ----------

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

# COMMAND ----------

SRC_SCHEMA = "imprd001_rawvault"
DEST_SCHEMA = "imprd001_analytics"
hive_last_operation_time = 1
sqldw_last_operation_time = 1
hive_conn_timeout = 300
sqldw_conn_timeout = 300
logger = 1

# All the latest components
component_cols = ["component_pathname", "component_alias",
                  "component_class", "component_parent_id", "component_clone_id",
                  "component_patch_number", "changetype"]
component_table = "{}.sat_doms_component_header_details_cv".format(SRC_SCHEMA)
component_sql = COMPONENT_QUERY.format(",".join(component_cols), SRC_SCHEMA)
print('component_sql')
print(component_sql)

components = sqlContext.sql(component_sql)


# Map component class definitions to get the asset type
component_def_cols = ["component_class_index", "component_abbreviation",
                      "component_class_name"]
component_def_table = "{}.sat_doms_component_class_defn_details_cv".format(SRC_SCHEMA)
component_def_sql = ("select {0} from {1} `a` inner join " +
                     "{2}.hub_doms_component_class_defn `b` on `a`.{3} = `b`.{3} where changetype != 'D'").format(
    ",".join(component_def_cols), component_def_table, SRC_SCHEMA,
    "hub_doms_component_class_defn_hsh_key")

print('component_def_sql')
print(component_def_sql)

component_def = sqlContext.sql(component_def_sql)


# COMMAND ----------

components = components[components["changetype"] != "D"]

# COMMAND ----------

components = components.toPandas()
display(components)

# COMMAND ----------

component_def = component_def.toPandas()
display(component_def)

# COMMAND ----------

components = pd.merge(components, component_def, left_on="component_class",
                      right_on="component_class_index", how="left")
components = components.rename(columns={"component_abbreviation": "ASSET_TYPE"})
components["ASSET_TYPE"] = components["ASSET_TYPE"].replace("", np.nan)
# No abbreviation, use the actual class name
components["ASSET_TYPE"] = components["ASSET_TYPE"].fillna(
    components["component_class_name"])
components["ASSET_TYPE"] = components["ASSET_TYPE"].astype("category")

# Get the clones and add it to our components dataframe
clones = components[["component_id", "hub_doms_component_hsh_key", "component_pathname",
                     "component_alias"]]

clones = clones.rename(
    columns={"hub_doms_component_hsh_key": "CLONE_HUB_DOMS_COMPONENT_HSH_KEY",
             "component_pathname": "CLONE_PATHNAME",
             "component_alias": "CLONE_ALIAS",
             "component_id": "CLONE_ID"})
components = pd.merge(components, clones, how="left", left_on="component_clone_id",
                      right_on="CLONE_ID")

# Get the parent of our components and add it to our components dataframe
parents = components[["component_id", "hub_doms_component_hsh_key", "component_pathname",
                      "component_alias"]]

parents = parents.rename(
    columns={"hub_doms_component_hsh_key": "PARENT_HUB_DOMS_COMPONENT_HSH_KEY",
             "component_pathname": "PARENT_PATHNAME",
             "component_alias": "PARENT_ALIAS",
             "component_id": "PARENT_ID"})
components = pd.merge(components, parents, how="left", left_on="component_parent_id",
                      right_on="PARENT_ID")

# COMMAND ----------

display(components)

# COMMAND ----------

#components.dropna(subset="ASSET_TYPE")

# COMMAND ----------

#components[components.dropna(subset="ASSET_TYPE")]

# COMMAND ----------

display(components)

# COMMAND ----------

components_nan = components[components.ASSET_TYPE.isnull()]

# COMMAND ----------

display(components_nan)

# COMMAND ----------

components =pd.DataFrame(components)
components

# COMMAND ----------

components = components[components.ASSET_TYPE != ' ']
display(components)

# COMMAND ----------

#components=components.dropna(subset="ASSET_TYPE")

# COMMAND ----------

display(components)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from imprd001_rawvault.sat_doms_component_class_defn_details_cv where component_abbreviation is null
# MAGIC --where HUB_DOMS_COMPONENT_HSH_KEY='37a0366cc0ad5c625084349e9fe95b5d6dab8e2a'
