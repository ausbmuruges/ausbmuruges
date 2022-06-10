# Databricks notebook source
import logging
import os
import sys
from functools import partial

import pytz
# import yaml
import time
import argparse
import pandas as pd
import numpy as np
import urllib
import uuid

from datetime import datetime, timedelta, date
from dateutil import parser

from pandas.io.sql import SQLTable
# from sqlalchemy.dialects import mssql
# from sqlalchemy import types
# from sqlalchemy import create_engine
# from collections import OrderedDict

# from sqlalchemy.pool import NullPool

# from mlxtend.frequent_patterns import apriori, association_rules
# from mlxtend.preprocessing import TransactionEncoder
# from sklearn.preprocessing import LabelEncoder

# from azure.storage.blob import BlockBlobService

# ------------------------------------------------------
# Edit these constants if required
# ------------------------------------------------------

JDBC_FILEPATH = "../../ausnet-logical-app/linux/edge/app/lib"

os.environ["CLASSPATH"] = "{0}/hive-jdbc-uber-2.6.3.0-235.jar:{0}/mssql-jdbc-7.0.0.jre8.jar".format(
    JDBC_FILEPATH)

# Has to be after the os.environ import
# import jaydebeapi

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

SRC_SCHEMA = "imdev01_rawvault"
DEST_SCHEMA = "imdev01_analytics"

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

component_def = dataframe_from_database(component_def_sql, component_def_table,
                                            col_names=component_def_cols)

# COMMAND ----------

def process(start_date, functions_to_execute, job_instance=None, is_old_data=False):
    """
    Retrieve and process the alarm/event data to be saved into various SQL Data Warehouse tables.
    This will:
        1. Retrieve data from the raw vault in Hive
        2. Process the data into useful analytics
        3. Save the processed data in the SQL Data Warehouse
    :param start_date: The first day of the month, where processing should begin, should be in
    the Melbourne timezone.
    :type start_date: :class: `datetime`
    :param functions_to_execute: Functions that will be called for execution
    :type functions_to_execute: list of str
    :param job_instance: The job instance so we can record if an error occurred during the process
    :type job_instance: :class:`ControlFramework.JobInstance`
    :param is_old_data: Is the data we are processing old data (for e.g. initial loading).
    If so, then we should not process any data that only looks at the latest data
    or any data that gets the latest updated versions of the old data.
    :type is_old_data: bool
    """
    sqldw_engine = check_sqldw_connection(job_instance)

    try:
        #connect_to_hive()

        logger.info("Running functions: {}".format(functions_to_execute))

        melb_tz = pytz.timezone("Australia/Melbourne")

        end_date = get_start_of_next_month(start_date)
        end_date = melb_tz.localize(end_date)

        logger.info("Getting alarm data between {} to {}".format(start_date, end_date))

        pickle_file_name = "doms_analytics-alarms-{}.pkl".format(start_date.strftime("%Y-%m-%d"))

        # Add one extra day before start time and after the end time
        if "get_historic_alarm_log" in functions_to_execute:
            logger.info("Running get_historic_alarm_log")
            start_date_utc = pytz.utc.normalize(start_date - timedelta(days=1))
            end_date_utc = pytz.utc.normalize(end_date + timedelta(days=1))
            alarm_data = get_historic_alarm_log(start_date_utc, end_date_utc)
            print("Executing get_historic_alarm_log")
            if alarm_data.empty:
                terminate_early_no_data(pickle_file_name)
            alarm_data.to_pickle(pickle_file_name)
        if "clean_alarms" in functions_to_execute:
            logger.info("Running clean_alarms")
            if "get_historic_alarm_log" not in functions_to_execute:
                alarm_data = pd.read_pickle(pickle_file_name)
            alarm_data = clean_alarms(alarm_data)
            alarm_data.to_pickle(pickle_file_name)
        if "transform_alarms" in functions_to_execute:
            logger.info("Running transform_alarms")
            if "clean_alarms" not in functions_to_execute:
                alarm_data = pd.read_pickle(pickle_file_name)
            alarm_data = transform_alarms(alarm_data)
            alarm_data.to_pickle(pickle_file_name)
        else:
            alarm_data = pd.read_pickle(pickle_file_name)

        if "judge_if_pheripheral" in functions_to_execute:
            logger.info("Running judge_if_pheripheral")
            # Add association encoding here
            alarm_data.index.name = "ROW_ID"
            alarm_data = alarm_data.reset_index()
            for col in ["refcl_noise", "refcl_related"]:
                matching = judge_if_pheripheral(alarm_data, start_date, end_date, col)
                predicted_col_name = col + "_prediction"
                if matching.empty:
                    alarm_data[predicted_col_name] = False
                else:
                    alarm_data = pd.merge(alarm_data, matching, on="ROW_ID", how="left")
                    alarm_data = alarm_data.rename(columns={"MATCHES": predicted_col_name})
                    alarm_data[predicted_col_name] = alarm_data[predicted_col_name].fillna(False)

            remove_noise_overlap(alarm_data)
            alarm_data.to_pickle(pickle_file_name)

        date_mask = ((alarm_data["event_insert_time_dtm"] >= start_date) &
                     (alarm_data["event_insert_time_dtm"] < end_date))

        # No alarm data between our date range
        if alarm_data[date_mask].empty:
            terminate_early_no_data(pickle_file_name)

        last_alarm_date = alarm_data.loc[alarm_data["event_insert_time_dtm"] < end_date,
                                         "event_insert_time_dtm"].max()
        # Set the update_time_id
        create_update_time(sqldw_engine, start_date.date(), last_alarm_date)

        # We have to pass in the start and end date here, since we need to use the extra data
        # before filtering by the date range
        if "create_statistics" in functions_to_execute:
            logger.info("Running create_statistics")
            create_statistics(sqldw_engine, alarm_data, start_date, end_date)
        if "create_stale_abnormal_alarms" in functions_to_execute:
            logger.info("Running create_stale_abnormal_alarms")
            create_stale_abnormal_alarms(sqldw_engine, alarm_data, start_date, end_date)

        # Only want exactly one month of data
        alarm_data = alarm_data.loc[date_mask, :]

        if "create_acknowledged_alarms" in functions_to_execute:
            logger.info("Running create_acknowledged_alarms")
            create_acknowledged_alarms(sqldw_engine, alarm_data)

        if not is_old_data:
            if "update_old_alarm_data" in functions_to_execute:
                logger.info("Running update_old_alarm_data")
                update_old_alarm_data(sqldw_engine, alarm_data["event_insert_time_dtm"].min())
            if "create_incorrect_field_time" in functions_to_execute:
                logger.info("Running create_incorrect_field_time")
                create_incorrect_field_time(sqldw_engine, alarm_data)

        create_monthly_treemap_data(sqldw_engine, alarm_data, functions_to_execute)
        create_noise_data(sqldw_engine, alarm_data, functions_to_execute)

        # Processing complete, delete the pickle file
        if os.path.exists(pickle_file_name):
            os.remove(pickle_file_name)

        #closing ADB connection at this point seems to have side effect of aborting job
        #if not hivedb_conn._closed:
            #hivedb_conn.close()
    except Exception as e:
        if job_instance:
            job_instance.fail(str(e))
        logger.error(str(e), exc_info=True)
        #if hivedb_conn and not hivedb_conn._closed:
        #    hivedb_conn.close()
        exit(1)


# COMMAND ----------

def get_historic_alarm_log(start, end, update_old_data=False):
    """
    Get the historic alarm log data over a specified period.

    Uses an active hive database connection.

    :param start: Start datetime in UTC for determining the period to get the alarms.
    :type start: :class: `datetime`
    :param end: End datetime in UTC for determining the period to get the alarms
    :type end: :class: `datetime`
    :param update_old_data: Get the older data to use for updating our existing processed data
    that is stored in Azure SQL DW
    :type update_old_data: bool
    :return: Pandas dataframe of the current view of the Historic Alarm log
    :rtype: :class:`pandas.DataFrame`
    """
    col_types = OrderedDict([
        # ('hub_doms_alarm_hsh_key', 'object'),
        # ('alarm_id', 'object'),
        ('alarm_reference', 'int16'),
        ('alarm_value', 'int16'),
        ('alarm_message', 'object'),
        ('alarm_status', 'int8'),
        ('alarm_priority', 'int8'),
        ('alarm_type', 'int8'),
        ('alarm_ack_class', 'int8'),
        ('alarm_ack_action', 'int8'),
        ('alarm_zone_id', 'object'),
        ('superseded_id', 'object'),
        ('alarm_component_id', 'object'),
        # ('alarm_incident_id', 'object'),
        ('alarm_display_class', 'int8'),
        ('domain', 'int8'),
        ('changetype', 'object')])

    columns = [col for col in col_types.keys()]
    datetime_cols = ["alarm_time_dtm", "alarm_ack_time_dtm", "event_insert_time_dtm"]
    columns += datetime_cols

    table = "{}.{}".format(TRANSFORM_SCHEMA, "sat_doms_historic_alarm_log_hv")

    start_time = start.strftime(DATETIME_FORMAT)
    end_time = end.strftime(DATETIME_FORMAT)

    if update_old_data:
        sql = ALARM_UPDATE_QUERY.format(",".join(columns), SRC_SCHEMA, start_time, end_time, TRANSFORM_SCHEMA)
    else:
        sql = ALARM_QUERY.format(",".join(columns), SRC_SCHEMA, start_time, end_time, TRANSFORM_SCHEMA)

    cursor = execute_hive_query(sql, "Get data from {}".format(table))
    alarms = pd.DataFrame(cursor.fetchall(),
                          columns=["hub_doms_alarm_hsh_key", "alarm_id", "extract_date_time",
                                   "load_date_time"] + columns)

    logger.info("Alarm data retrieved")

    #Debugging
    #logger.info("")
    #logger.info("dataframe structure:")
    #logger.info(alarms.dtypes)
    #logger.info("")
    #logger.info("first 10 rows of data")
    #logger.info(alarms.iloc[:10])

    alarms = alarms.astype(col_types)
    # Make dates naive for easier date comparison for dst
    # start = start.replace(tzinfo=None)
    # end = end.replace(tzinfo=None)
    # april_dst_date = datetime(start.year, 4, 1)
    # april_dst_date = april_dst_date + timedelta(days=6 - april_dst_date.weekday())
    # oct_dst_date = datetime(end.year, 10, 1)
    # oct_dst_date = oct_dst_date + timedelta(days=6 - oct_dst_date.weekday())

    for col in datetime_cols:
        # Column is in UTC, convert to Melbourne time
        alarms[col] = pd.to_datetime(alarms[col], utc=True).dt.tz_convert("Australia/Melbourne")

        # ambi = "raise"
        # # Check if within first Sunday of April, transitioning out of daylight savings
        # # i.e. +11:00 to +10:00
        # if start <= april_dst_date <= end:
        #     ambi = np.full(len(alarms), False)
        #
        # # Check if within first Sunday of October, transitioning into Daylight savings
        # # i.e. +10:00 to +11:00
        # if start <= oct_dst_date <= end:
        #     ambi = np.full(len(alarms), True)
        # Column is in Melbourne time
        # alarms[col] = pd.to_datetime(alarms[col]).dt.tz_localize("Australia/Melbourne",
        #                                                          ambiguous=ambi)

    # Extra datetime columns
    alarms["extract_date_time"] = pd.to_datetime(alarms["extract_date_time"]).dt.tz_localize("UTC")
    alarms["load_date_time"] = pd.to_datetime(alarms["load_date_time"]).dt.tz_localize("UTC")

    return alarms


# COMMAND ----------

start_date='2022-05-01 00:00:00+10:0'
end='2022-06-01 00:00:00+10:00'
#functions_to_execute =
get_historic_alarm_log(start_date, end, update_old_data=True)
#job_instance=608694
#process(start_date, functions_to_execute, job_instance=None, is_old_data=True)
