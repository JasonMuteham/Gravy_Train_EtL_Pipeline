import duckdb
import logging
from pathlib import Path
import os
from tenacity import retry, stop_after_attempt, wait_fixed, TryAgain

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def connect(db, db_name, db_path=None):
    """
    Connects to a database and returns a connection object.

    Parameters:
        db (str): The type of database to connect to. Valid values are "duckdb" and any other string.
        db_name (str): The name of the database.
        db_path (str, optional): The path to the database folder. Defaults to None.
   
    Returns:
        Connection: A connection object for the connected database.

    Raises:
        Exception: If there is an error creating the database folder.
        Exception: If there is an error connecting to the database.
    """
    if db == "duckdb":
        if db_path:
            db_name = f"{db_path}/{db_name}"
            try:
                Path(db_path).mkdir(exist_ok=True)
            except Exception as e:
                logging.critical(f"Can not create folder {db_path}")
                raise e
        try:
            return duckdb.connect(f"{db_name}")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)
            raise e
    else:
        # Need motherduck code in here
        MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
        try:
            return duckdb.connect(f"md:{db_name}?motherduck_token={MOTHERDUCK_TOKEN}")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)
            raise e

def checkdb(con):
    """
    Check if httpfs extension is installed 
    """
    sql = """
    SELECT installed, loaded FROM duckdb_extensions() WHERE extension_name = 'httpfs'
    """
    result = con.sql(sql).fetchall()
    httpfs_installed = result[0][0]
    httpfs_loaded = result[0][1]

    if not(httpfs_installed):
        try:
            sql = "INSTALL httpfs;"
            con.sql(sql)
        except Exception as e:
            logging.error(f"Database error installing httpfs: {e}")
    
    if not(httpfs_loaded):    
        try:
            sql = "LOAD httpfs;"
            con.sql(sql)
        except Exception as e:
            logging.error(f"Database error loading httpfs: {e}")
    return

def schema(con, schema):
    try:
        sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
        con.sql(sql)
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(e)
        raise e

def csv_filelist(con, url: str, sql_table):
    sql = f"SELECT * FROM read_csv_auto('{url}')"
    result = con.sql(sql).fetchall()
    replace = True
    for csv_file in result:
        csv(con, csv_file[0], sql_table, schema="staging", replace=replace)
        replace = False
    
def csv(con, url, sql_table, schema="staging", replace=True):
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(2))
    def read_csv(con, url, sql_table, schema, replace):
        try:
            if replace:
                sql = f"CREATE OR REPLACE TABLE {schema}.{sql_table} AS SELECT * FROM read_csv_auto('{url}')"  # noqa: E501
            else:
                sql = f"INSERT INTO {schema}.{sql_table} SELECT * FROM read_csv_auto('{url}')"  # noqa: E501

            logging.info(f"- SQL: {sql}")
            con.sql(sql)
            sql = f"""
                SELECT 
                    COUNT(*),
                    (SELECT column_count FROM duckdb_tables() WHERE schema_name = '{schema}' and table_name = '{sql_table}') as columns 
                FROM {schema}.{sql_table};
            """  # noqa: F541
            result = con.sql(sql).fetchall()
            logging.info(f"- Verifying Uploaded Data - Result: {schema}.{sql_table}: {result}")
        except Exception as e:
            logging.error(f"Database error: {e}")
            raise TryAgain
    try:
        read_csv(con, url, sql_table, schema, replace)
    except TryAgain as e:
        logging.critical(f'- myduck.csv: Retry limit reached: {url}')

def load(con,sql_table,schema_to, schema_from="staging",sql_write="replace",sql_filter=None):
    @retry(stop=stop_after_attempt(3), reraise=True, wait=wait_fixed(2))
    def load_data(con,sql_table,schema_to, schema_from, sql_write, sql_filter):
        try:
            if sql_filter:
                if sql_write == "replace":
                    sql = f"CREATE OR REPLACE TABLE {schema_to}.{sql_table} AS {sql_filter}"  # noqa: E501
                else:
                    sql = f"INSERT INTO {schema_to}.{sql_table} {sql_filter}"  # noqa: E501

            else:
                if sql_write == "replace":
                    sql = f"CREATE OR REPLACE TABLE {schema_to}.{sql_table} AS SELECT * FROM {schema_from}.{sql_table}"  # noqa: E501
                else:
                    sql = f"INSERT INTO {schema_to}.{sql_table} SELECT * FROM {schema_from}.{sql_table}"  # noqa: E501

            logging.info(f"- Loading from: {schema_from}.{sql_table} to: {schema_to}.{sql_table}")  # noqa: E501
            logging.debug(f"- SQL: {sql}")
            con.sql(sql)
            sql = f"""
                SELECT 
                    COUNT(*),
                    (SELECT column_count FROM duckdb_tables() WHERE schema_name = '{schema_to}' and table_name = '{sql_table}') as columns 
                FROM {schema_to}.{sql_table};
            """  # noqa: F541
        #sql = f"SUMMARIZE {schema}.{sql_table}"
            result = con.sql(sql).fetchall()
            logging.info(f"- Verifying Uploaded Data - Result: {schema_to}.{sql_table}: {result}")  # noqa: E501
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)
            raise TryAgain    
       
    try:
        load_data(con,sql_table,schema_to, schema_from, sql_write, sql_filter)
    except TryAgain as e:
        logging.critical(f'- myduck.load: Retry limit reached: {url}')

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def df_load(conn, df_upload, sqlfile, sqlschema, sqlwrite="append"):

    try:
        if sqlwrite == "replace":
            sql = f"CREATE OR REPLACE TABLE {sqlschema}.{sqlfile} AS SELECT * FROM df_upload"
        else:
            sql = f"INSERT INTO {sqlschema}.{sqlfile} SELECT * FROM df_upload"

        logging.info(f"- SQL: {sql}")
        conn.sql(sql)

        sql = f"SELECT * FROM {sqlschema}.{sqlfile}"
        result = conn.sql(sql).df()
        logging.info(f"- Verifying Uploaded Data - Result: {sqlschema}.{sqlfile}: {result.shape}")
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(e)
        raise e