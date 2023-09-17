import duckdb
import logging
from pathlib import Path
import os


def connect(db, db_name, dbpath=None):
    """
    Connects to a database and returns a connection object.

    Parameters:
        db (str): The type of database to connect to. Valid values are "duckdb" and any other string.
        db_name (str): The name of the database.
        dbpath (str, optional): The path to the database folder. Defaults to None.
   
    Returns:
        Connection: A connection object for the connected database.

    Raises:
        Exception: If there is an error creating the database folder.
        Exception: If there is an error connecting to the database.
    """
    if db == "duckdb":
        try:
            Path(dbpath).mkdir(exist_ok=True)
        except Exception as e:
            logging.critical(f"Can not create folder {dbpath}")
            raise
        try:
            return duckdb.connect(f"{dbpath}{db_name}")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)
    else:
        # Need motherduck code in here
        MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
        try:
            return duckdb.connect(f"md:{db_name}?motherduck_token={MOTHERDUCK_TOKEN}")
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            print(e)


def df_load(conn, df_upload, sqlfile, sqlschema, sqlwrite="append"):
    try:
        sql = f"CREATE SCHEMA IF NOT EXISTS {sqlschema}"
        conn.sql(sql)
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(e)

    try:
        if sqlwrite == "replace":
            sql = f"CREATE OR REPLACE TABLE {sqlschema}.{sqlfile} AS SELECT * FROM df_upload"
        else:
            sql = f"INSERT INTO {sqlschema}.{sqlfile} SELECT * FROM df_upload"

        logging.info(f"- SQL: {sql}")
        conn.sql(sql)

        sql = f"SELECT * FROM {sqlschema}.{sqlfile}"
        result = conn.sql(sql).df()
        logging.info(f"- SQL Result: {sqlschema}.{sqlfile}: {result.shape}")
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(e)
