### START FUNCTION
"""
data_ingestion_helpers module

Provides functions for interacting with databases and reading CSV files from web URLs.

This module uses logging to track progress and errors.
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text

# Configure logger for the module
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def create_db_engine(db_path):
    """
    Creates a database engine object for the specified database path.

    Args:
        db_path (str): The path to the database file.

    Returns:
        sqlalchemy.engine.Engine: The created engine object.

    Raises:
        ImportError: If SQLAlchemy is not installed.
        RuntimeError: If engine creation fails.
    """

    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e


def query_data(engine, sql_query):
    """
    Executes an SQL query on the given database engine and returns the results as a DataFrame.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine object.
        sql_query (str): The SQL query to execute.

    Returns:
        pandas.DataFrame: The DataFrame containing the query results.

    Raises:
        ValueError: If the query returns an empty DataFrame.
        RuntimeError: If query execution fails.
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e


def read_from_web_CSV(URL):
    """
    Reads a CSV file from the specified web URL and returns it as a DataFrame.

    Args:
        URL (str): The URL of the CSV file.

    Returns:
        pandas.DataFrame: The DataFrame containing the CSV data.

    Raises:
        pd.errors.EmptyDataError: If the URL doesn't point to a valid CSV file.
        RuntimeError: If reading the file fails.
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e


    
### END FUNCTION