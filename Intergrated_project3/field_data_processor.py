### START FUNCTION

from sqlalchemy import create_engine
import pandas as pd
from data_ingestion import create_db_engine, query_data, read_from_web_CSV
import logging


class FieldDataProcessor:

    def __init__(self, config_params, logging_level="INFO"): # When we instantiate this class, we can optionally specify what logs we want to see

        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_map_data = config_params['weather_mapping_csv']
        self.initialize_logging(logging_level)

        # We create empty objects to store the DataFrame and engine in
        self.df = None
        self.engine = None
    # This method enables logging in the class.
    def initialize_logging(self, logging_level):
        """
        Sets up logging for this instance of FieldDataProcessor.
        """
        logger_name = __name__ + ".FieldDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevents log messages from being propagated to the root logger

	 # Set logging level
        if logging_level.upper() == "DEBUG":
            log_level = logging.DEBUG
        elif logging_level.upper() == "INFO":
            log_level = logging.INFO
        elif logging_level.upper() == "NONE":  # Option to disable logging
            self.logger.disabled = True
            return
        else:
            log_level = logging.INFO  # Default to INFO

        self.logger.setLevel(log_level)

        # Only add handler if not already added to avoid duplicate messages
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
	# Use self.logger.info(), self.logger.debug(), etc.


    # let's focus only on this part from now on
    def ingest_sql_data(self):
        """
        Extracts data from the SQLite database using the specified query and returns it as a DataFrame.

        Returns:
        pandas.DataFrame: The DataFrame containing the retrieved data.
         """

        self.logger.info("Initializing data ingestion from SQLite database.")

        try:
            # Create database engine
            self.engine = create_engine(self.db_path)
		 # Execute SQL query
            self.df = query_data(self.engine, self.sql_query)

            # Success message
            self.logger.info("Data successfully loaded from SQLite database.")

            return self.df

        except Exception as e:
            # Handle errors
            self.logger.error(f"Error during data ingestion: {e}")
            raise e
    
    def rename_columns(self):
        """
        Renames specified columns in the DataFrame.
        """

        # Extract the columns to rename from the configuration
        column1, column2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]

        # Temporarily rename one of the columns to avoid a naming conflict
        temp_name = "__temp_name_for_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        # Perform the swap using a temporary name
        self.df = self.df.rename(columns={column1: temp_name, column2: column1})
        self.df = self.df.rename(columns={temp_name: column2})

        self.logger.info(f"Swapped columns: {column1} with {column2}")
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies corrections to specified columns in the DataFrame.

        Args:
            column_name (str, optional): The name of the column containing crop types to correct. Defaults to 'Crop_type'.
            abs_column (str, optional): The name of the column containing values to take the absolute value of. Defaults to 'Elevation'.
        """

        self.df[abs_column] = self.df[abs_column].abs()  # Take the absolute value of the 'Elevation' column
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))  # Correct crop names
    def apply_corrections(self, column_name='Crop_type', abs_column='Elevation'):
        """
        Applies corrections to specified columns in the DataFrame.

        Args:
            column_name (str, optional): The name of the column containing crop types to correct. Defaults to 'Crop_type'.
            abs_column (str, optional): The name of the column containing values to take the absolute value of. Defaults to 'Elevation'.
        """

        self.df[abs_column] = self.df[abs_column].abs()  # Take the absolute value of the 'Elevation' column
        self.df[column_name] = self.df[column_name].apply(lambda crop: self.values_to_rename.get(crop, crop))  # Correct crop names

    def weather_station_mapping(self):
        weather_map_df = read_from_web_CSV(self.weather_map_data)
        self.df = self.df.merge(weather_map_df, on='Field_ID', how='left')

    def process(self):
        """
        Processes the data by calling the individual methods in the correct order
        and performing additional cleaning steps.

        Returns:
            pandas.DataFrame: The final processed DataFrame.
        """

        # Call methods to perform data ingestion, renaming, correction, and station mapping
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        field_df = self.weather_station_mapping()

        # Drop any unnecessary columns (e.g., "Unamed:0")
        if field_df is not None:  
            field_df.drop(columns="Unamed:0", inplace=True)

### END FUNCTION