import re
import numpy as np
import pandas as pd
import logging
from data_ingestion import read_from_web_CSV
# Define the WeatherDataProcessor class
class WeatherDataProcessor:

    def __init__(self, config_params, logging_level="INFO"):
        """
        Initializes the WeatherDataProcessor object.

        Args:
            config_params (dict): Dictionary containing configuration parameters.
                - 'weather_csv_path': Path to the weather data CSV file.
                - 'regex_patterns': Dictionary of regular expression patterns for measurement extraction.
            logging_level (str, optional): Logging level ("DEBUG", "INFO", or "NONE"). Defaults to "INFO".
        """

        self.weather_station_data = config_params['weather_csv_path']
        self.patterns = config_params['regex_patterns']
        self.weather_df = None  # Initialize as None or empty DataFrame
        self.initialize_logging(logging_level)

    def initialize_logging(self, logging_level):
        """
        Initializes logging for the class.

        Args:
            logging_level (str): Logging level ("DEBUG", "INFO", or "NONE").
        """

        logger_name = __name__ + ".WeatherDataProcessor"
        self.logger = logging.getLogger(logger_name)
        self.logger.propagate = False  # Prevent propagation to root logger

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

        # Add handler if not already added to prevent duplicates
        if not self.logger.handlers:
            ch = logging.StreamHandler()  # Create console handler
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)

    def weather_station_mapping(self):
        """
        Reads weather station data from the specified path and assigns it to the internal DataFrame.

        Logs a message upon successful loading.
        """

        self.weather_df = read_from_web_CSV(self.weather_station_data)
        self.logger.info("Successfully loaded weather station data from the web.")

        # Optional transformations on `self.weather_df` can be added here.

    def extract_measurement(self, message):
        """
        Attempts to extract measurement and value from a message using the provided patterns.

        Args:
            message (str): The message to extract information from.

        Returns:
            tuple or None: A tuple containing the extracted measurement name and value if found, or None otherwise.
        """

        for key, pattern in self.patterns.items():
            match = re.search(pattern, message)
            if match:
                self.logger.debug(f"Measurement extracted: {key}")
                return key, float(next((x for x in match.groups() if x is not None)))

        self.logger.debug("No measurement match found.")
        return None, None

    def process_messages(self):
        """
        Processes messages in the weather data DataFrame to extract measurements.

        Logs messages indicating success or if the DataFrame is not initialized.

        Returns:
            pandas.DataFrame or None: The processed DataFrame with extracted measurements, or None if not initialized.
        """

        if self.weather_df is not None:
            result = self.weather_df['Message'].apply(self.extract_measurement)
            self.weather_df['Measurement'], self.weather_df['Value'] = zip(*result)
            self.logger.info("Messages processed and measurements extracted.")
        else:
            self.logger.warning("weather_df is not initialized, skipping message processing.")
            return None

        return self.weather_df

    def calculate_means(self):
        """
        Calculates mean values for each weather station and measurement type.

        Logs messages indicating success or if the DataFrame is not initialized.

        Returns:
            pandas.DataFrame or None: A DataFrame containing mean values for each measurement by weather station
        """
        if self.weather_df is not None:
            means = self.weather_df.groupby(by=['Weather_station_ID', 'Measurement'])['Value'].mean()
            self.logger.info("Mean values calculated.")
            return means.unstack()
        else:
            self.logger.warning("weather_df is not initialized, cannot calculate means.")
            return None
    def process(self):
        """
        Processes the weather data by performing the following steps:

            1. Loads weather station data from the specified path.
            2. Processes messages to extract measurements using provided patterns.
            3. Calculates mean values for each weather station and measurement type.

        Logs messages at each stage of the process.

        Returns:
            pandas.DataFrame or None: The processed DataFrame with calculated mean values, or None if initialization failed.
        """

        self.weather_station_mapping()  # Load and assign data to weather_df
        self.process_messages()  # Process messages to extract measurements
        means = self.calculate_means()  # Calculate mean values
        self.logger.info("Data processing completed.")

        return means