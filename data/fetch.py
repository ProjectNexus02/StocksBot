from urllib.parse import urlencode
from metrics.metrics_recorder import MetricsRecorder
import requests
import yaml
import os
import re
import json


class DataFetcher:
    """ Generates an object for fetching data from external apis """

    _api_keys = None

    # Alpha Vantage
    ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"
    DEFAULT_FUNCTION = "TIME_SERIES_INTRADAY"
    DEFAULT_INTERVAL = "1min"
    DEFAULT_OUTPUT_SIZE = "full"

    def __init__(self):
        """
        Initialize an instance of DataFetcher
        """

        # check if the api keys were previously imported, import if needed
        if DataFetcher._api_keys is None:
            current_dir = os.path.dirname(os.path.realpath(__file__))
            settings_dir = os.path.join(current_dir, "api_keys.yaml")
            with open(settings_dir, "r") as stream:
                DataFetcher._api_keys = yaml.safe_load(stream)
        self.__keys = DataFetcher._api_keys

    def fetch_from_alphavantage(self, equity, function=DEFAULT_FUNCTION, interval=DEFAULT_INTERVAL,
                                outputsize=DEFAULT_OUTPUT_SIZE, save_to_db=True):
        """
        Fetches time series of the equity specified from the Alpha Vantage api

        More details about the Alpha Vantage api can be found on https://www.alphavantage.co/documentation/

        :param equity: Name of the equity
        :type equity: String
        :param function: (Optional) The time series to query, default value is DEFAULT_FUNCTION
        :type function: String
        :param interval: (Optional) The interval between two consecutive data points, default value is DEFAULT_INTERVAL
        :type interval: String
        :param outputsize: (Optional) The size of the time series data, default value is DEFAULT_OUTPUT_SIZE
        :type outputsize: String
        :param save_to_db: (Optional) Specifies whether the data is to be stored to the database, default value is True
        :type save_to_db: Boolean
        :return: The data returned by the api call
        :rtype: Object
        """
        base_url = self.ALPHA_VANTAGE_URL
        api_key = self.__keys["alphavantage"]
        query_params = {
            "function": function,
            "symbol": equity,
            "interval": interval,
            "outputsize": outputsize,
            "datatype": "json",  # use JSON instead of CSV
            "apikey": api_key
        }
        response = self.__request(base_url, query_params)
        data = response.json()

        if save_to_db:
            key = f"Time Series ({interval})"
            time_series = data.get(key)
            if time_series is None:
                print(f"Could not get points from key: {key}")
                print("Data will not be recorded into the database.")
            else:
                measurement = f"{equity}_stocks"
                recorder = MetricsRecorder()

                points = [
                    {"measurement": measurement,
                     "time": time,
                     "fields": self.__remove_separator(time_series[time])}
                    for time in time_series
                ]
                success = recorder.record_metrics(points)
                if success:
                    print("Data was successfully recorded into the database.")
                else:
                    print("Data was unsuccessfully recorded into the database.")
        return data

    @staticmethod
    def __remove_separator(dict_obj):
        """
        Removes the separators used to enumerate items in a dictionary

        e.g. {"1. key1": "val1", "2. key2": "val2"} -> {"key1": "val1", "key2": "val2"}

        :param dict_obj: The dictionary
        :type dict_obj: Dictionary
        :return: The dictionary with separators removed
        :rtype: Dictionary
        """
        pattern = "^\d+\s*[-\\.)]?\s+"
        return {re.sub(pattern, "", key): dict_obj[key] for key in dict_obj}

    @staticmethod
    def __request(base_url, query_params):
        """
        Makes a request to the base_url with the given query_params
        :param base_url: The base url for the request
        :type base_url: String
        :param query_params: The parameters for the request
        :type query_params: Dictionary
        :return: The content returned by the url request
        :rtype: Response
        """
        url = f"{base_url}?{urlencode(query=query_params)}"
        try:
            response = requests.get(url)
            return response
        except Exception as e:
            raise Exception(f"An error has occurred trying to connect to {url}. Got the following error: {e}")
