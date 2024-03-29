from influxdb import InfluxDBClient
import yaml
import os


class MetricsRecorder(InfluxDBClient):
    """ Generates an object for recording time series data """
    DEFAULT_BATCH_SIZE = 1000
    _db_settings = None

    def __init__(self):
        """
        Initializes an instance of MetricsRecorder
        """

        # see if the settings were previously imported, import if needed
        if MetricsRecorder._db_settings is None:
            current_dir = os.path.dirname(os.path.realpath(__file__))
            settings_dir = os.path.join(current_dir, "db_settings.yaml")
            with open(settings_dir, "r") as stream:
                MetricsRecorder._db_settings = yaml.safe_load(stream)
        settings = MetricsRecorder._db_settings

        super().__init__(host=settings["host"],
                         port=settings["port"],
                         username=settings["user"],
                         password=settings["password"])

        # check if the database exists, create if not
        databases = self.get_list_database()
        if settings["database"] not in databases:
            self.create_database(settings["database"])

        self._database = settings["database"]

    def record_metrics(self, points, batch_size=DEFAULT_BATCH_SIZE):
        """
        Records metrics into the database

        Each point must follow this schema:
        {"measurement": "measurement",
         "time": "time",
         "fields": {"key1": "val1", "key2": "val2", ...}
        }

        :param points: The points to be recorded into the database
        :type points: List of dictionaries
        :param batch_size: (Optional) The batch size, default value is DEFAULT_BATCH_SIZE
        :returns: True if the points are successfully recorded
        :rtype: Boolean
        """
        database = MetricsRecorder._db_settings["database"]
        return self.write_points(points, database=database, batch_size=batch_size)
