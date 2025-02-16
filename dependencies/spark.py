from pyspark.sql import SparkSession
from dependencies.logging import LoggerWrapper
import logging

class StartSparkSession:

    def __init__(self, jars, app_name='my_spark_job', master="local[*]"):
        """
        Initializes the SparkSession configuration.

        :param jars: Path(s) to additional JARs (string or list of strings).
        :param app_name: Name of the Spark application.
        :param master: Cluster connection details (defaults to local[*]).
        """
        self.app_name = app_name
        self.master = master
        self.jars = jars if isinstance(jars, str) else ",".join(jars)
        self.spark = None
        self.logger = LoggerWrapper(name="MyAppLogger")

    def start_spark(self):
        """

        :param app_name: Name of Spark app.
        :param master: Cluster connection details (defaults to local[*]).
        :return: SparkSession object and Spark Logger.
        """
        try:
            self.spark = SparkSession.builder \
                .master(self.master) \
                .appName(self.app_name) \
                .config("spark.jars", self.jars) \
                .getOrCreate()
            self.logger.info("Spark session created successfully.")
        except Exception as e:
            self.logger.error(f"Error creating Spark session: {e}")
            raise
        return self.spark

    def stop_spark(self):

        """
        Stops the Spark session.
        :return:
        """
        try:
            if self.spark is not None:
                self.spark.stop()
                self.logger.info("Spark session stopped successfully.")
                self.spark = None  # Reset the session
            else:
                self.logger.warn("No active Spark session found to stop.")
        except Exception as e:
            self.logger.error(f"Error stopping Spark session: {e}")
            raise

