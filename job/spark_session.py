import os
from dependencies.logging import LoggerWrapper
from dependencies.spark import StartSparkSession
logger = LoggerWrapper(name="SparkSessionLogger")

def start_spark_session(config):
    """
    Starts a Spark session with required dependencies.

    :param config: Takes Config Key-value pairs.
    :return: Spark session objects.
    """
    try:
        logger.info("Starting Spark session...")
        spark = StartSparkSession(jars=os.path.abspath(config["spark"]["jars"]))
        spark_session = spark.start_spark()
        logger.info("Spark session started successfully.")
        return spark, spark_session
    except Exception as e:
        logger.error(f"Error starting Spark session: {e}")
        raise Exception(f"Error starting Spark session: {e}")