from dependencies.logging import LoggerWrapper
logger = LoggerWrapper(name="StopServiceLogger")

def stop_services(spark, pg_database):
    """
    Stops Spark and PostgreSQL connections gracefully.

    :param spark: Spark session Object
    :param pg_database: PostgreSQL connection object
    :return: None
    """
    try:
        logger.info("Stopping Spark session...")
        spark.stop_spark()
        logger.info("Spark session stopped successfully.")

        logger.info("Closing PostgreSQL connection...")
        pg_database.close_connection()
        logger.info("PostgreSQL connection closed successfully.")
    except Exception as e:
        logger.error(f"Error stopping services: {e}")
        raise Exception(f"Error stopping services: {e}")