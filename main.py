import os
import sys
import logging
from dependencies.etl import ExtractData, PostgresLoader, TransformData
from dependencies.spark import StartSparkSession
from config.config import validate_config
from job.db import get_postgres_connection, setup_postgres_schema
from job.spark_session import start_spark_session
from job.extractor import extract_data
from job.transformer import transform_data
from job.visualize_date import display_transformed_data
from job.loader import load_data_to_postgres
from job.stop_services import stop_services

# Configure logging
from dependencies.logging import LoggerWrapper
logger = LoggerWrapper(name="AppLogger")


def main(config_path):
    """
    Main function to execute ETL pipeline.

    """
    config = validate_config(config_path)
    extract_data(config)

    spark, spark_session = start_spark_session(config)
    cleaned_albums_df, cleaned_tracks_df, master_dataframe = transform_data(spark_session, config)

    display_transformed_data(cleaned_albums_df, cleaned_tracks_df, master_dataframe)

    pg_database = get_postgres_connection(config)
    setup_postgres_schema(pg_database, config)
    load_data_to_postgres(master_dataframe, config)

    stop_services(spark, pg_database)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python main.py /spotify-kaggle-analysis/config.yml ")
        sys.exit(1)

    main(sys.argv[1])
