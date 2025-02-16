import pytest
from pyspark.sql import SparkSession
from unittest.mock import patch, MagicMock
from dependencies.spark import StartSparkSession


def test_start_spark():
    jars = "path/to/jar1.jar,path/to/jar2.jar"
    spark_session_manager = StartSparkSession(jars)

    # Mock logger to avoid actual logging
    with patch.object(spark_session_manager.logger, 'info') as mock_info, \
            patch.object(spark_session_manager.logger, 'error') as mock_error:
        spark = spark_session_manager.start_spark()
        assert isinstance(spark, SparkSession)
        assert spark_session_manager.spark is not None
        mock_info.assert_called_with("Spark session created successfully.")


def test_stop_spark():
    jars = "path/to/jar1.jar,path/to/jar2.jar"
    spark_session_manager = StartSparkSession(jars)
    spark_session_manager.start_spark()

    with patch.object(spark_session_manager.logger, 'info') as mock_info:
        spark_session_manager.stop_spark()
        assert spark_session_manager.spark is None
        mock_info.assert_called_with("Spark session stopped successfully.")


def test_stop_spark_no_active_session():
    jars = "path/to/jar1.jar,path/to/jar2.jar"
    spark_session_manager = StartSparkSession(jars)

    with patch.object(spark_session_manager.logger, 'warn') as mock_warn:
        spark_session_manager.stop_spark()
        mock_warn.assert_called_with("No active Spark session found to stop.")
