import pytest
import requests
import tempfile
import shutil
import os
import pandas as pd
from unittest.mock import patch, MagicMock
from dependencies.etl import ExtractData, TransformData, PostgresLoader  # Replace with actual module name
from pyspark.sql import SparkSession



def test_download_kaggle_dataset():
    extractor = ExtractData("user", "key", "path.zip", ["dataset1"], "https://example.com/")

    with patch.object(extractor, 'extract_from_api') as mock_extract:
        extractor.download_kaggle_dataset()
        mock_extract.assert_called_once_with("https://example.com/dataset1", "dataset1")


def test_create_spotify_dataframe():
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()

    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    album_path = os.path.join(temp_dir, "albums.csv")
    track_path = os.path.join(temp_dir, "tracks.csv")

    pd.DataFrame({"track_id": [1, 2], "duration_ms": [200000, 180000],
                  "release_date": ["2024-01-01 00:00:00 UTC", "2023-12-01 00:00:00 UTC"],
                  "label": ["Label1", "Label2"]}).to_csv(album_path, index=False)
    pd.DataFrame({"id": [1, 2], "track_popularity": [60, 40], "explicit": [False, False]}).to_csv(track_path,
                                                                                                  index=False)

    transformer = TransformData(spark, temp_dir, "albums.csv", "tracks.csv")

    albums_df, tracks_df = transformer.create_spotify_dataframe()

    assert albums_df.count() > 0
    assert tracks_df.count() > 0

    # Clean up temporary files
    shutil.rmtree(temp_dir)


def test_write_to_postgres():
    loader = PostgresLoader("jdbc:postgresql://localhost:5432/db", {"user": "user", "password": "pass"})
    df_mock = MagicMock()

    with patch.object(df_mock.write, 'jdbc') as mock_jdbc:
        loader.write_to_postgres(df_mock, "test_table")
        mock_jdbc.assert_called_once()
