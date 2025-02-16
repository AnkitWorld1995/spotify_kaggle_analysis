import requests
import zipfile
import os
from dependencies import spark
from pyspark.sql.functions import col, round, to_date, when
from dependencies.logging import LoggerWrapper


class ExtractData:

    def __init__(self, username, key, extract_path, dataset_id_list, base_url):
        self.username = username
        self.key = key
        self.extract_path = extract_path
        self.base_url = base_url
        self.dataset_id_list = dataset_id_list
        self.logger = LoggerWrapper(name="MyAppLogger")

    def download_kaggle_dataset(self):
        """
         Download The Kaggle Dataset Using kaggle API following Rest
         protocols and save it to local Directory.
        :return:
        """

        for filename in self.dataset_id_list:
            url = f"{self.base_url}{filename}"
            try:
                self.extract_from_api(url, filename)
            except Exception as e:
                raise RuntimeError(f"Data Extraction From API For File: {filename} failed with exception: {e}")

    def extract_from_api(self, dataset_url, file_name):

        """
            Extracts Datasets From Kaggle API And Save It As
            A CSV File Local Directory.

        :param dataset_url: Kaggle Dataset URL For Extraction
        :param file_name: Kaggle File Name.
        :return: Writes The CSV Data To Local Directory.
        """
        auth = (self.username, self.key)
        response = requests.get(dataset_url, auth=auth, stream=True)

        if response.status_code == 200:
            with open(self.extract_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            self.logger.info(f"Dataset downloaded successfully to: {self.extract_path}")
        else:
            self.logger.error(f"Failed to download dataset. Status Code: {response.status_code}")

        with zipfile.ZipFile(self.extract_path, "r") as zip_ref:
            all_files = zip_ref.namelist()
            for file in all_files:
                if file == file_name:
                    zip_ref.extract(file, "./data")
                    self.logger.info(f"Extracted: {file}")

        os.remove(self.extract_path)


class TransformData:
    def __init__(self, spark_session, extract_path, album_dataset_id, track_dataset_id):
        self.spark = spark_session
        self.extract_path = extract_path
        self.album_dataset = album_dataset_id
        self.track_dataset = track_dataset_id

    def create_spotify_dataframe(self):
        """

        :return:  Album Dataframe & Track Dataframe
        """

        al_path = f"{self.extract_path}/{self.album_dataset}"
        tr_path = f"{self.extract_path}/{self.track_dataset}"

        spotify_albums_df = self.spark \
            .read \
            .option('header', True) \
            .option('inferSchema', True) \
            .csv(al_path)

        spotify_tracks_df = self.spark \
            .read \
            .option('header', True) \
            .option('inferSchema', True) \
            .csv(tr_path)

        return spotify_albums_df, spotify_tracks_df

    @staticmethod
    def clean_transform_dataframes(spotify_albums_df, spotify_tracks_df):
        """
        Cleans the Album and Track DataFrames.

        :param spotify_albums_df: Album Dataframe
        :param spotify_tracks_df: Track Dataframe
        """
        cleaned_tracks_df = spotify_tracks_df.dropna()

        updated_spotify_albums_df = (
            spotify_albums_df
            .withColumn("duration_min", round(col("duration_ms").cast('float') / 60000, 2))
            .withColumn('release_date', to_date(col("release_date"), "yyyy-MM-dd HH:mm:ss 'UTC'"))
            .withColumn("radio_mix", when(col("duration_min") <= 3.00, True).otherwise(False))
            .select(
                'track_id',
                'track_name',
                'duration_min',
                'release_date',
                'label',
                'radio_mix'
            )
        )

        updated_spotify_albums_df_cleaned = updated_spotify_albums_df.dropna()

        return updated_spotify_albums_df_cleaned, cleaned_tracks_df

    @staticmethod
    def create_master_dataframe(updated_spotify_albums_df_cleaned, cleaned_tracks_df):
        """

        :param updated_spotify_albums_df_cleaned:
        :param cleaned_tracks_df:
        :return: Master Dataframe Combination of Album and Track Dataframes
        """

        master_df = (
            updated_spotify_albums_df_cleaned
            .join(cleaned_tracks_df, updated_spotify_albums_df_cleaned.track_id == cleaned_tracks_df.id, how='inner')
            .filter(
                (col('track_popularity') > 50) & (col('explicit') == False)
            )
            .select(
                'track_id',
                'track_name',
                'duration_min',
                'release_date',
                'label',
                'track_popularity',
                'radio_mix'
            )
        )

        return master_df


class PostgresLoader:
    def __init__(self, jdbc_url, properties):
        self.jdbc_url = jdbc_url
        self.properties = properties
        self.logger = LoggerWrapper(name="MyAppLogger")

    def write_to_postgres(self, df, table_name):
        """
        Writes a Spark DataFrame to a PostgreSQL table.

        :param df: Spark DataFrame
        :param table_name: Table name
        """

        try:
            df.write.jdbc(url=self.jdbc_url, table=table_name, mode="overwrite", properties=self.properties)
            self.logger.info(f"Data written to PostgreSQL table: {table_name}")
        except Exception as e:
            self.logger.error(f"Failed to write data to PostgreSQL table: {table_name}")
            raise RuntimeError(f"Failed to write data to PostgreSQL table: {table_name}") from e
