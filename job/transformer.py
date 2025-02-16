
from dependencies.logging import LoggerWrapper
from dependencies.etl import TransformData
logger = LoggerWrapper(name="TransformerLogger")

def transform_data(spark_session, config):
    """Transforms extracted data."""
    try:
        logger.info("Starting data transformation...")
        transformer = TransformData(
            spark_session,
            config["paths"]["extract_path"],
            config["files"]["albums"],
            config["files"]["tracks"]
        )

        albums_df, tracks_df = transformer.create_spotify_dataframe()
        cleaned_albums_df, cleaned_tracks_df = transformer.clean_transform_dataframes(albums_df, tracks_df)
        master_dataframe = transformer.create_master_dataframe(cleaned_albums_df, cleaned_tracks_df)

        logger.info("Data transformation completed successfully.")
        return cleaned_albums_df, cleaned_tracks_df, master_dataframe

    except Exception as e:
        logger.error(f"Error in data transformation: {e}")
        spark_session.stop()
        raise Exception(e)