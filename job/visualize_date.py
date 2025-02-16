from dependencies.logging import LoggerWrapper
logger = LoggerWrapper(name="VizualizeLogger")

def display_transformed_data(cleaned_albums_df, cleaned_tracks_df, master_dataframe):
    """
    Displays transformed data.
    :param cleaned_albums_df: Input cleaned albums dataframe.
    :param cleaned_tracks_df: Input cleaned tracks dataframe.
    :param master_dataframe: Input master dataframe.
    :return:
    """
    try:
        logger.info("Displaying transformed data...")
        cleaned_albums_df.show()
        cleaned_tracks_df.show()
        master_dataframe.show()
    except Exception as e:
        logger.error(f"Error displaying data: {e}")