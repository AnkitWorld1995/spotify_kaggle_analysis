from dependencies.logging import LoggerWrapper
from dependencies.etl import ExtractData
logger = LoggerWrapper(name="extractLogger")

def extract_data(config):
    """
    Extracts data from Kaggle API.
    :param config: Takes Config Key-value pairs.
    :return None
    """
    try:
        logger.info("Starting data extraction...")
        extractor = ExtractData(
            config["kaggle"]["username"],
            config["kaggle"]["key"],
            config["paths"]["extract_zip_path"],
            config["kaggle"]["dataset_id_list"],
            config["kaggle"]["base_url"]
        )
        extractor.download_kaggle_dataset()
        logger.info("Data extraction completed successfully.")
    except Exception as e:
        logger.error(f"Error in data extraction: {e}")
        raise Exception(f"Error in data extraction: {e}")
