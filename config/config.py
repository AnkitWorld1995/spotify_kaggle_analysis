import yaml, sys, os
from dependencies.logging import LoggerWrapper

logger = LoggerWrapper(name="ConfigLogger")


def read_config(file_path):
    """
    Reads YAML configuration file and returns a dictionary.

    :parameter: File Path
    :return: Dictionary
    """
    try:
        with open(file_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logger.error(f"Error reading config file: {e}")
        raise RuntimeError(f"Config file error: {e}")


def validate_config(config_path):
    """
    Validates and loads the config file.

    :parameter: Configuration file path From Config File.
    :return: Read Config Function.
    """
    absolute_path = os.path.abspath(config_path)
    if not os.path.exists(absolute_path):
        logger.error("Config file not found. Please provide a valid path.")
        raise RuntimeError(f"Config file not found. Please provide a valid path.")

    logger.info(f"Using config file: {absolute_path}")
    return read_config(absolute_path)
