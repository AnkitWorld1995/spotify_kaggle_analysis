
from dependencies.etl import PostgresLoader
from dependencies.logging import LoggerWrapper
logger = LoggerWrapper(name="loaderLogger")

def load_data_to_postgres(master_dataframe, config):
    """
    Loads transformed data into PostgreSQL.
    :parameter master_dataframe: Input Master DataFrame To Load into PostgreSQL Database.
    :return None
    """
    try:
        logger.info("Loading data into PostgreSQL...")
        pg_loader = PostgresLoader(config["postgres"]["url"], {
            "user": config["postgres"]["user"],
            "password": config["postgres"]["password"],
            "driver": config["postgres"]["driver"]
        })
        pg_loader.write_to_postgres(master_dataframe, f"{config['postgres']['schema']}.{config['postgres']['table']}")
        logger.info("Data successfully loaded into PostgreSQL.")
    except Exception as e:
        logger.error(f"Error loading data into PostgreSQL: {e}")
        raise Exception(f"Error loading data into PostgreSQL: {e}")