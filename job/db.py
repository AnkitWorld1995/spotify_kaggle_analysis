
from dependencies.logging import LoggerWrapper
from dependencies.db_connection import Postgresql

logger = LoggerWrapper(name="DBLogger")

def get_postgres_connection(config):
    """
    Establishes a PostgresSQL connection.
    :param config: Take Configuration Key-Values.
    :return: Postgres Connection object.
    """
    try:
        logger.info(f"Connecting to PostgresSQL database: {config['postgres']['database']}")

        return Postgresql(
            host=config["postgres"]["host"],
            port=config["postgres"]["port"],
            db_name=config["postgres"]["database"],
            user_name=config["postgres"]["user"],
            password=config["postgres"]["password"]
        )

    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        raise Exception(f"Error connecting to PostgreSQL: {e}")


def setup_postgres_schema(pg_database, config):
    """
    Creates PostgreSQL schema and table.
    :param pg_database: Postgres Database object.
    :param config: Take Configuration Key-Values.
    :return: None

    """
    try:
        logger.info("Setting up PostgreSQL schema and tables...")
        pg_database.drop_table(config["postgres"]["schema"], config["postgres"]["table"])
        pg_database.drop_schema(config["postgres"]["schema"])
        pg_database.create_schema(config["postgres"]["schema"])
        logger.info("PostgreSQL schema setup completed.")
    except Exception as e:
        logger.error(f"Error in PostgreSQL schema setup: {e}")
        raise Exception(f"Error in PostgreSQL schema setup: {e}")
