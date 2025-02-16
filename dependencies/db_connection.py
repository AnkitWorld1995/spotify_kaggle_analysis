import os
import pandas as pd
import numpy as np
import psycopg2
from pandas import DataFrame
from datetime import datetime
from dependencies.logging import LoggerWrapper

CUR_DIR = os.path.abspath(os.path.dirname(__file__))


class DbConnections:
    """
    Database connections Class To connect Postgres DB
    For DML.

    """

    def __init__(self, host: str, port: str, db_name: str, user: str, password: str, database_type: str):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.database_type = database_type
        self.logger = LoggerWrapper(name="MyAppLogger")
        self.conn = self.establish_connection()
        self.cursor = self.conn.cursor()

    def establish_connection(self):
        """
        Establish a connection to the Local Database
        """
        try:
            if self.database_type == "postgresql":
                conn = psycopg2.connect(host=self.host, port=self.port, dbname=self.db_name, user=self.user,
                                        password=self.password)
                conn.set_session(autocommit=True)
                self.logger.info(f'Successfully connected to {self.db_name}!')

                return conn

        except Exception as e:
            self.logger.warn(
                f'Error when connecting to {self.database_type} database! Please check your connection and make '
                f'sure database is running and you have given the correct informations!')
            raise Exception(f'Error when connecting to {self.database_type} database!')

    def execute_query(self, query: str):

        try:
            self.cursor.execute(query)
            self.logger.info('\n == Query Executed Successfully!')

        except Exception as e:
            self.cursor.execute("ROLLBACK")
            self.close_connection()
            self.logger.error(f"Error when executing query: '{e}'")
            raise Exception("There is a problem with your query. Please control it. Marking task as failed! ")

    def truncate_table(self, table_schema: str, table_name: str) -> None:
        query = f"TRUNCATE TABLE {table_schema}.{table_name}"
        self.logger.info(query)
        self.execute_query(query)

    def create_schema(self, schema_name: str) -> None:
        query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
        self.logger.info(query)
        self.execute_query(query)

    def drop_schema(self, schema_name: str) -> None:
        query = f"DROP SCHEMA IF EXISTS {schema_name}"
        self.logger.info(query)
        self.execute_query(query)

    def drop_table(self, schema_name, table_name: str) -> None:
        query = f"DROP TABLE IF EXISTS {schema_name}.{table_name}"
        self.logger.info(query)
        self.execute_query(query)

    def close_connection(self):
        """
        Terminates connection to the database
        """
        self.conn.close()
        self.cursor.close()
        self.logger.info("Connection is successfully terminated!")


class Postgresql(DbConnections):
    def __init__(self, host: str, port: str, db_name: str, user_name: str, password: str):
        super().__init__(host, port, db_name, user_name, password, 'postgresql')
