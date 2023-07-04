from typing import List, Tuple

import psycopg2

from talos.config.settings import Settings
from talos.exceptions.db import *
from talos.logger import logger


class Database:
    # can assume settings valid per validate() in __init__ of components
    CONFIG = {
        "database": Settings.DB_USER,
        "host": Settings.DB_HOSTNAME,
        "user": Settings.DB_USER,
        "password": Settings.DB_PASSWORD,
        "port": Settings.DB_PORT,
    }

    def __init__(self):
        self.connection: psycopg2.connection = None
        self.cursor: psycopg2.cursor = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self) -> None:
        try:
            self.connection = psycopg2.connect(**self.CONFIG)
            self.cursor = self.connection.cursor()
        except Exception as e:
            logger.exception(
                "An error occured connecting to the database. Potentially resolvable by retry."
            )
            raise DatabaseNonFatalException() from e

    def disconnect(self) -> None:
        try:
            if self.cursor and not self.cursor.closed:
                self.cursor.close()

            if self.connection and not self.connection.closed:
                self.connection.close()
        except Exception as e:
            logger.exception(
                "An error occured closing connections to the database. Potentially resolvable by retry."
            )
            raise DatabaseNonFatalException() from e

    def execute(self, query: str, params: Tuple = None, auto_commit: bool = True) -> None:
        self._validate_connection()

        try:
            self.cursor.execute(query, params)

            if auto_commit:
                self.connection.commit()
        except (psycopg2.OperationalError, psycopg2.InternalError) as nfe:
            logger.exception(
                "An error occured with the database in execute()."
            )
            raise DatabaseNonFatalException() from nfe
        except Exception as e:
            logger.exception(
                "A (likely client-side) fatal error occured calling execute()."
            )
            raise DatabaseFatalException() from e

    def fetchone(self) -> Tuple:
        self._validate_connection()

        try:
            return self.cursor.fetchone()
        except (psycopg2.OperationalError, psycopg2.InternalError) as nfe:
            logger.exception(
                "An error occured with the database in fetchone()."
            )
            raise DatabaseNonFatalException() from nfe
        except Exception as e:
            logger.exception("A fatal error occured calling fetchone().")
            raise DatabaseFatalException() from e

    def fetchall(self) -> List[Tuple]:
        self._validate_connection()

        try:
            return self.cursor.fetchall()
        except (psycopg2.OperationalError, psycopg2.InternalError) as nfe:
            logger.exception(
                "An error occured with the database in fetchall()."
            )
            raise DatabaseNonFatalException() from nfe
        except Exception as e:
            logger.exception("A fatal error occured calling fetchall().")
            raise DatabaseFatalException() from e

    def _validate_connection(self):
        if not self.cursor or not self.connection:
            logger.error(
                "Connection not initialised before attempting queries."
            )
            raise DatabaseNotInitialisedException()
