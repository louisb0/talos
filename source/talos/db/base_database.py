from typing import List, Tuple

import psycopg2

from talos.config.settings import Settings
from talos.exceptions.db import *
from talos.logger import logger


class BaseDatabase:
    CONFIG = {
        "database": Settings.DB_USER,
        "host": Settings.DB_HOSTNAME,
        "user": Settings.DB_USER,
        "password": Settings.DB_PASSWORD,
        "port": Settings.DB_PORT,
    }

    def __init__(self):
        """
        Initializes the Database object.
        """
        self.connection: psycopg2.connection = None
        self.cursor: psycopg2.cursor = None

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def connect(self) -> None:
        """
        Establishes a connection to the PostgreSQL database and creates a cursor.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        logger.debug(
            f"Connecting to the database... ({self.CONFIG['database']})")
        if not self.connection or self.connection.closed:
            self.connection = psycopg2.connect(**self.CONFIG)
            logger.debug("Connected to the database.")

        if not self.cursor or self.cursor.closed:
            self.cursor = self.connection.cursor()
            logger.debug("Created the cursor for the database.")

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def disconnect(self) -> None:
        """
        Closes the cursor and the connection to the PostgreSQL database.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        logger.debug(
            f"Disconnecting from the database... ({self.CONFIG['database']})")
        if self.cursor and not self.cursor.closed:
            self.cursor.close()
            logger.debug("Closed the cursor for the database.")

        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.debug("Disconnected from the database.")

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def fetchone(self) -> Tuple:
        """
        Fetches the next row of a query result set.

        Returns:
            Tuple: A tuple representing the next row of a query result set.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        self._validate_connection()

        return self.cursor.fetchone()

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def fetchall(self) -> List[Tuple]:
        """
        Fetches all (remaining) rows of a query result set.

        Returns:
            List[Tuple]: A list of tuples representing the rows of a query result set.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        self._validate_connection()

        return self.cursor.fetchall()

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def commit(self):
        """
        Commits the current pending executions on the PostgreSQL database.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        self._validate_connection()

        self.connection.commit()
        logger.debug("Committed changes to the database.")

    def _validate_connection(self):
        """
        Checks if the connection and cursor are established.

        Raises:
            DatabaseNotInitialisedException: If the connection or cursor is not established.
        """
        if not self.cursor or not self.connection:
            logger.error(
                "Connection not initialised before attempting queries."
            )
            raise DatabaseNotInitialisedException()
