from typing import List, Tuple

from talos.exceptions.db import *
from talos.logger import logger

from .base_database import BaseDatabase


class TransactionalDatabase(BaseDatabase):
    """
    Used when a certain execute flow makes codependent queries to the
    database, e.g. foreign keys, allowing for implementation of a two phase
    commit.
    """

    def __enter__(self):
        """
        Connects to the database and begins a transaction when the object is used in a `with` statement.
        """
        self.connect()
        self.begin_transaction()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Performs a rollback or commit depending on success of `with` block,
        before finally disconnecting from the database.
        """
        if exc_type is not None:  # An exception occurred
            self.rollback_transaction()
        else:
            self.commit()

        self.disconnect()

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def execute(self, query: str, params: Tuple = None) -> None:
        """
        Executes a query on the PostgreSQL database.

        Args:
            query (str): The SQL query to execute.
            params (Tuple, optional): Parameters to bind to the query.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        self._validate_connection()

        self.cursor.execute(query, params)
        logger.debug(f"Executed query={query} with params={params}.")

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def begin_transaction(self):
        """
        Begins a transaction on the PostgreSQL database.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        self._validate_connection()

        self.cursor.execute("BEGIN")
        logger.debug(f"Began a new transaction.")

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def rollback_transaction(self):
        """
        Rolls back the current transaction on the PostgreSQL database.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        self._validate_connection()

        self.connection.rollback()
        logger.debug(f"Rolled back the transaction.")
