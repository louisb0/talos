from typing import List, Tuple

from talos.exceptions.db import *

from .base_database import BaseDatabase


class TransactionalDatabase(BaseDatabase):
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