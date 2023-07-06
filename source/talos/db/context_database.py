from typing import List, Tuple

from talos.exceptions.db import *
from talos.logger import logger

from .base_database import BaseDatabase


class ContextDatabase(BaseDatabase):
    def __enter__(self):
        """
        Connects to the database when the Database object is used in a `with` statement.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Disconnects from the database when exiting the `with` statement block.
        """
        self.disconnect()

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def execute(self, query: str, params: Tuple = None, auto_commit: bool = True) -> None:
        """
        Executes a query on the PostgreSQL database.

        Args:
            query (str): The SQL query to execute.
            params (Tuple, optional): Parameters to bind to the query.
            auto_commit (bool, optional): Whether to commit the transaction automatically. Default is True.

        Raises:
            DatabaseNonFatalException: For non-fatal internal psycopg2 exceptions.
            DatabaseFatalException: For fatal internal psycopg2 exceptions.
        """
        self._validate_connection()

        self.cursor.execute(query, params)

        if auto_commit:
            self.connection.commit()
