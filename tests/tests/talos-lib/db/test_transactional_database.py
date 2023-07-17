import unittest
from unittest.mock import patch, Mock
import logging

from talos.db import TransactionalDatabase

class TestTransactionalDatabase(unittest.TestCase):
    """
    These tests made me realise the interface for this is bad and will be redesigned.
    Notes about this added to transactional_database.py

    Coverage:
        * begin_transaction() proxies a call to beginning psycopg2 transaction
        * rollback() proxies ^^
        * execute() proxies w/ params, does NOT commit
    """

    def setUp(self):
        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

    @patch("psycopg2.connect")
    def test_transaction(self, mock_connect):
        mock_connect.return_value.cursor.return_value = Mock()

        db = TransactionalDatabase()
        db.connect()
        db.begin_transaction()

        db.cursor.execute.assert_called_with("BEGIN")

    @patch("psycopg2.connect")
    def test_rollback(self, mock_connect):
        mock_connect.return_value.cursor.return_value = Mock()

        db = TransactionalDatabase()
        db.connect()
        db.begin_transaction()
        db.rollback_transaction()

        db.connection.rollback.assert_called_once()

    @patch("psycopg2.connect")
    def test_execute(self, mock_connect):
        mock_connect.return_value.cursor.return_value = Mock()

        db = TransactionalDatabase()
        db.connect()
        db.begin_transaction()
        db.execute("SELECT * FROM table WHERE x=%s", ("y",))

        db.cursor.execute.assert_called_with("SELECT * FROM table WHERE x=%s", ("y",))
        db.connection.commit.assert_not_called()