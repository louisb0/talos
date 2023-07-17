import unittest
from unittest.mock import patch, Mock
import logging

from talos.db import TransactionalDatabase
from talos.exceptions.db import DatabaseFatalException

class TestTransactionalDatabase(unittest.TestCase):
    """
    These tests made me realise the interface for this is bad and will be redesigned.
    Notes about this added to transactional_database.py

    Coverage:
        * begin_transaction() proxies a call to beginning psycopg2 transaction
        * rollback() proxies ^^
        * execute() proxies w/ params, does NOT commit
        * test context manager
            * if no exception occurs, using cm connects, begins transaction, commits, disconnects
            * if exception occurs, using cm connects, begins transaction, rollback, disconnects
    """
    def setUp(self):
        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

    @patch("talos.db.base_database.BaseDatabase._validate_connection")
    @patch("talos.db.transactional_database.TransactionalDatabase.rollback_transaction")
    @patch("talos.db.transactional_database.TransactionalDatabase.begin_transaction")
    @patch("talos.db.base_database.BaseDatabase.commit")
    @patch("talos.db.base_database.BaseDatabase.disconnect")
    @patch("talos.db.base_database.BaseDatabase.connect")
    def test_context_manager(self, mock_connect, mock_disconnect, mock_commit, mock_transaction, mock_rollback, mock_validate):
        with self.subTest(msg="intended_functionality"):
            with TransactionalDatabase() as tdb:
                pass

            mock_connect.assert_called_once()
            mock_transaction.assert_called_once()

            mock_disconnect.assert_called_once()
            mock_commit.assert_called_once()

        with self.subTest(msg="exception_raised"):
            with self.assertRaises(Exception):
                mock_commit_calls = mock_commit.call_count
                with TransactionalDatabase() as tdb:
                    raise Exception()
                
            # same as assert_not_called, workaround for one mock obj for both subtests
            self.assertEqual(mock_commit_calls, mock_commit.call_count)
            mock_rollback.assert_called_once()

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