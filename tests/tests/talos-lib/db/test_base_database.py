import unittest
from unittest.mock import patch, MagicMock, Mock
import logging

from talos.db.base_database import BaseDatabase
from talos.exceptions.db import DatabaseNotInitialisedException
from talos.logger import logger


class TestBaseDatabase(unittest.TestCase):
    """
    We don't test error handling; correctness ensured by testing
    the decorators.

    Coverage:
        * __init__ fields are set
        * connect() sets connection and cursor
        * connect() is idemptotent, calling it twice no error
        * disconnect() closes connection and cursor <- poorly implemented
        * disconnect() is idempotent <- poorly implemented
        * fetchone() relays to psycopg2 fetchone
        * fetchall() relays to psycopg2 fetchall
        * commit() relays to psycopg2 commit
        * _validate_connection raises exception when no connection
        * commit, fetchone, fetchall, call _validate_connection
    """
    def setUp(self):
        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

    def test_init_sets_fields(self):
        db = BaseDatabase()

        self.assertIsNone(db.connection)
        self.assertIsNone(db.cursor)

    @patch("psycopg2.connect")
    def test_connect_creates_objects(self, mock_connect):
        db = BaseDatabase()
        db.connect()

        mock_connect.assert_called_once_with(**db.CONFIG)
        self.assertIsNotNone(db.connection)
        self.assertIsNotNone(db.cursor)

    @patch("psycopg2.connect")
    def test_connect_idempotency(self, mock_connect):
        mock_connect.return_value.cursor.return_value = Mock()

        db = BaseDatabase()
        db.connect()
        db.cursor.closed = False
        db.connection.closed = False
        db.connect()

        mock_connect.assert_called_once_with(**db.CONFIG)

    @patch("psycopg2.connect")
    def test_disconnect_closes_objects(self, mock_connect):
        db = BaseDatabase()
        db.connect()
        # this pretty stupid basically guarantees the assert
        db.cursor.closed = False
        db.connection.closed = False
        db.disconnect()

        db.connection.close.assert_called_once()
        db.cursor.close.assert_called_once()

    @patch('psycopg2.connect')
    def test_disconnect_idempotency(self, mock_connect):
        db = BaseDatabase()
        db.connect()
        # this also pretty stupid basically guarantees the assert
        db.cursor.closed = False
        db.connection.closed = False
        db.disconnect()
        db.cursor.closed = True
        db.connection.closed = True
        db.disconnect()

        db.connection.close.assert_called_once()
        db.cursor.close.assert_called_once()

    @patch('psycopg2.connect')
    def test_fetchone(self, mock_connect):
        db = BaseDatabase()
        db.connect()
        db.fetchone()

        db.cursor.fetchone.assert_called_once()

    @patch('psycopg2.connect')
    def test_fetchall(self, mock_connect):
        db = BaseDatabase()
        db.connect()
        db.fetchall()

        db.cursor.fetchall.assert_called_once()
    
    @patch('psycopg2.connect')
    def test_commit(self, mock_connect):
        db = BaseDatabase()
        db.connect()
        db.commit()

        db.connection.commit.assert_called_once()

    def test_validate_connection(self):
        db = BaseDatabase()

        with self.assertRaises(DatabaseNotInitialisedException):
            db._validate_connection()

    @patch('psycopg2.connect')
    def test_connection_validation(self, mock_connect):
        db = BaseDatabase()
        db.connect()

        with self.subTest(method='fetchone'):
            with patch.object(db, '_validate_connection') as mock_validate:
                db.fetchone()
                mock_validate.assert_called_once()

        with self.subTest(method='fetchall'):
            with patch.object(db, '_validate_connection') as mock_validate:
                db.fetchall()
                mock_validate.assert_called_once()

        with self.subTest(method='commit'):
            with patch.object(db, '_validate_connection') as mock_validate:
                db.commit()
                mock_validate.assert_called_once()