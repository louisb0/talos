import unittest
from unittest.mock import patch, Mock

import logging

from talos.db import ContextDatabase

class TestContextDatabase(unittest.TestCase):
    """
    Testing db.connection/commit directly here seems intuitivitely wrong.
    Could be better to move the execute function up to BaseDatabase, test that there,
    then confirm that BaseDatabase.execute() is called with relayed params. Will
    come back to get me when switch to DynamoDB or similar. TODO

    Coverage:
        * __enter__ (i.e. `with ContextDB()`) creates a connection
        * __exit__ closes the connection
        * execute() 'works';
            * called with params
            * called without params
            * called with/without autocommit
    
    """
    def setUp(self):
        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

    @patch("talos.db.base_database.BaseDatabase.connect")
    def test_enter(self, mock_connect):
        db = ContextDatabase()

        with db:
            mock_connect.assert_called_once()
    
    @patch("talos.db.base_database.BaseDatabase.disconnect")
    @patch("talos.db.base_database.BaseDatabase.connect")
    def test_exit(self, mock__proxy_connect, mock_proxy_disconnect):
        db = ContextDatabase()
        with db:
            pass

        mock_proxy_disconnect.assert_called_once()

    @patch("psycopg2.connect")
    def test_execute(self, mock_connect):
        mock_connect.return_value.cursor.return_value = Mock()

        # order here matters; commit not called assertion fails if its not last
        # since we use one mock object for all subtests

        with self.subTest(msg="execute_no_param"):
            db = ContextDatabase()
            with db:
                db.execute("SELECT * FROM table", auto_commit=False)

            db.cursor.execute.assert_called_with("SELECT * FROM table", None)
            db.connection.commit.assert_not_called()

        with self.subTest(msg="execute_with_param"):
            db = ContextDatabase()
            with db:
                db.execute("SELECT * FROM table WHERE x=%s", ("y",), auto_commit=False)

            db.cursor.execute.assert_called_with("SELECT * FROM table WHERE x=%s", ("y",))
            db.connection.commit.assert_not_called()
    
        with self.subTest(msg="execute_with_auto_commit"):
            db = ContextDatabase()
            with db:
                db.execute("SELECT * FROM table", auto_commit=True)

            db.cursor.execute.assert_called_with("SELECT * FROM table", None)
            db.connection.commit.assert_called_once()

       

        