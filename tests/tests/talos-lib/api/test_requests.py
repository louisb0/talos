import unittest
from unittest.mock import patch, Mock, ANY
import logging

from talos.config import Settings
from talos.api.requests import Requests
from talos.exceptions.api import *

"""
token generation mechanism

send() raises invalid if incorrect type
send() returns dict if is_json
send() returns response if not is_json

with_auth causes _get_token() to be called
_get_token() only calls generate_token() every REQUESTS_PER_TOKEN requests

_generate_token() raises NonFatalExcpe

"""


class TestRequests(unittest.TestCase):
    """
    Coverage:
        * test that send() with get/post proxy to requests.get
          or requests.post() with correct params
        * test send() with invalid request type raises APIFatalException
        * test is_json param on send() proxies to requests.Response.json()
            ^ likely too closely coupled to implementation, changing
              down the line gonna be an issue
        * with_auth makes a call to _get_token()
        * if MAX_REQUESTS_PER_TOKEN is met, a new token is generated
        * if no token is found, TokenNotFound() (fatal error) is raised
    
    """

    FAKE_URL = "https://google.com"

    def setUp(self):
        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

        self.test_requests = Requests()

    def tearDown(self):
        self.test_requests = None

    @patch.object(Requests, "_post")
    @patch.object(Requests, "_get")
    def test_proxy_calls(self, mock_get, mock_post):
        self.test_requests.send(
            url=self.FAKE_URL,
            type=Requests.TYPE_GET
        )
        self.test_requests.send(
            url=self.FAKE_URL,
            type=Requests.TYPE_POST,
            body={"key": "value"}
        )

        mock_get.assert_called_once_with(
            url=self.FAKE_URL,
            headers=ANY
        )
        mock_post.assert_called_once_with(
            url=self.FAKE_URL,
            body={"key": "value"},
            headers=ANY
        )

    def test_invalid_type(self):
        with self.assertRaises(APIFatalException):
            self.test_requests.send(
                url=self.FAKE_URL,
                type=3
            )

    @patch("requests.post")
    @patch("requests.get")
    def test_is_json(self, mock_get, mock_post):
        with self.subTest("is_json_true"):
            options = {
                "url": self.FAKE_URL,
                "is_json": True
            }

            self.test_requests.send(
                **options,
                type=Requests.TYPE_GET
            )
            self.test_requests.send(
                **options,
                type=Requests.TYPE_POST
            )

            mock_get.return_value.json.assert_called_once()
            mock_post.return_value.json.assert_called_once()

        with self.subTest("is_json_false"):
            mock_get.return_value.json.reset_mock()
            mock_post.return_value.json.reset_mock()

            options = {
                "url": self.FAKE_URL,
                "is_json": False
            }

            self.test_requests.send(
                **options,
                type=Requests.TYPE_GET
            )
            self.test_requests.send(
                **options,
                type=Requests.TYPE_POST
            )

            mock_get.return_value.json.assert_not_called()
            mock_post.return_value.json.assert_not_called()

    @patch("requests.post")
    @patch("requests.get")
    def test_with_auth(self, mock_get, mock_post):
        with patch.object(Requests, "_get_token") as mock_get_token:
            self.test_requests.send(
                url=self.FAKE_URL,
                type=Requests.TYPE_GET,
                with_auth=True
            )

            mock_get_token.assert_called_once()

    @patch("requests.get")
    @patch.object(Requests, "_generate_token")
    def test_token_rotation(self, mock_generate, mock_get):
        mock_generate.return_value = "token"

        # send max requests on that token, 1 gen on first
        for _ in range(Settings.REQUESTS_PER_TOKEN):
            self.test_requests.send(
                url=self.FAKE_URL,
                type=Requests.TYPE_GET,
                with_auth=True
            )

        # send max + 1, causing second gen
        self.test_requests.send(
            url=self.FAKE_URL,
            type=Requests.TYPE_GET,
            with_auth=True
        )

        # assert we rotated
        self.assertEqual(mock_generate.call_count, 2)

    @patch.object(Requests, "_get")
    def test_no_token(self, mock_get):
        mock_get.return_value.text = "no token here"

        with patch("tenacity.retry_if_exception_type.__call__", return_value=False) as _:
            with self.assertRaises(TokenNotFound):
                self.test_requests._generate_token()