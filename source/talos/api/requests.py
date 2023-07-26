import requests
import regex
from typing import Dict, Union

from talos.logger import logger
from talos.config import Settings
from talos.exceptions.api import *
from talos.util.decorators import retry_exponential


class Requests:
    """
    A class which proxies requests to the 'requests' library with error handling
    and token generation.
    """
    TYPE_GET = 0
    TYPE_POST = 1

    def __init__(self):
        self.requests_on_token = 0
        self._current_token = None

    def send(self, url: str, type: int, body: dict = None, is_json: bool = False, with_auth: bool = False):
        """
        Proxies requests to internal methods based on the `type` parameter.

        Args:
            url (str): The URL to send a GET request.
            type (int): The type of request, per constants in this class (TYPE_GET, TYPE_POST, ...)
            body (dict = None): The body of the POST request.
            is_json (bool = False): True if the response should be parsed from JSON to dict.
            with_auth (bool = False): True if a token and user agent should be used.

        Returns:
            requests.Response: The response, if is_json is false.
            Dict: The parsed response, if is_json is true.

        Raises:
            InvalidRequestType (APIFatalException): Raised if the `type` parameter is unknown.
            APINonFatalException: For transient errors which may be resolved by retry.
            APIFatalException: For errors which cannot be resolved by retry.
        """
        headers = self._generate_headers() if with_auth else None
        response = None

        logger.debug(
            f"Preparing request with url={url} type={type} is_json={is_json} with_auth={with_auth} body={body} headers={headers}."
        )

        if type == self.TYPE_GET:
            response: requests.Response = self._get(
                url=url,
                headers=headers
            )
        elif type == self.TYPE_POST:
            response: requests.Response = self._post(
                url=url,
                body=body,
                headers=headers
            )
        else:
            raise InvalidRequestType()

        if with_auth:
            self.requests_on_token += 1

        logger.debug(
            f"Request sent. requests_on_token={self.requests_on_token}. Returning..."
        )

        return response if not is_json else response.json()

    def send_from_message(self, message: dict) -> dict:
        """
        The interface for the post-scraper component, used to execute the queued
        API requests. 

        Args:
            message (dict): The API request object as part of the RabbitMQ message.
        """
        url = message["url"]
        method = message["method"]
        body = message.get("body")

        return self.send(
            url=url,
            type=method,
            body=body,
            is_json=True,
            with_auth=True
        )

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def _get(self, url: str, headers: dict = None) -> requests.Response:
        """
        Sends a GET request using the requests library. Has it's own function for
        the purpose of reraising custom exceptions.

        Args:
            url (str): The URL to send a GET request.
            headers (dict): The headers to sent with the request.

        Returns:
            requests.Response
        """
        logger.debug(f"Sending GET with url={url}...")

        return requests.get(
            url=url,
            headers=headers
        )

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def _post(self, url: str, body: dict, headers: dict = None) -> requests.Response:
        """
        Sends a POST request using the requests library. Has it's own function for
        the purpose of reraising custom exceptions.

        Args:
            url (str): The URL to send a POST request.
            body (dict): The body of the POST request.
            headers (dict): The headers to sent with the request.

        Returns:
            requests.Response: 
        """
        logger.debug(f"Sending POST with url={url}...")

        return requests.post(
            url=url,
            json=body,
            headers=headers
        )

    def _get_token(self) -> str:
        """
        Compares the requests made with the requests per token setting to either
        generate a new token, or return the current.

        Returns:
            str: The Bearer authorization token found.
        """
        if self.requests_on_token % Settings.REQUESTS_PER_TOKEN == 0:
            self._current_token = self._generate_token()

        return self._current_token

    @retry_exponential(minimum_wait_time=1, maximum_wait_time=30, exception_types=(APINonFatalException,))
    def _generate_token(self) -> str:
        """
        Fetches a token from Reddit by parsing the homepage HTML. Sets the token
        in self._current_token.

        Raises:
            TokenNotFound (APINonFatalException): If no token is found, a non-fatal
            exception is raised, signalling a retry of the generation.
        """
        logger.info("Attempting to fetch a new token...")

        response = self.send(
            url="https://reddit.com",
            type=Requests.TYPE_GET
        )

        match = regex.search(r'"accessToken":"(.*?)"', response.text)
        if match:
            logger.info(f"Found token {match.group(1)}.")
            return match.group(1)
        else:
            logger.info("Failed to find token.")
            raise TokenNotFound()

    def _generate_headers(self) -> Dict:
        """
        Generates a token and creates headers with the authorization and user agent.

        Returns:
            Dict: The header object with a Bearer token and user agent. 
        """
        token = self._get_token()

        return {
            "Authorization": "Bearer " + token,
            "User-Agent": Settings.USER_AGENT
        }
