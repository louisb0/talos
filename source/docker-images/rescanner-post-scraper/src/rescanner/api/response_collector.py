import requests
import regex
import base64
import time
from typing import List, Dict

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from talos.config import Settings
from talos.logger import logger


class ResponseCollector:
    def __init__(self, subreddit, stopping_post_id):
        """
        Initializes the ResponseCollector object.

        Args:
            subreddit (str): The subreddit from which to collect responses.
            stopping_post_id (str): The post id at which to stop collecting responses.
        """
        self.subreddit: str = subreddit
        self.stopping_post_id: str = stopping_post_id

        self.response_requester = ResponseRequester()
        self.responses: List = []
        self.after: str = None

    def collect_responses(self) -> Dict:
        """
        Collects responses from the specified subreddit until it reaches the stopping post id or max pagination.

        Returns:
            Dict: A list of responses collected, stored in self.responses.
        """
        while True:
            response = self._get_next_response()

            if self._is_response_empty(response):
                break

            if self._reached_stopping_id(response):
                break

            self.responses.append(response)
            self._update_after()

        return self.responses

    def _get_next_response(self) -> Dict:
        """
        Gets the next response from the subreddit.

        Returns:
            Dict: The raw response and the posts contained within it.
        """
        response = self.response_requester.get_new_posts(
            subreddit=self.subreddit,
            after=self.after
        )

        all_posts = [
            edge["node"] for edge in response["data"]["subredditInfoByName"]["elements"]["edges"]
        ]

        user_posts = [
            post for post in all_posts if post["__typename"] == "SubredditPost"
        ]

        return {
            "raw_response": response,
            "contained_posts": user_posts
        }

    def _update_after(self) -> None:
        """
        Updates the 'after' API parameter to the last post contained in the response.
        """
        if not self.responses:
            return

        recent_posts = self.responses[-1]["contained_posts"]
        self.after = recent_posts[-1]["id"]

    def _reached_stopping_id(self, response) -> bool:
        """
        Checks if the stopping post id has been reached.

        Args:
            response (Dict): The response to check.

        Returns:
            bool: True if the stopping post id is in the response, False otherwise.
        """
        if not response:
            return False

        return self.stopping_post_id in [post["id"] for post in response["contained_posts"]]

    def _is_response_empty(self, response) -> bool:
        """
        Checks if a response is empty, i.e. contains no posts.

        Args:
            response (Dict): The response to check.

        Returns:
            bool: True if the response is empty, False otherwise.
        """
        return len(response["contained_posts"]) == 0


class ResponseRequester:
    def __init__(self):
        """
        Initializes the ResponseRequester object, generating headers for further requests.
        """
        self.headers = self._generate_headers()

    # TODO: create custom decorator and error wrappers in /source/talos
    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(min=2, max=60),
           retry=retry_if_exception_type(Exception),
           before_sleep=lambda retry_state: logger.info(
               f"Retrying... {retry_state}"),
           reraise=True)
    def get_new_posts(self, subreddit, after=None):
        """
        Gets new posts from the specified subreddit.

        Args:
            subreddit (str): The subreddit from which to get new posts.
            after (str, optional): The id after which to get new posts.

        Returns:
            Dict: The json response from the post request.
        """
        request_body = self._format_body(subreddit, after)

        return requests.post(
            url="https://gql.reddit.com/",
            json=request_body,
            headers=self.headers
        ).json()

    def _format_body(self, subreddit, after=None):
        """
        Formats the body for a request.

        Args:
            subreddit (str): The subreddit from which to get new posts.
            after (str, optional): The id after which to get new posts.

        Returns:
            Dict: The formatted body.
        """
        body = {
            "id": "e111e3a11997",
            "variables": {
                "name": subreddit,
                "includeIdentity": False,
                "isFake": False,
                "includeDevPlatformMetadata": True,
                "includeRecents": False,
                "includeTrending": False,
                "includeSubredditRankings": True,
                "includeSubredditChannels": True,
                "isAdHocMulti": False,
                "isAll": False,
                "isLoggedOutGatedOptedin": False,
                "isLoggedOutQuarantineOptedin": False,
                "isPopular": False,
                "recentPostIds": [],
                "subredditNames": [],
                "sort": "NEW",
                "pageSize": Settings.MAX_POSTS_PER_REQUEST
            }
        }

        if after is not None:
            body["variables"]["after"] = base64.b64encode(
                after.encode("utf-8")
            ).decode("utf-8")

        return body

    # TODO: create custom decorator and error wrappers in /source/talos
    @retry(stop=stop_after_attempt(5),
           wait=wait_exponential(min=2, max=60),
           retry=retry_if_exception_type(Exception),
           before_sleep=lambda retry_state: logger.info(
               f"Retrying... {retry_state}"),
           reraise=True)
    def _generate_headers(self):
        """
        Generates headers for a request.

        Returns:
            Dict: The headers, including a user agent and bearer authorization token.
        """
        token = self._generate_token()
        if token is None:
            raise ValueError()

        return {
            "Authorization": "Bearer " + token,
            "User-Agent": Settings.USER_AGENT
        }

    def _generate_token(self):
        """
        Generates a token for authorization.

        Returns:
            str: The bearer authorization token.
        """
        response = requests.get("https://reddit.com")
        match = regex.search(r'"accessToken":"(.*?)"', response.text)

        return match.group(1) if match else None
