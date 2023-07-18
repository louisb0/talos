import base64
from typing import List, Dict

from talos.util.decorators import retry_exponential
from talos.config import Settings
from talos.exceptions.api import *
from talos.api import Requests

# probably refactor into an iterator at some point
# when i move to storing posts instead of api responses

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
        response = self.response_requester.get_response_with_posts(
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
        self.requests = Requests()

    @retry_exponential(minimum_wait_time=1, maximum_wait_time=30, exception_types=(APINonFatalException,))
    def get_response_with_posts(self, subreddit, after=None):
        """
        Get the raw API response containing new posts for the given subreddit.

        Args:
            subreddit (str): The subreddit from which to get new posts.
            after (str, optional): The id after which to get new posts.

        Returns:
            Dict: The json response from the post request.
        """
        request_body = self._format_body(subreddit, after)

        return self.requests.send(
            url="https://gql.reddit.com/",
            type=Requests.TYPE_POST,
            body=request_body,
            is_json=True,
            with_auth=True
        )

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
