
import base64
from typing import List, Dict, Tuple

from talos.config import Settings
from talos.api import Requests
from talos.exceptions.api import *
from talos.util.decorators import retry_exponential
from talos.logger import logger


class PostCollector:
    def __init__(self, subreddit: str, stopping_post_ids: List[Tuple[str]], requests_obj: Requests):
        self.subreddit: str = subreddit
        self.stopping_post_ids: List[Tuple[str]] = stopping_post_ids

        self.response_fetcher = PostResponseFetcher(requests_obj)
        self.unprocessed_posts: List = []

        self.after: str = None

    def get_unseen_posts(self) -> Dict:
        """
        Fetches all the newest posts up until self.stopping_post_id.
        """

        """
        Note: This function seems unituitive to me, this whole class is fiddly.
        It continually loops, making sure self.unprocessed_posts is filled with
        posts from the latest API response. Then, it pops the latest one to check
        if it's a duplicate. If it is, break, if not, add it to the list of unseen.
        """
        unseen_posts = []

        while True:
            if not self.unprocessed_posts:
                self._fetch_posts()

            # if still no posts after fetching, we reached the end
            if not self.unprocessed_posts:
                break

            next_post = self.unprocessed_posts.pop(0)
            if any(next_post["id"] in tup for tup in self.stopping_post_ids):
                break

            unseen_posts.append(next_post)

        return unseen_posts

    def _fetch_posts(self):
        """
        Fetches the new posts from the subreddit, using and updating self.after.
        Appends all user posts from the latest API response to self.unprocessed_posts.
        """
        response = self.response_fetcher.get_response_with_posts(
            subreddit=self.subreddit,
            after=self.after
        )

        for edge in response["data"]["subredditInfoByName"]["elements"]["edges"]:
            post = edge["node"]

            if post["__typename"] == "SubredditPost":
                self.unprocessed_posts.append(post)

        if self.unprocessed_posts:
            self.after = self.unprocessed_posts[-1]["id"]

        logger.info(
            f"Fetched {len(self.unprocessed_posts)} posts, new_after={self.after}."
        )


class PostResponseFetcher:
    def __init__(self, requests_obj: Requests):
        """
        Initializes the ResponseRequester object, generating headers for further requests.
        """
        self.requests = requests_obj

    @retry_exponential(minimum_wait_time=1, maximum_wait_time=30, exception_types=(APINonFatalException,))
    def get_response_with_posts(self, subreddit: str, after: str = None) -> Dict:
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

    def _format_body(self, subreddit: str, after: str = None) -> Dict:
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
