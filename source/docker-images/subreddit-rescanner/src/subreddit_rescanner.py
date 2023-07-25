import json
import sys

from talos.config import Settings
from talos.logger import logger
from talos.components import ConsumerComponent
from talos.db import TransactionalDatabase
from talos.api import Requests

from lib.api import PostCollector
from lib.util import db_helpers, time_helpers


class SubredditRescanner(ConsumerComponent):
    """
    The purpose of the SubredditRescanner is to consume from the subreddit-rescan
    messages produced by rescan-producer, containing the subreddit to rescan.

    Then, it gets the newest, unseen, posts from this subreddit. It writes these
    to the INITIAL_POSTS_TABLE, schedules a post rescan in POST_RESCANS_TABLE.
    """

    def __init__(self, retry_attempts: int, time_between_attempts: int, producing_queue: str):
        """
        Initializes the SubredditRescanner object.

        Args:
            retry_attempts (int): The number of retry attempts for handling errors.
            time_between_attempts (int): The time to wait between attempts in seconds.
            producing_queue (str): The name of the queue from which to receive data.
        """
        super().__init__(retry_attempts, time_between_attempts, producing_queue)
        Settings.validate()

    def handle_critical_error(self):
        """
        Handles critical errors which could not be retried.
        """
        logger.critical("handle_critical_error() hit. Exiting...")
        sys.exit(1)

    def _handle_one_pass(self, message: str) -> None:
        """
        Receives a rescan message from SUBREDDIT_RESCAN_QUEUE, containing the subreddit.
        All of the newest posts are fetched and stored, as well as their associated post
        specific rescans (fetching comments and updated meta data after engagement develops).

        Args:
            message (str): The message containing the subreddit from which to fetch posts.
        """
        subreddit = json.loads(message)["subreddit"]
        logger.info(
            f"Received rescan request for {subreddit}. Running rescan..."
        )

        posts = PostCollector(
            subreddit,
            stopping_post_ids=db_helpers.get_last_seen_post_ids(subreddit),
            requests_obj=self.requests_obj
        ).get_unseen_posts()

        with TransactionalDatabase() as tdb:
            rescan_id = db_helpers.create_subreddit_rescan_entry(
                tdb, subreddit)

            for post in posts:
                db_helpers.create_initial_post_entry(
                    tdb=tdb,
                    post_data=post,
                    rescan_id=rescan_id,
                )
                db_helpers.create_post_rescan_entry(
                    tdb=tdb,
                    scheduled_start_at=time_helpers.get_scheduled_scrape_time(
                        post),
                    post_id=post["id"]
                )
            db_helpers.mark_subreddit_rescan_processed(tdb, subreddit)

        logger.info(
            f"Completed rescan (id: {rescan_id}). {len(posts)} posts added to the database.\n"
        )

    def run(self):
        # Persistent object for token rotation.
        self.requests_obj = Requests()

        super().run()
