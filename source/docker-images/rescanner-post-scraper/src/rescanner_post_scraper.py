import json
import sys

from talos.config import Settings
from talos.logger import logger
from talos.components import ConsumerComponent
from talos.db import TransactionalDatabase

from rescanner.api import PostCollector
from rescanner.util import db_helpers


class RescannerPostScraper(ConsumerComponent):
    def __init__(self, retry_attempts: int, time_between_attempts: int, producing_queue: str):
        """
        Initializes the RescannerPostScraper object.

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
        Handles one pass of the run loop, collecting the newest posts and storing them
        on disk and their references in the database.

        Args:
            message (str): The message containing the subreddit from which to scrape data.
        """
        subreddit = json.loads(message)["subreddit"]
        logger.info(
            f"Received rescan request for {subreddit}. Running rescan...")

        posts = PostCollector(
            subreddit,
            stopping_post_id=db_helpers.get_last_seen_post_id(subreddit)
        ).get_unseen_posts()
        logger.info(
            f"{len(posts)} unseen posts found. Preparing to write to DB...")

        with TransactionalDatabase() as tdb:
            rescan_id = db_helpers.create_rescan_entry(tdb, subreddit)

            for post in posts:
                db_helpers.create_scraped_post_entry(
                    tdb=tdb,
                    post_data=post,
                    rescan_id=rescan_id,
                )

            db_helpers.mark_rescan_processed(tdb, subreddit)
            logger.info(
                f"Completed rescan (id: {rescan_id}). {len(posts)} posts added to the database.\n")

    def run(self):
        super().run()
