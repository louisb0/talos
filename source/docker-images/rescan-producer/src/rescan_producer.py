import time
import sys

from talos.config import Settings
from talos.logger import logger
from talos.components import ProducerComponent

from rescan_producer.util import db_helpers, logic_helpers, queue_helpers

class RescanProducer(ProducerComponent):
    def __init__(self, retry_attempts, time_between_attempts):
        """
        Initializes the RescanProducer object.

        Args:
            retry_attempts (int): The number of retry attempts for handling errors.
            time_between_attempts (int): The time to wait between attempts in seconds.
        """
        super().__init__(retry_attempts, time_between_attempts)
        Settings.validate()

    def handle_critical_error(self):
        """
        Handles critical errors which could not be retried.
        """
        logger.critical("handle_critical_error() hit. Exiting...")
        sys.exit(1)

    def _handle_one_pass(self):
        """
        Handles one pass of the run loop, reading from subscriptions and scheduling rescans.
        """
        logger.info("Checking for required rescans...")

        subscriptions = db_helpers.fetch_subscriptions()

        for subscription in subscriptions:
            subreddit = subscription[0]

            if logic_helpers.is_rescan_required(subscription):
                queue_helpers.queue_rescan(subreddit)
                db_helpers.mark_rescan_queued(subreddit)
                logger.info(f"A rescan is required for {subreddit}. Queued.")
            else:
                logger.info(f"No rescan is required for {subreddit}.")

        logger.info(
            f"Pass complete. Sleeping for {Settings.SECONDS_BETWEEN_RESCANS} seconds..."
        )
        time.sleep(Settings.SECONDS_BETWEEN_RESCANS)

    def run(self):
        super().run()
