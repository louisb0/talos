import time
import sys

from talos.config import Settings
from talos.logger import logger
from talos.components import ProducerComponent
from talos.queuing import RabbitMQ
from talos.db import ContextDatabase

from lib.util import db_helpers, logic_helpers, queue_helpers


class RescanProducer(ProducerComponent):
    """
    This is one of three components in the data system.

    The purpose of the RescanProducer to produce messages to SUBREDDIT_RESCAN_QUEUE,
    and POST_RESCAN_QUEUE.

    We produce to the SUBREDDIT_RESCAN_QUEUE by reading from the SUBSCRIPTIONS_TABLE, 
    finding subreddits which require a 'rescan', i.e. reiteration over new posts.

    We produce to the POST_RESCAN_QUEUE by reading from the POST_RESCANS_TABLE,
    finding individual posts which have a scheduled 'rescan' time. This rescan
    fetches the post meta data again, as well as comments, once the post is 7
    days old, or older. This allows engagement to develop.
    """

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

    def produce_subreddit_rescans(self):
        """
        Reads from the SUBSCRIPTIONS_TABLE and queues a 'rescan' of a subreddit.
        This subreddit rescan is consumed by the subreddit-rescanner.
        """
        logger.info("Checking for due subreddit rescans...")

        subscriptions = db_helpers.fetch_subscriptions()
        for subscription in subscriptions:
            subreddit = subscription[0]

            if logic_helpers.is_rescan_required(subscription):
                queue_helpers.queue_subreddit_rescan(subreddit)
                db_helpers.mark_subscription_queued(subreddit)

    def produce_post_rescans(self) -> None:
        """
        Reads from the POST_RESCANS_TABLE, where the scheduled time has passed.
        For each of these rescans, we queue the message with an API request which
        contains updated post meta data, as well as comments.
        """
        logger.info("Checking for due post rescans...")
        due_post_rescans = db_helpers.fetch_due_post_rescans()

        with RabbitMQ(queues=(Settings.POST_RESCAN_QUEUE,)) as rabbitmq:
            with ContextDatabase() as cdb:
                for due_post_rescan in due_post_rescans:
                    post_rescan_id, _, _, _, _, post_id = due_post_rescan
                    
                    queue_helpers.queue_post_rescan(
                        rabbitmq=rabbitmq,
                        api_request={
                            "url": f"https://gateway.reddit.com/desktopapi/v1/postcomments/{post_id}",
                            "method": 0  # Requests.TYPE_GET
                        },
                        post_id=post_id,
                        post_rescan_id=post_rescan_id
                    )

                    db_helpers.mark_post_rescan_queued(
                        cdb=cdb,
                        post_rescan_id=post_rescan_id
                    )
                
                cdb.commit()

    def _handle_one_pass(self):
        """
        Handles one pass of the run loop, reading from subscriptions and scheduling rescans.
        """
        self.produce_subreddit_rescans()
        self.produce_post_rescans()

        logger.info(
            f"Pass complete. Sleeping for {Settings.RESCAN_PRODUCER_SLEEP_TIME_SECS} seconds...\n"
        )
        time.sleep(Settings.RESCAN_PRODUCER_SLEEP_TIME_SECS)

    def run(self):
        super().run()
