import json
import time
import sys
from datetime import datetime, timedelta, timezone

from psycopg2.extensions import AsIs

from talos.config import Settings
from talos.db import ContextDatabase
from talos.queuing import RabbitMQ
from talos.logger import logger
from talos.components import ProducerComponent


class RescanUtility():
    @staticmethod
    def fetch_subscriptions():
        with ContextDatabase() as db:
            db.execute(
                query="SELECT * FROM %s",
                params=(AsIs(Settings.SUBSCRIPTIONS_TABLE),),
                auto_commit=False
            )

            return db.fetchall()

    @staticmethod
    def is_rescan_required(subscription):
        is_subscribed, time_between_scans, last_scanned_at, is_currently_queued = \
            subscription[1:5]

        if not is_subscribed or is_currently_queued:
            return False

        if last_scanned_at is None:
            return True

        next_scan_time = last_scanned_at.replace(tzinfo=timezone.utc) + \
            timedelta(seconds=time_between_scans)

        return datetime.now(timezone.utc) >= next_scan_time

    @staticmethod
    def mark_rescan_queued(subreddit):
        with ContextDatabase() as db:
            db.execute(
                query="UPDATE %s SET is_currently_queued=true WHERE subreddit=%s",
                params=(AsIs(Settings.SUBSCRIPTIONS_TABLE), subreddit)
            )

    @staticmethod
    def queue_rescan(subreddit):
        with RabbitMQ(queues=(Settings.RESCAN_QUEUE,)) as queue:
            queue.publish_message(
                queue_name=Settings.RESCAN_QUEUE,
                message=json.dumps({
                    "subreddit": subreddit
                })
            )


class RescanProducer(ProducerComponent):
    def __init__(self, retry_attempts, time_between_attempts):
        super().__init__(retry_attempts, time_between_attempts)
        Settings.validate()

    def handle_critical_error(self):
        logger.error("A fatal error occured.")

    def _handle_one_pass(self):
        logger.info("Beginning a rescan.")

        subscriptions = RescanUtility.fetch_subscriptions()

        for subscription in subscriptions:
            subreddit = subscription[0]

            if RescanUtility.is_rescan_required(subscription):
                RescanUtility.queue_rescan(subreddit)
                RescanUtility.mark_rescan_queued(subreddit)
                logger.info(f"Queued rescan for {subreddit}")
            else:
                logger.info(f"No rescan required for {subreddit}")

        logger.info(
            f"Rescan complete. Sleeping for {Settings.SECONDS_BETWEEN_RESCANS} seconds..."
        )

        time.sleep(Settings.SECONDS_BETWEEN_RESCANS)

    def run(self):
        super().run()