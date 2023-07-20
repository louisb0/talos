from typing import Tuple

from psycopg2.extensions import AsIs

from talos.db import ContextDatabase
from talos.config import Settings
from talos.logger import logger


def fetch_subscriptions() -> Tuple[str]:
    """
    Fetches from SUBSCRIPTIONS_TABLE all due subreddit rescans, so that they
    can be queued and consumed by subreddit-rescanner.
    """
    with ContextDatabase() as db:
        db.execute(
            query="SELECT * FROM %s",
            params=(AsIs(Settings.SUBSCRIPTIONS_TABLE),),
            auto_commit=False
        )

        subscriptions = db.fetchall()
        logger.debug(f"Found subscriptions={subscriptions}.")

        return subscriptions


def mark_subscription_queued(subreddit: str) -> None:
    """
    Updates the SUBSCRIPTIONS_TABLE to indicate the subreddit rescan was queued.

    Args:
        subreddit (str): The subreddit which to mark queued.
    """
    with ContextDatabase() as db:
        db.execute(
            query="UPDATE %s SET is_currently_queued=true WHERE subreddit=%s",
            params=(AsIs(Settings.SUBSCRIPTIONS_TABLE), subreddit),
            auto_commit=True
        )

        logger.debug(
            f"Marked the rescan for {subreddit} subscription as queued."
        )


def fetch_due_post_rescans() -> Tuple[str]:
    """
    Fetches from POST_RESCAN_TABLE all due post rescans, so that they can
    be queued and consumed by post-rescanner.
    """
    with ContextDatabase() as db:
        db.execute(
            query="SELECT * FROM %s WHERE began_processing=FALSE AND scheduled_start_at <= NOW();",
            params=(AsIs(Settings.POST_RESCAN_TABLE),),
            auto_commit=False
        )

        due_post_rescans = db.fetchall()
        logger.debug(f"Found due post rescans={due_post_rescans}.")

        return due_post_rescans


def mark_post_rescan_queued(cdb: ContextDatabase, post_rescan_id: int) -> None:
    """
    Updates the POST_RESCAN_TABLE table to update the last time this post
    rescan was seen by the system.

    Args:
        post_rescan_id (int): The ID of the post rescan to mark as touched.
    """
    cdb.execute(
        query="UPDATE %s SET began_processing=TRUE, last_seen=NOW() WHERE id=%s",
        params=(AsIs(Settings.POST_RESCAN_TABLE), post_rescan_id),
        auto_commit=False
    )

    logger.debug(f"Updated last_seen for post_rescan_id={post_rescan_id}.")
