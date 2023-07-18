from typing import Tuple

from psycopg2.extensions import AsIs

from talos.db import ContextDatabase
from talos.config import Settings


def mark_rescan_queued(subreddit: str) -> None:
    """
    Updates the subscriptions table to indicate the rescan was queued.

    Args:
        subreddit (str): The subreddit which to mark queued.
    """
    with ContextDatabase() as db:
        db.execute(
            query="UPDATE %s SET is_currently_queued=true WHERE subreddit=%s",
            params=(AsIs(Settings.SUBSCRIPTIONS_TABLE), subreddit)
        )

def fetch_subscriptions() -> Tuple[str]:
    """
    Fetches all the current subscriptions stored, so that we can schedule
    their rescans.
    """
    with ContextDatabase() as db:
        db.execute(
            query="SELECT * FROM %s",
            params=(AsIs(Settings.SUBSCRIPTIONS_TABLE),),
            auto_commit=False
        )

        return db.fetchall()
