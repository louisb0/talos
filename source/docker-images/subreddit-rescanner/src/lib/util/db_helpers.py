from typing import List, Tuple, Union, Dict
import json
from datetime import datetime

from psycopg2.extensions import AsIs

from talos.db import ContextDatabase, TransactionalDatabase
from talos.config import Settings
from talos.logger import logger


def get_last_seen_post_ids(subreddit: str) -> Union[List[Tuple[str]], None]:
    """
    Retrieves the IDs of the last seen post in the given subreddit.

    Args:
        subreddit (str): The subreddit to query.

    Returns:
        Union[Tuple, None]: A tuple of IDs of the last seen posts, or None if no posts were found.
    """

    # use context db so we don't have to hold a transaction open
    # for the entire scrape period, locking the tables
    with ContextDatabase() as cdb:
        cdb.execute(
            """
            WITH latest_rescan_with_posts AS (
                SELECT subreddit_rescans.id AS rescan_id
                FROM %s
                JOIN %s ON subreddit_rescans.id = initial_posts.rescan_id
                WHERE subreddit_rescans.subreddit = %s
                GROUP BY subreddit_rescans.id
                ORDER BY subreddit_rescans.ran_at DESC
                LIMIT 1
            )
            SELECT initial_posts.id AS post_id
            FROM latest_rescan_with_posts
            JOIN initial_posts ON latest_rescan_with_posts.rescan_id = initial_posts.rescan_id;
            """,
            (AsIs(Settings.SUBREDDIT_RESCAN_TABLE), AsIs(
                Settings.INITIAL_POSTS_TABLE), subreddit)
        )

        return cdb.fetchall()


def create_subreddit_rescan_entry(tdb: TransactionalDatabase, subreddit: str) -> int:
    """
    Creates a rescan entry in the 'rescans' table for the given subreddit.

    Args:
        tdb (TransactionalDatabase): The database instance with an active transaction to write data.
        subreddit (str): The subreddit for which to create a rescan entry.

    Returns:
        int: The ID of the created rescan entry.
    """
    tdb.execute(
        query="INSERT INTO %s (subreddit) VALUES (%s) RETURNING id",
        params=(AsIs(Settings.SUBREDDIT_RESCAN_TABLE), subreddit)
    )

    return tdb.fetchone()[0]


def create_initial_post_entry(tdb: TransactionalDatabase, post_data: Dict, rescan_id: int):
    """
    Creates a scraped post entry in the 'scraped_posts' table.

    Args:
        tdb (TransactionalDatabase): The database instance with an active transaction to write data.
        post_data (dict): The post object, containing it's ID.
        rescan_id (int): The ID of the rescan previously created in the transaction.
    """
    tdb.execute(
        query="INSERT INTO %s (id, metadata, rescan_id) VALUES (%s, %s, %s)",
        params=(AsIs(Settings.INITIAL_POSTS_TABLE),
                post_data["id"], json.dumps(post_data), rescan_id)
    )


def create_post_rescan_entry(tdb: TransactionalDatabase, scheduled_start_at: datetime, post_id: str):
    """
    Creates an post rescan entry in the 'post_rescans' table.

    Args:
        tdb (TransactionalDatabase): The database instance with an active transaction to write data.
        scheduled_start_at (datetime): The time for which the post should be rescanned.
        post_id (str): The ID of the post to be rescanned.
    """
    tdb.execute(
        query="INSERT INTO %s (scheduled_start_at, post_id) VALUES (%s, %s)",
        params=(AsIs(Settings.POST_RESCAN_TABLE), scheduled_start_at, post_id)
    )


def mark_subreddit_rescan_processed(tdb: TransactionalDatabase, subreddit: str):
    """
    Marks the rescan for the given subreddit as processed, allowing for further rescans to be requeued.

    Args:
        tdb (TransactionalDatabase): he database instance with an active transaction to write data.
        subreddit (str): The subreddit whose rescan to mark as processed.
    """
    tdb.execute(
        query="UPDATE %s SET is_currently_queued=false, last_scanned=NOW() WHERE subreddit=%s",
        params=(AsIs(Settings.SUBSCRIPTIONS_TABLE), subreddit)
    )