from typing import List, Union, Dict
import json

from psycopg2.extensions import AsIs

from talos.db import ContextDatabase, TransactionalDatabase
from talos.config import Settings


def get_last_seen_post_id(subreddit: str) -> Union[int, None]:
    """
    Retrieves the ID of the last seen post in the given subreddit.

    Args:
        subreddit (str): The subreddit to query.

    Returns:
        Union[int, None]: The ID of the last seen post, or None if no posts were found.
    """

    # use context db so we don't have to hold a transaction open
    # for the entire scrape period, locking the tables
    with ContextDatabase() as cdb:
        cdb.execute(
            """
            SELECT 
                sp.id AS post_id
            FROM 
                %s r
            JOIN 
                %s sp ON r.id = sp.rescan_id
            WHERE 
                r.subreddit = %s
            ORDER BY 
                sp.scraped_at DESC
            LIMIT 1;
            """,
            (AsIs(Settings.RESCANS_TABLE), AsIs(
                Settings.SCRAPED_POST_TABLE), subreddit)
        )

        result = cdb.fetchone()
        return result[0] if result is not None else None


def create_rescan_entry(tdb: TransactionalDatabase, subreddit: str) -> int:
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
        params=(AsIs(Settings.RESCANS_TABLE), subreddit)
    )

    return tdb.fetchone()[0]


def create_scraped_post_entry(tdb: TransactionalDatabase, post_data: Dict, rescan_id: int):
    """
    Creates a scraped post entry in the 'scraped_posts' table.

    Args:
        tdb (TransactionalDatabase): The database instance with an active transaction to write data.
        post_data (dict): The post object, containing it's ID.
        rescan_id (int): The ID of the rescan previously created in the transaction.
    """
    tdb.execute(
        query="INSERT INTO %s (id, post_data, rescan_id) VALUES (%s, %s, %s)",
        params=(AsIs(Settings.SCRAPED_POST_TABLE),
                post_data["id"], json.dumps(post_data), rescan_id)
    )


def mark_rescan_processed(tdb: TransactionalDatabase, subreddit: str):
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
