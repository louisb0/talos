from typing import List, Union

from talos.db import ContextDatabase, TransactionalDatabase

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
                rescans r
            JOIN 
                scraped_posts sp ON r.id = sp.rescan_id
            WHERE 
                r.subreddit = 'universityofauckland'
            ORDER BY 
                sp.scraped_at DESC
            LIMIT 1;
            """,
            (subreddit,)
        )

        result = cdb.fetchone()
        return result[0] if result is not None else None


def create_filestore_entries(tdb: TransactionalDatabase, file_names: List[str]) -> List[int]:
    """
    Creates entries in the 'filestore' table for each file name.

    Args:
        tdb (TransactionalDatabase): The database instance with an active transaction to write data.
        file_names (List[str]): The names of the files for which to create entries.

    Returns:
        List[int]: The IDs of the created filestore entries.
    """
    ids = []
    for file_name in file_names:
        tdb.execute(
            query="INSERT INTO filestore (file_name) VALUES (%s) RETURNING id",
            params=(file_name,)
        )
        ids.append(tdb.fetchone()[0])

    return ids


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
        query="INSERT INTO rescans (subreddit) VALUES (%s) RETURNING id",
        params=(subreddit,)
    )

    return tdb.fetchone()[0]


def create_rescan_response_entries(tdb: TransactionalDatabase, rescan_id: int, filestore_ids: List[int]) -> None:
    """
    Creates rescan response entries in the 'rescan_responses' table for each filestore ID.

    Args:
        tdb (TransactionalDatabase): The database instance with an active transaction to write data.
        rescan_id (int): The ID of the rescan for which to create entries.
        filestore_ids (List[int]): The IDs of the filestore entries for which to create rescan response entries.
    """
    for filestore_id in filestore_ids:
        tdb.execute(
            query="INSERT INTO rescan_responses (rescan_id, file_id) VALUES (%s, %s)",
            params=(rescan_id, filestore_id)
        )


def create_scraped_post_entry(tdb: TransactionalDatabase, post_id: int, rescan_id: int, file_id: int):
    """
    Creates a scraped post entry in the 'scraped_posts' table.

    Args:
        tdb (TransactionalDatabase): The database instance with an active transaction to write data.
        post_id (int): The ID of the post.
        rescan_id (int): The ID of the rescan previously created in the transaction.
        file_id (int): The ID of the file previously created in the transaction.
    """
    tdb.execute(
        query="INSERT INTO scraped_posts (id, rescan_id, file_id) VALUES (%s, %s, %s)",
        params=(post_id, rescan_id, file_id)
    )


def mark_rescan_processed(tdb: TransactionalDatabase, subreddit: str):
    """
    Marks the rescan for the given subreddit as processed, allowing for further rescans to be requeued.

    Args:
        tdb (TransactionalDatabase): he database instance with an active transaction to write data.
        subreddit (str): The subreddit whose rescan to mark as processed.
    """
    tdb.execute(
        query="UPDATE subscriptions SET is_currently_queued=false WHERE subreddit=%s",
        params=(subreddit,)
    )
