import json
from typing import List

from psycopg2.extensions import AsIs

from talos.db import ContextDatabase
from talos.config import Settings


def insert_updated_post(db: ContextDatabase, post: dict, post_rescan_id: int) -> None:
    """
    Inserts the new post meta data into the database.

    Args:
        post (dict): The JSON object containing the post meta data.
        post_rescan_id (int): The ID of the post rescan which the update is associated with.
    """
    db.execute(
        query="INSERT INTO %s (updated_metadata, post_scan_id) VALUES (%s, %s)",
        params=(AsIs(Settings.UPDATED_POSTS_TABLE),
                json.dumps(post), post_rescan_id),
        auto_commit=False
    )


def set_post_rescan_started(db: ContextDatabase, post_rescan_id: int) -> None:
    """
    Updates an existing post rescan with the last seen time.

    Args:
        post_rescan_id (int): The ID of the post rescan to update.
    """
    db.execute(
        query="UPDATE %s SET started_at=NOW() WHERE id=%s",
        params=(AsIs(Settings.POST_RESCAN_TABLE), post_rescan_id),
        auto_commit=False
    )


# def update_post_rescan_seen(post_rescan_id: int) -> None:
#     """
#     Updates an existing post rescan with the last seen time.

#     Args:
#         post_rescan_id (int): The ID of the post rescan to update.
#     """
#     with ContextDatabase() as db:
#         db.execute(
#             query="UPDATE %s SET last_seen=NOW() WHERE id=%s",
#             params=(AsIs(Settings.POST_RESCAN_TABLE), post_rescan_id),
#             auto_commit=True
#         )


def insert_comments(comments: List[dict], post_rescan_id: int) -> None:
    """
    Inserts a batch of comments - those in the API response - into the database.

    Args:
        comments (List[dict]): A list of all the JSON comment objects to insert.
        post_rescan_id (int): The ID of the post rescan which the comments are associated with.
    """
    with ContextDatabase() as db:
        for comment in comments:
            db.execute(
                query="INSERT INTO %s (id, parent_id, comment_data, post_scan_id) VALUES (%s, %s, %s, %s)",
                params=(AsIs(Settings.SCRAPED_COMMENTS_TABLE),
                        comment["id"], comment["parentId"], json.dumps(comment), post_rescan_id),
                auto_commit=False
            )

        db.commit()
