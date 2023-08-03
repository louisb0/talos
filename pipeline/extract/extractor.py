from typing import Tuple
from datetime import datetime

import pandas as pd

from util.db import Database


class Extractor:
    def __init__(self, db: Database, past: datetime, subreddit: str):
        self.db = db
        self.past = past
        self.subreddit = subreddit

    def get_dfs(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        posts = self._get_posts_from_db()
        comments = self._get_comments_from_db(tuple(posts["post_scan_id"]))

        return self._process_dfs(posts, comments)

    def _get_posts_from_db(self):
        self.db.cursor.execute("""
            SELECT 
                initial_posts.*, 
                updated_posts.*
            FROM 
                initial_posts
            JOIN 
                post_rescans
            ON 
                initial_posts.id = post_rescans.post_id
            JOIN 
                updated_posts
            ON 
                post_rescans.id = updated_posts.post_scan_id
            JOIN 
                subreddit_rescans
            ON 
                initial_posts.rescan_id = subreddit_rescans.id
            JOIN 
                subscriptions
            ON 
                subreddit_rescans.subreddit = subscriptions.subreddit
            WHERE 
                updated_posts.scraped_at >= %s
            AND 
                subscriptions.subreddit = %s
        """, (self.past, self.subreddit))

        return pd.DataFrame(
            self.db.cursor.fetchall(),
            columns=["post_id", "initial_scraped_at", "initial_data", "rescan_id",
                     "updated_idx", "updated_scraped_at", "updated_data", "post_scan_id"]
        )

    def _get_comments_from_db(self, post_scan_ids: Tuple[int]):
        self.db.cursor.execute(
            """
                SELECT 
                    scraped_comments.*
                FROM 
                    scraped_comments
                WHERE 
                    scraped_comments.post_scan_id IN %s
            """,
            (post_scan_ids,)
        )

        return pd.DataFrame(
            self.db.cursor.fetchall(),
            columns=[desc[0] for desc in self.db.cursor.description]
        )

    # arguably new class from below here
    def _process_dfs(self, posts: pd.DataFrame, comments: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        # change post_scan_id to post_id in comments
        post_id_mapping = posts[
            ['post_id', 'post_scan_id']
        ].set_index('post_scan_id').to_dict()['post_id']

        comments['post_id'] = comments['post_scan_id'].map(post_id_mapping)
        comments = comments.drop(columns=['post_scan_id'])

        # select/rename fields
        posts = posts[["post_id", "initial_scraped_at",
                       "initial_data", "updated_data"]]
        posts.columns = ["id", "scraped_at", "initial_json", "updated_json"]

        comments.columns = ["id", "parent_id", "json", "post_id"]

        return posts, comments
