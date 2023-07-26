"""
CREATE TABLE posts (
    id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(300),
    author VARCHAR(30),
    subreddit VARCHAR(30) NOT NULL,
    type_hint VARCHAR(10) NOT NULL,
    score INT NOT NULL,
    upvote_ratio FLOAT,
    comment_count INT NOT NULL,
    flairs TEXT, 
    text_content TEXT,
    media_link TEXT,
    is_nsfw BOOLEAN NOT NULL,
    post_link TEXT NOT NULL,
    content_link TEXT,
    created_at TIMESTAMP NOT NULL,
    scraped_at TIMESTAMP NOT NULL
)
"""

from datetime import datetime

from util import Database


class Post:
    def __init__(self, initial: dict, updated: dict, scraped_at: datetime):
        self.id = initial.get("id")
        self.title = initial.get("title")
        self.author = Parser.parse_author(initial)
        self.subreddit = initial.get("subreddit", {}).get("name")
        self.type_hint = Parser.parse_type(initial, updated)
        self.score = updated.get("score")
        self.upvote_ratio = updated.get("upvoteRatio")
        self.comment_count = updated.get("numComments")
        self.flairs = Parser.parse_flairs(updated)
        self.text_content = Parser.parse_text_content(initial)
        self.media_link = Parser.parse_media_link(
            initial) if not self.text_content else None
        self.is_nsfw = updated.get("isNSFW")
        self.post_link = updated.get("permalink")
        self.content_link = Parser.parse_content_link(initial)
        self.scraped_at = scraped_at
        self.created_at = initial.get("createdAt")

    def insert(self, db: Database):
        query = """
        INSERT INTO posts (
            id, 
            title, 
            author, 
            subreddit, 
            type_hint, 
            score, 
            upvote_ratio, 
            comment_count, 
            flairs, 
            text_content, 
            media_link, 
            is_nsfw, 
            post_link, 
            content_link, 
            created_at, 
            scraped_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = (
            self.id,
            self.title,
            self.author,
            self.subreddit,
            self.type_hint,
            self.score,
            self.upvote_ratio,
            self.comment_count,
            self.flairs,
            self.text_content,
            self.media_link,
            self.is_nsfw,
            self.post_link,
            self.content_link,
            self.created_at,
            self.scraped_at
        )

        print(f"Processed post {self.id} subreddit={self.subreddit}")
        db.cursor.execute(query, data)

    def __str__(self):
        return (
            f"Post(\n"
            f"\tID: {self.id}\n"
            f"\tTitle: {self.title}\n"
            f"\tAuthor: {self.author}\n"
            f"\tSubreddit: {self.subreddit}\n"
            f"\tType: {self.type_hint}\n"
            f"\tScore: {self.score}\n"
            f"\tUpvote ratio: {self.upvote_ratio}\n"
            f"\tComment count: {self.comment_count}\n"
            f"\tFlairs: {self.flairs}\n"
            f"\tText content: {self.text_content}\n"
            f"\tMedia link: {self.media_link}\n"
            f"\tNSFW: {self.is_nsfw}\n"
            f"\tPost link: {self.post_link}\n"
            f"\tContent: {self.content}\n"
            f"\tScraped at: {self.scraped_at}\n"
            f"\tCreated at: {self.created_at}\n"
            f")"
        )


class Parser:
    @staticmethod
    def parse_author(initial: dict):
        author_info = initial.get("authorInfo")
        return author_info.get("name") if author_info else "[unknown]"

    @staticmethod
    def parse_type(initial: dict, updated: dict):
        isSelfPost = initial.get("isSelfPost", False)
        gallery = initial.get("gallery")
        media = initial.get("media")
        url = initial.get("url")

        if isSelfPost:
            type = "TEXT"
        elif gallery is not None:
            type = "GALLERY"
        elif media is not None:
            mediaType = media.get("typeHint", "")
            if mediaType == "IMAGE" or "i.redd.it" in url:
                type = "IMAGE"
            elif mediaType == "GIFVIDEO":
                type = "GIF"
            elif mediaType in ["VIDEO", "EMBED"] or "v.redd.it" in url:
                type = "VIDEO"
            else:
                type = "LINK"
        else:
            type = "LINK"

        return type

    @staticmethod
    def parse_flairs(updated: dict):
        flair_info = updated.get("flair")
        flairs = [flair_item.get("text", None)
                  for flair_item in flair_info] if flair_info else None

        if flairs:
            flairs = [flair for flair in flairs if flair is not None]

        return ",".join(flairs) if flairs else None

    @staticmethod
    def parse_text_content(initial: dict):
        content = initial.get("content")
        return content.get("markdown") if content else None

    @staticmethod
    def parse_media_link(initial: dict):
        media = initial.get("media")
        return media.get("markdownContent") if media else None

    @staticmethod
    def parse_content_link(initial: dict):
        url = initial.get("url")
        return url if "reddit.com" not in url else None
