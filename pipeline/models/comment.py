"""
CREATE TABLE comments (
    id VARCHAR(20) PRIMARY KEY,
    parent_id VARCHAR(20) NOT NULL,
    author VARCHAR(30),
    score INT NOT NULL,
    content TEXT,
    replies INT NOT NULL,
    is_deleted BOOLEAN NOT NULL,
    post_id VARCHAR(20) NOT NULL,
    FOREIGN KEY post_id REFERENCES posts(id)
)
"""
from util import Database

class Comment:
    def __init__(self, comment: dict):
        self.id = comment.get("id")
        self.parent_id = comment.get("parentId")
        self.author = comment.get("author")
        self.score = comment.get("score")
        self.content = comment.get("bodyMD")
        self.is_deleted = comment.get("isDeleted")
        self.post_id = comment.get("postId")

    def insert(self, db: Database):
        query = """
        INSERT INTO comments (
            id, 
            parent_id, 
            author, 
            score, 
            content, 
            is_deleted,
            post_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        data = (
            self.id,
            self.parent_id,
            self.author,
            self.score,
            self.content,
            self.is_deleted,
            self.post_id
        )

        print(f"Processed comment {self.id} post={self.post_id}")
        db.cursor.execute(query, data)
