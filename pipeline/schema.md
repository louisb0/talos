```sql
CREATE TABLE posts (
    id VARCHAR(20) PRIMARY KEY,
    title VARCHAR(300),
    author VARCHAR(30),
    subreddit VARCHAR(30) NOT NULL,
    type_hint VARCHAR(10) NOT NULL,
    score INT NOT NULL,
    upvote_ratio FLOAT,
    comment_count INT NOT NULL,
    num_awards INT NOT NULL,
    flairs TEXT, 
    text_content TEXT,
    media_link TEXT,
    post_link TEXT NOT NULL,
    content_link TEXT,
    is_nsfw BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    scraped_at TIMESTAMP NOT NULL
);

CREATE TABLE comments (
    id VARCHAR(20) PRIMARY KEY,
    parent_id VARCHAR(20),
    author VARCHAR(30),
    score INT NOT NULL,
    content TEXT,
    is_deleted BOOLEAN NOT NULL,
    post_id VARCHAR(20) NOT NULL,
    FOREIGN KEY (post_id) REFERENCES posts(id)
);
```