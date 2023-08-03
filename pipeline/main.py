from datetime import datetime, timezone, timedelta

from util import Database
from extract import Extractor
from models import Post, Comment

if __name__ == "__main__":
    db = Database()
    
    past = datetime.now(timezone.utc) - timedelta(days=10)
    posts, comments = Extractor(db, past, "nootropics").get_dfs()

    # process posts and insert into db
    subsetted_info = posts[["initial_json", "updated_json", "scraped_at"]]
    for initial_json, updated_json, scraped_at in subsetted_info.itertuples(index=False):
        Post(
            initial_json,
            updated_json,
            scraped_at
        ).insert(db)

    # same for comments
    for comment_json in comments["json"]:
        Comment(
            comment_json
        ).insert(db)

    db.connection.commit()
