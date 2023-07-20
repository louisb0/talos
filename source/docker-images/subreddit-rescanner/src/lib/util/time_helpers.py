from typing import Dict
from datetime import datetime, timezone, timedelta

def get_scheduled_scrape_time(post: Dict) -> datetime:
    """
    Calculates the UTC time at which a post turns 7 days old.

    Args:
        post (dict): The post JSON object with a 'createdAt' field.
    """
    created_at = datetime.strptime(
                post["createdAt"], "%Y-%m-%dT%H:%M:%S.%f%z"
            )

    age_in_seconds = (datetime.now(timezone.utc) -
                        created_at).total_seconds()

    remaining_seconds = 7*24*60*60 - age_in_seconds

    if remaining_seconds <= 0:
        return datetime.now(timezone.utc)
    else:
        return datetime.now(timezone.utc) + \
            timedelta(seconds=remaining_seconds)