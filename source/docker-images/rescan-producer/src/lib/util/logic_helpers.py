from typing import Tuple
from datetime import datetime, timedelta, timezone


def is_rescan_required(subscription: Tuple[str]) -> bool:
    """
    Checks if a rescan is required. That is, the item is subscribed, not queued,
    and either has never been scanned or is due rescanning (per last scanned, time between scans).

    Args:
        subscription (Tuple[str]): The subscription object from the database.
    
    Returns:
        bool: If the (potentially) subscribed subreddit is due a rescan of posts.
    """
    is_subscribed, time_between_scans, last_scanned_at, is_currently_queued = \
        subscription[1:5]

    if not is_subscribed or is_currently_queued:
        return False

    if last_scanned_at is None:
        return True

    next_scan_time = last_scanned_at.replace(tzinfo=timezone.utc) + \
        timedelta(seconds=time_between_scans)

    return datetime.now(timezone.utc) >= next_scan_time
