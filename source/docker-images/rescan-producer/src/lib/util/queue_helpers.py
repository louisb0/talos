import json

from talos.queuing import RabbitMQ
from talos.config import Settings
from talos.logger import logger

def queue_rescan(subreddit: str) -> None:
    """
    Adds the subreddit to the rescan queue for rescanner-post-scraper to consume.
    """
    with RabbitMQ(queues=(Settings.RESCAN_QUEUE,)) as queue:
        queue.publish_message(
            queue_name=Settings.RESCAN_QUEUE,
            message=json.dumps({
                "subreddit": subreddit
            })
        )
    
    logger.debug(f"Queued rescan for {subreddit}.")
