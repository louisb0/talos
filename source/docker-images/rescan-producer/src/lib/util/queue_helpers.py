import json
from typing import Dict

from talos.queuing import RabbitMQ
from talos.config import Settings


def queue_subreddit_rescan(subreddit: str) -> None:
    """
    Adds the subreddit to the subreddit rescan queue, which is consumed
    by `subreddit-rescanner`.
    """
    with RabbitMQ(queues=(Settings.SUBREDDIT_RESCAN_QUEUE,)) as queue:
        queue.publish_message(
            queue_name=Settings.SUBREDDIT_RESCAN_QUEUE,
            message=json.dumps({
                "subreddit": subreddit
            })
        )


def queue_post_rescan(rabbitmq: RabbitMQ, api_request: Dict, post_id: str, post_rescan_id: int):
    """
    Adds the API request to fetch the updated post meta data and
    comments to the post rescan queue, which is consumed by `post-rescanner`.
    """
    rabbitmq.publish_message(
        queue_name=Settings.POST_RESCAN_QUEUE,
        message=json.dumps({
            "post_id": post_id,
            "post_rescans_id": post_rescan_id,
            "type": "base",
            "api_request": api_request
        })
    )
