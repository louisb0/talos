import json
from typing import List

from talos.queuing import RabbitMQ
from talos.config import Settings

def queue_more_comments_scan(rabbitmq: RabbitMQ, more_comments: List[dict], post_rescan_id: int) -> None:
    """
    Queues into POST_RESCAN_QUEUE subsequent API requests to fetch nested 'show more'
    comment sections.

    Args:
        rabbitmq (RabbitMQ): The active RabbitMQ instance used to publish the message.
        more_comments (List[Dict]): List of moreComment objects.
        post_rescan_id (int): The ID of the post rescan which the comments originated.
    """
    for comment in more_comments:
        rabbitmq.publish_message(
            queue_name=Settings.POST_RESCAN_QUEUE,
            message=json.dumps({
                "post_id": comment["postId"],
                "post_rescans_id": post_rescan_id,
                "type": "more",
                "api_request": {
                    "url": f"https://gateway.reddit.com/desktopapi/v1/morecomments/{comment['id']}",
                    "method": 1,  # Requests.TYPE_POST,
                    "body": {
                        "token": comment["token"]
                    }
                }
            })
        )

def queue_continue_thread_scan(rabbitmq: RabbitMQ, continue_threads: List[dict], post_rescan_id: int) -> None:
    """
    Queues into POST_RESCAN_QUEUE subsequent API requests to fetch nested 'continue thread'
    comment sections.

    Args:
        rabbitmq (RabbitMQ): The active RabbitMQ instance used to publish the message.
        continue_threads (List[Dict]): List of continueThread objects.
        post_rescan_id (int): The ID of the post rescan which the comments originated.
    """
    for comment in continue_threads:
        rabbitmq.publish_message(
            queue_name=Settings.POST_RESCAN_QUEUE,
            message=json.dumps({
                "post_id": comment["postId"],
                "post_rescans_id": post_rescan_id,
                "type": "continue",
                "api_request": {
                    "url": f"https://gateway.reddit.com/desktopapi/v1/postcomments/{comment['postId']}/{comment['parentId']}",
                    "method": 0,  # Requests.TYPE_GET
                }
            })
        )