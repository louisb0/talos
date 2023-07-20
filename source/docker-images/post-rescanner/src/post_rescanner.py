import json
import sys
import time

from talos.components import ConsumerComponent
from talos.config import Settings
from talos.logger import logger
from talos.api import Requests
from talos.queuing import RabbitMQ

from lib.util import CommentCollector, db_helpers, queue_helpers


class PostRescanner(ConsumerComponent):
    def __init__(self, retry_attempts: int, time_between_attempts: int, producing_queue: str):
        """
        Initializes the PostRescanner object.

        Args:
            retry_attempts (int): The number of retry attempts for handling errors.
            time_between_attempts (int): The time to wait between attempts in seconds.
            producing_queue (str): The name of the queue from which to receive data.
        """
        super().__init__(retry_attempts, time_between_attempts, producing_queue)
        Settings.validate()

    def handle_critical_error(self) -> None:
        """
        Handles critical errors which could not be retried.
        """
        logger.critical("handle_critical_error() hit. Exiting...")
        sys.exit(1)

    def _handle_one_pass(self, message: str) -> None:
        """
        Receives a rescan message from POST_RESCAN_QUEUE, of the following structure;

            "post_id": ...,
            "post_rescans_id": ...,
            "type": ...,
            "api_request": {
                "url": f"https://gateway.reddit.com/desktopapi/v1/...",
                "method": 1,  # Requests.TYPE_POST,
                "body": {
                    "token": ...
                }
            }

        It uses this to then send the API request, fetching 'base layer' data or nested
        comment data. Within the base layer data is the 'aged' post meta data, which is
        inserted into UPDATED_POSTS_TABLE, as well as unnested comments. Any nested comments
        present in the comment section is then requeued again into POST_RESCAN_QUEUE.
        
        Args:
            message (str): The message containing the information required to rescan.
        """
        message = json.loads(message)
        post_rescan_id = message["post_rescans_id"]

        logger.info(
            f"Processing post_rescan={post_rescan_id} (post_id={message['post_id']})..."
        )

        response = self.requests_obj.send_from_message(message["api_request"])
        raw_comments, more_comments, continue_threads = CommentCollector(
            api_response=response
        ).collect_comments()

        if message["type"] == "base":
            # The 'base layer' contains the updated post metadata.
            db_helpers.insert_updated_post(
                post=response["posts"][message["post_id"]],
                post_rescan_id=post_rescan_id
            )

            db_helpers.set_post_rescan_started(post_rescan_id)
        elif message["type"] == "continue":
            # continueThread contains duplicate comment from base layer, 'continuing' from that comment.
            raw_comments.pop(0)

        db_helpers.insert_comments(
            comments=raw_comments,
            post_rescan_id=post_rescan_id
        )
        
        db_helpers.update_post_rescan_seen(post_rescan_id)

        with RabbitMQ((Settings.POST_RESCAN_QUEUE,)) as rabbitmq:
            queue_helpers.queue_more_comments_scan(
                rabbitmq=rabbitmq,
                more_comments=more_comments,
                post_rescan_id=post_rescan_id
            )

            queue_helpers.queue_continue_thread_scan(
                rabbitmq=rabbitmq,
                continue_threads=continue_threads,
                post_rescan_id=post_rescan_id
            )

        logger.info(
            f"Finished. rawComments={len(raw_comments)}, moreComments={len(more_comments)}, continueThreads={len(continue_threads)}."
        )
        time.sleep(1)

    def run(self):
        # Persistent object for token rotation.
        self.requests_obj = Requests()

        super().run()
