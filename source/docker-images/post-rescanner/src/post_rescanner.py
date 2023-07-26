import json
import sys
import time
from typing import Tuple

from talos.components import ConsumerComponent
from talos.config import Settings
from talos.logger import logger
from talos.api import Requests
from talos.queuing import RabbitMQ
from talos.db import ContextDatabase

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
        logger.alert("handle_critical_error() hit. Exiting...")
        sys.exit(1)

    def collect_post_data(self, message: dict) -> Tuple[dict, dict, dict, dict]:
        """
        Collects all data from Reddit using the information in the RabbitMQ message.

        Args:
            message (str): The message from the POST_RESCAN_QUEUE

        Returns:
            Tuple[Dict, Dict, Dict, Dict]: response, reaw_comments, more_comments, continue_threads
        """
        response = self.requests_obj.send_from_message(message["api_request"])
        raw_comments, more_comments, continue_threads = CommentCollector(
            api_response=response
        ).collect_comments()

        return response, raw_comments, more_comments, continue_threads

    def handle_base_layer_message(self, post: dict, post_rescan_id: int) -> None:
        """
        Performs additional processing required for 'base layer' responses,
        i.e. updating the post and marking the rescan started.

        Args:
            post (dict): The JSON object representing the updated post.
            post_rescan_id (int): The ID of the post rescan this update is associated with.
        """
        with ContextDatabase() as cdb:
            db_helpers.insert_updated_post(
                db=cdb,
                post=post,
                post_rescan_id=post_rescan_id
            )
            db_helpers.set_post_rescan_started(
                db=cdb,
                post_rescan_id=post_rescan_id
            )
            cdb.commit()

    def process_found_comments(self, raw_comments: dict, more_comments: dict, continue_threads: dict, post_rescan_id: int) -> None:
        """
        Inserts the raw_comments into the database, and queues subsequent requests
        for nested moreComments and continueThreads.

        Args:
            raw_comments (dict): The comments found in the post.
            more_comments (dict): The objects used to fetch moreComments.
            continue_threads (dict): The objects used to fetch continueThreads.
            post_rescan_id (int): The post rescan ID these comments are associated with.
        """
        db_helpers.insert_comments(
            comments=raw_comments,
            post_rescan_id=post_rescan_id
        )

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

        response, raw_comments, more_comments, continue_threads = self.collect_post_data(
            message)

        if message["type"] == "base":
            self.handle_base_layer_message(  # base layer contains updated post
                post=response["posts"][message["post_id"]],
                post_rescan_id=post_rescan_id
            )
        elif message["type"] == "continue":
            raw_comments.pop(0)  # duplicate root in continue thread

        self.process_found_comments(
            raw_comments,
            more_comments,
            continue_threads,
            post_rescan_id
        )

        logger.info(
            f"Processed {len(raw_comments)} comments, queued a further {len(more_comments) + len(continue_threads)} requests. " +
            f"Sleeping for {Settings.TIME_BETWEEN_POST_RESCANS}s..."
        )
        time.sleep(Settings.TIME_BETWEEN_POST_RESCANS)

    def run(self):
        # Persistent object for token rotation.
        self.requests_obj = Requests()

        super().run()
