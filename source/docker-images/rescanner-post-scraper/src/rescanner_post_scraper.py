import json
from typing import Dict, List, Tuple

from talos.config import Settings
from talos.logger import logger
from talos.components import ConsumerComponent
from talos.db import TransactionalDatabase


from rescanner.api import ResponseCollector
from rescanner.util import db_helpers, file_helpers


class RescannerPostScraper(ConsumerComponent):
    def __init__(self, retry_attempts: int, time_between_attempts: int, producing_queue: str):
        """
        Initializes the RescannerPostScraper object.

        Args:
            retry_attempts (int): The number of retry attempts for handling errors.
            time_between_attempts (int): The time to wait between attempts in seconds.
            producing_queue (str): The name of the queue from which to receive data.
        """
        super().__init__(retry_attempts, time_between_attempts, producing_queue)
        Settings.validate()

    def handle_fatal_error(self):
        pass

    def handle_bad_message(self):
        pass

    def collect_and_write_data(self, message: str) -> Tuple[str, Dict[Dict, Dict], List[str]]:
        """
        Collects the subreddit, responses and writes them to the file.

        Args:
            message (str): The message containing the subreddit from which to collect data.

        Returns:
            Tuple[str, Dict[Dict, Dict], List[str]]: The subreddit, the responses, and the paths of the files 
            to which the responses were written.
        """
        subreddit = json.loads(message)["subreddit"]

        responses: dict = ResponseCollector(
            subreddit,
            stopping_post_id=db_helpers.get_last_seen_post_id(subreddit)
        ).collect_responses()

        file_paths = file_helpers.write_responses_to_disk(subreddit, responses)

        return subreddit, responses, file_paths

    def parse_and_store_data(self, tdb: TransactionalDatabase, subreddit: str, responses: Dict[Dict, Dict], file_paths: List[str]) -> None:
        """
        Parses and stores the collected data in the database.

        Args:
            tdb (TransactionalDatabase): The database instance with an active transaction to write data.
            subreddit (str): The subreddit from which the data was collected.
            responses (Dict[Dict, Dict]): The responses that were collected.
            file_paths (List[str]): The paths of the files to which the responses were written.
        """
        filestore_ids = db_helpers.create_filestore_entries(
            tdb, file_paths
        )
        rescan_id = db_helpers.create_rescan_entry(tdb, subreddit)

        db_helpers.create_rescan_response_entries(
            tdb=tdb,
            rescan_id=rescan_id,
            filestore_ids=filestore_ids
        )

        for index, response in enumerate(responses):
            for post in response["contained_posts"]:
                db_helpers.create_scraped_post_entry(
                    tdb=tdb,
                    post_id=post["id"],
                    rescan_id=rescan_id,
                    file_id=filestore_ids[index]
                )

    def _handle_one_pass(self, message: str) -> None:
        """
        Handles one pass of the run loop. It collects, parses, and stores data, using a two-phase commit.

        Args:
            message (str): The message containing the subreddit from which to scrape data.
        """
        tdb = None
        file_paths = None

        try:
            logger.info(f"Received rescan request {message}...")

            subreddit, responses, file_paths = self.collect_and_write_data(message)
            logger.info(f"Collected {len(responses)} responses, wrote {file_paths} to disk.")

            tdb = TransactionalDatabase()
            tdb.connect()
            tdb.begin_transaction()

            self.parse_and_store_data(tdb, subreddit, responses, file_paths)
            db_helpers.mark_rescan_processed(tdb, subreddit)
            
            tdb.commit()
            logger.info(f"{len(responses)} stored in database. Cleaning up...")
        except Exception:
            if tdb:
                tdb.rollback_transaction()

            if file_paths:
                file_helpers.rollback_written_responses(file_paths)

            raise
        finally:
            if tdb:
                tdb.disconnect()

    def run(self):
        super().run()