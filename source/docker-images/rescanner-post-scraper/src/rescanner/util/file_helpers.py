import os
from typing import Dict, List

from talos.util import files
from talos.config import Settings

# EVERYTHING UNUSED AFTER REFACTOR OF THIS COMPONENT

def write_responses_to_disk(subreddit: str, responses: Dict[Dict, Dict]) -> List[str]:
    """
    Writes raw subreddit responses to disk.

    Args:
        subreddit (str): The subreddit from which responses were collected.
        responses (Dict[Dict, Dict]): The object containing the raw responses to write to disk.

    Returns:
        List[str]: The paths of the files to which the responses were written.
    """
    file_contents = [response["raw_response"] for response in responses]
    first_ids = [response["contained_posts"][0]["id"]
                 for response in responses]
    paths = [f"{subreddit}-{first_id}.json" for first_id in first_ids]

    return files.write_to_disk(
        content=file_contents,
        paths=paths
    )


def rollback_written_responses(paths: List[str]):
    """
    Deletes the subreddit responses from disk, in case one of the two phases fail 
    in the two-phase commit, allowing for a retry.
    """
    files.delete_from_disk(paths)


def create_responses_directory() -> None:
    """
    Creates the /app/responses/ directory if it doesn't exist on launch.
    """
    if not os.path.exists(Settings.RESPONSE_STORAGE_PATH):
        os.mkdir(Settings.RESPONSE_STORAGE_PATH)