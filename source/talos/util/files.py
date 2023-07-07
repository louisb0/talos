import json
import os
from typing import List, Dict

from talos.config import Settings
from talos.logger import logger
from talos.exceptions.util.files import *

@log_reraise_fatal_exception
@log_reraise_non_fatal_exception
def write_to_disk(content: List[Dict], paths: List[str]) -> List[str]:
    complete_paths = []
    for index, item in enumerate(content):
        file_path = f"{Settings.RESPONSE_STORAGE_PATH}/{paths[index]}"
        with open(file_path, "w") as out:
            out.write(
                json.dumps(item)
            )

        complete_paths.append(file_path)

    return complete_paths

@log_reraise_fatal_exception
@log_reraise_non_fatal_exception
def delete_from_disk(paths: List[str]):
    for path in paths:
        file_path = f"{Settings.RESPONSE_STORAGE_PATH}/{path}"

        try:
            os.remove(file_path)
        except FileNotFoundError:
            logger.info(f"Tried to delete file {path} which does not exist. Continuing...")
            continue