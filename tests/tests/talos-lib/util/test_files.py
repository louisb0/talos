import unittest
from unittest.mock import Mock, patch
import os
import logging
import json

from talos.util.files import write_to_disk, delete_from_disk
from talos.config import Settings

class TestFiles(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(Settings.RESPONSE_STORAGE_PATH):
            os.mkdir(Settings.RESPONSE_STORAGE_PATH)

    def setUp(self):
        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

    def test_files(self):
        paths = ["f1.json", "f2.json", "f3.json"]

        with self.subTest("test_write"):
            contents = [{"key": "value1"}, {"key": "value2"}, {"key": "value3"}]
            stored_paths = write_to_disk(contents, paths)

            # same num of files in mem written to disk
            self.assertEqual(len(paths), len(stored_paths))
            for i, path in enumerate(stored_paths):
                # file name preserved (path added under hood)
                self.assertIn(paths[i], path)

                # assert content
                with open(path, "r") as f:
                    self.assertEqual(json.load(f), contents[i])

        with self.subTest("test_delete"):
            delete_from_disk(paths)

            for path in stored_paths:
                self.assertFalse(os.path.exists(path))
