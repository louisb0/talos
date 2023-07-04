
from talos.config import Settings
from talos.components import ConsumerComponent

class RescannerPostScraperUtility():
    pass

class RescannerPostScraper(ConsumerComponent):
    def __init__(self, retry_attempts, time_between_attempts):
        super().__init__(retry_attempts, time_between_attempts)
        Settings.validate()

    def handle_fatal_error(self):
        pass

    def handle_bad_message(self):
        pass

    def _handle_one_pass(self, *args):
        pass

    def run(self):
        pass