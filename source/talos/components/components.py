import time
from abc import ABC, abstractmethod

from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from talos.config import Settings
from talos.queuing import RabbitMQ
from talos.exceptions.base import FatalException, NonFatalException
from talos.exceptions.queuing import BadMessageException
from talos.logger import logger


class BaseComponent(ABC):
    def __init__(self, retry_attempts, time_between_attempts):
        self.retry_attempts = retry_attempts
        self.time_between_attempts = time_between_attempts

    def handle_propogated_error(self, error):
        if not isinstance(error, (NonFatalException, FatalException)):
            logger.exception("An unknown error occurred.")

        self.handle_fatal_error()

    def handle_one_pass_with_retry(self, *args, **kwargs):
        @retry(stop=stop_after_attempt(self.retry_attempts),
               wait=wait_fixed(self.time_between_attempts),
               retry=retry_if_exception_type(NonFatalException),
               before_sleep=lambda retry_state: logger.info(f"Retrying... {retry_state}"),
               reraise=True)
        def _handle_one_pass_with_retry(*args, **kwargs):
            self._handle_one_pass(*args, **kwargs)

        _handle_one_pass_with_retry(*args, **kwargs)

    @abstractmethod
    def handle_fatal_error(self):
        pass

    @abstractmethod
    def _handle_one_pass(self, *args):
        pass

    def run(self):
        logger.info(
            f"{type(self).__name__} starting. Sleeping for {Settings.STARTUP_SLEEP_TIME_SECS}..."
        )

        time.sleep(Settings.STARTUP_SLEEP_TIME_SECS)


class ProducerComponent(BaseComponent):
    def __init__(self, retry_attempts, time_between_attempts):
        super().__init__(retry_attempts, time_between_attempts)

    @abstractmethod
    def handle_fatal_error(self):
        pass

    @abstractmethod
    def _handle_one_pass(self, *args):
        pass

    def run(self):
        super().run()

        while True:
            try:
                self.handle_one_pass_with_retry()
            except Exception as e:
                self.handle_propogated_error(e)


class ConsumerComponent(BaseComponent):
    def __init__(self, retry_attempts, time_between_attempts, producing_queue):
        super().__init__(retry_attempts, time_between_attempts)
        self.producing_queue = producing_queue

    @abstractmethod
    def handle_fatal_error(self):
        pass

    @abstractmethod
    def handle_bad_message(self):
        pass

    @abstractmethod
    def _handle_one_pass(self, *args):
        pass

    def run(self):
        super().run()

        try:
            with RabbitMQ(self.producing_queue) as queue:
                queue.continually_consume_messages(
                    queue_name=self.producing_queue,
                    callback_function=self.handle_one_pass_with_retry
                )
        except BadMessageException as bme:
            self.handle_bad_message(bme)
        except Exception as e:
            self.handle_propogated_error(e)