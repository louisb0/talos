import time
from abc import ABC, abstractmethod

from talos.config import Settings
from talos.logger import logger
from talos.exceptions.base import FatalException, NonFatalException
from talos.util.decorators import retry_fixed


class BaseComponent(ABC):
    """
    Abstract base class for all components in the system.

    Args:
        retry_attempts: Number of attempts before stopping retries on _handle_one_pass().
        time_between_attempts: Time in seconds between retry attempts on _handle_one_pass().
    """

    def __init__(self, retry_attempts: int, time_between_attempts: int):
        self.retry_attempts = retry_attempts
        self.time_between_attempts = time_between_attempts

    def route_error(self, error: BaseException):
        """
        Routes an error to a handle when propagated up from a lower-level function or method.
        Reached when exceeding NonFatalException retries, or in case of a FatalException.

        Args:
            error: The exception instance that has been raised.
        """
        if not isinstance(error, (NonFatalException, FatalException)):
            logger.exception("An unknown error occurred.")

        self.handle_critical_error()

    def handle_one_pass_with_retry(self, *args, **kwargs):
        """
        Handles one pass of the component's main loop with retry capabilities. Called by run() in subclasses.
        Should not be implemented.

        Args:
            args: Positional arguments to be passed to the _handle_one_pass method.
            kwargs: Keyword arguments to be passed to the _handle_one_pass method.
        """
        @retry_fixed(retry_attempts=self.retry_attempts, time_between_attempts=self.time_between_attempts, exception_types=(NonFatalException,))
        def _handle_one_pass_with_retry(*args, **kwargs):
            self._handle_one_pass(*args, **kwargs)

        _handle_one_pass_with_retry(*args, **kwargs)

    @abstractmethod
    def handle_critical_error(self):
        """
        Abstract method to handle critical errors which could not be retried.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def _handle_one_pass(self, *args, **kwargs):
        """
        Abstract method for handling one pass of the component's main loop, such as handling a
        message or reading from a table for tasks.
        Must be implemented by subclasses, executed with retry by run() in subclasses.

        Args:
            args: Positional arguments to be used by the method.
            kwargs: Keyword arguments to be passed to the _handle_one_pass method.
        """
        pass

    def run(self):
        """
        Starts the component after a delay defined by Settings.STARTUP_SLEEP_TIME_SECS.
        Subclasses call this implementation on start to reduce boiler plate.
        """
        logger.info(
            f"{type(self).__name__} starting. Sleeping for {Settings.STARTUP_SLEEP_TIME_SECS}s while containers start up..."
        )

        time.sleep(Settings.STARTUP_SLEEP_TIME_SECS)