from abc import abstractmethod

from talos.components import BaseComponent


class ProducerComponent(BaseComponent):
    """
    Abstract base class for all producer components in the system.

    Args:
        retry_attempts: Number of attempts before stopping retries on _handle_one_pass().
        time_between_attempts: Time in seconds between retry attempts on _handle_one_pass().
    """

    def __init__(self, retry_attempts: int, time_between_attempts: int):
        super().__init__(retry_attempts, time_between_attempts)

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
        Abstract method for handling one pass of the component's main loop, typically reading
        from a table to produce tasks for queues.

        Important: Should contain time.sleep(), as this is executed in a while true.

        Args:
            args: Positional arguments to be used by the method.
            kwargs: Keyword arguments to be passed to the _handle_one_pass method.
        """
        pass

    def run(self):
        """
        Starts the producer component, continually running one pass.
        Refers exceptions to handle_critical_error().
        """
        super().run()

        while True:
            try:
                self.handle_one_pass_with_retry()
            except Exception as e:
                self.handle_propogated_error(e)
