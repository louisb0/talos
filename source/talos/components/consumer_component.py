from abc import abstractmethod

from talos.components import BaseComponent
from talos.queuing import RabbitMQ


class ConsumerComponent(BaseComponent):
    """
    Abstract base class for all producer components in the system.

    Args:
        retry_attempts (int): Number of attempts before stopping retries on _handle_one_pass().
        time_between_attempts (int): Time in seconds between retry attempts on _handle_one_pass().
        producing_queue (str): The name of the queue to consume from, i.e. the producer.
    """

    def __init__(self, retry_attempts: int, time_between_attempts: int, producing_queue: str):
        super().__init__(retry_attempts, time_between_attempts)

        self.producing_queue = producing_queue

    @abstractmethod
    def handle_critical_error(self):
        """
        Abstract method to handle critical errors which could not be retried.
        Must be implemented by subclasses.
        """
        pass

    @abstractmethod
    def _handle_one_pass(self, *args):
        """
        Abstract method for handling one pass of the component's main loop, in this
        case reading from the queue

        Important: Should contain time.sleep(), as this is executed in a while true.

        Args:
            args: Positional arguments to be used by the method.
            kwargs: Keyword arguments to be passed to the _handle_one_pass method.
        """
        pass

    def run(self):
        """
        Starts the consumer component, continually consuming one message from self.producing_queue.
        Refers exceptions to handle_critical_error().
        """
        super().run()

        try:
            with RabbitMQ((self.producing_queue,)) as queue:
                queue.continually_consume_messages(
                    queue_name=self.producing_queue,
                    callback_function=self.handle_one_pass_with_retry
                )
        except Exception as e:
            self.route_error(e)
