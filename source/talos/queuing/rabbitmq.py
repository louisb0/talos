from typing import Tuple, Dict, List, Callable

import pika
from pika.adapters.blocking_connection import BlockingChannel

from talos.config.settings import Settings
from talos.exceptions.queuing import *
from talos.logger import logger


class RabbitMQ:
    CONFIG = {
        "host": Settings.RABBITMQ_HOSTNAME,
        "port": Settings.RABBITMQ_SERVICE_PORT,
    }

    def __init__(self, queues: Tuple[str]):
        """
        Initialize the RabbitMQ object, should be used with a context manager.
        
        Args:
            queues (Tuple[str]): A tuple containing the names of the queues.
        """
        self.connection: pika.BlockingConnection = None
        self.channel: BlockingChannel = None
        self.queues = queues

    def __enter__(self):
        """
        Context manager enter function. Connects to the RabbitMQ server.

        Returns:
            self

        Raises:
            RabbitMQNonFatalException: For non-fatal internal AMPQ exceptions.
            RabbitMQFatalException: For fatal internal AMPQ exceptions.
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit function. Disconnects from the RabbitMQ server.

        Args:
            exc_type, exc_val, exc_tb: Exception type, value and traceback respectively.

        Raises:
            RabbitMQFatalException: If any exception occurs during disconnection.
        """
        self.disconnect()

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def connect(self) -> None:
        """
        Connects to the RabbitMQ server and establishes a channel.

        Raises:
            RabbitMQNonFatalException: For non-fatal internal AMPQ exceptions.
            RabbitMQFatalException: For fatal internal AMPQ exceptions.
        """
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(**self.CONFIG)
        )
        self.channel = self.connection.channel()

        self._declare_exchange()
        for queue_name in self.queues:
            self._declare_queue(queue_name)

    @log_reraise_fatal_exception
    def disconnect(self) -> None:
        """
        Disconnects from the RabbitMQ server.

        Raises:
            RabbitMQFatalException: If any exception occurs during disconnection.
        """
        if self.connection and self.connection.is_open:
            self.connection.close()

    @log_reraise_fatal_exception
    @log_reraise_bad_message_exception
    @log_reraise_non_fatal_exception
    def publish_message(self, queue_name: str, message: str) -> None:
        """
        Publishes a message to a specific queue.

        Args:
            queue_name (str): The name of the queue.
            message (str): The message to be published.

        Raises:
            BadMessageException: If any exceptions occur during message publishing.
            RabbitMQNonFatalException: For non-fatal internal AMPQ exceptions.
            RabbitMQFatalException: For fatal internal AMPQ exceptions.
        """
        self._validate_connection()
        self._validate_queue(queue_name)

        self.channel.basic_publish(
            exchange=Settings.RABBITMQ_EXCHANGE_NAME,
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )

    # should propogate up, no decorator
    def publish_messages(self, queue_name: str, messages: List[Dict]) -> None:
        """
        Publishes a list of messages to a specific queue. 

        Args:
            queue_name (str): The name of the queue.
            messages (List[Dict]): The list of messages to be published.

        Note: This function is not decorated, exceptions should propagate up.
        """
        for message in messages:
            self.publish_message(queue_name, message)

    @log_reraise_fatal_exception
    @log_reraise_bad_message_exception
    @log_reraise_non_fatal_exception
    def continually_consume_messages(self, queue_name: str, callback_function: Callable) -> None:
        """
        Consumes messages from a specific queue indefinitely. Each message is passed to a callback function.
        Messages are (n)ack'd within the function.

        Args:
            queue_name (str): The name of the queue to consume from.
            callback_function (Callable): The function to be called for each message.

        Raises:
            BadMessageException: If any message based exceptions occur during message consumption.
            RabbitMQNonFatalException: For non-fatal internal AMPQ exceptions.
            RabbitMQFatalException: For fatal internal AMPQ exceptions.
        """
        self._validate_connection()
        self._validate_queue(queue_name)

        def callback(ch, method, properties, body):
            try:
                callback_function(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                raise e

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False
        )
        self.channel.start_consuming()

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def consume_one_message(self, queue_name: str) -> str:
        """
        Consumes one message from a specific queue.

        Args:
            queue_name (str): The name of the queue to consume from.

        Returns:
            str: The consumed message.

        Raises:
            RabbitMQNonFatalException: For non-fatal internal AMPQ exceptions.
            RabbitMQFatalException: For fatal internal AMPQ exceptions.
        """
        self._validate_connection()
        self._validate_queue(queue_name)

        method_frame, header_frame, body = self.channel.basic_get(
            queue=queue_name
        )

        if method_frame:
            self.channel.basic_ack(method_frame.delivery_tag)
            return body

    def consume_n_messages(self, queue_name: str, n: int) -> List[str]:
        """
        Consumes n messages from a specific queue.

        Args:
            queue_name (str): The name of the queue to consume from.
            n (int): The number of messages to consume.

        Returns:
            List[str]: A list of consumed messages.

        Note: This function is not decorated, exceptions should propagate up.
        """
        self._validate_connection()
        messages = []

        for i in range(n):
            messages.append(self.consume_one_message(queue_name))

        return messages

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def _declare_exchange(self) -> None:
        """
        Declares an exchange in the RabbitMQ server.

        Raises:
            RabbitMQNonFatalException: For non-fatal internal AMPQ exceptions.
            RabbitMQFatalException: For fatal internal AMPQ exceptions.
        """
        self._validate_connection()

        self.channel.exchange_declare(
            exchange=Settings.RABBITMQ_EXCHANGE_NAME,
            exchange_type='direct',
            durable=True,
        )

    @log_reraise_fatal_exception
    @log_reraise_non_fatal_exception
    def _declare_queue(self, queue_name: str) -> None:
        """
        Declares a queue in the RabbitMQ server.

        Args:
            queue_name (str): The name of the queue to declare.

        Raises:
            RabbitMQNonFatalException: For non-fatal internal AMPQ exceptions.
            RabbitMQFatalException: For fatal internal AMPQ exceptions.
        """
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(
            exchange=Settings.RABBITMQ_EXCHANGE_NAME,
            queue=queue_name,
            routing_key=queue_name
        )

    def _validate_connection(self):
        """
        Checks if the connection and channel are established, raises an exception if not.

        Raises:
            NotInitialisedException: If the connection or channel is not established.
        """
        if not self.channel or not self.connection:
            raise NotInitialisedException()

    def _validate_queue(self, queue_name):
        """
        Checks if a queue exists, raises an exception if not.

        Args:
            queue_name (str): The name of the queue to check.

        Raises:
            UnknownQueueException: If a queue does not exist.
        """
        if queue_name not in self.queues:
            raise UnknownQueueException()