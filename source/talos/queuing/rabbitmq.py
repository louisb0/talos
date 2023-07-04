from typing import Tuple, Dict, List, Callable

import pika
from pika.adapters.blocking_connection import BlockingChannel

from talos.config.settings import Settings
from talos.exceptions.queuing import RabbitMQNotInitialisedException
from talos.logger import logger


class RabbitMQ:
    CONFIG = {
        "host": Settings.RABBITMQ_HOSTNAME,
        "port": Settings.RABBITMQ_SERVICE_PORT,
    }

    def __init__(self, queues: Tuple[str]):
        self.connection: pika.BlockingConnection = None
        self.channel: BlockingChannel = None

        self.queues = queues

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def connect(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(**self.CONFIG)
        )
        self.channel = self.connection.channel()

        self._declare_exchange()
        for queue_name in self.queues:
            self._declare_queue(queue_name)

    def disconnect(self):
        if self.connection and self.connection.is_open:
            self.connection.close()

    def publish_message(self, queue_name: str, message: str):
        self._validate_connection()

        self.channel.basic_publish(
            exchange='talos-exchange',
            routing_key=queue_name,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            )
        )

    def publish_messages(self, queue_name: str, messages: List[Dict]):
        self._validate_connection()

        for message in messages:
            self.publish_message(queue_name, message)

    def continually_consume_messages(self, queue_name: str, callback_function: Callable):
        self._validate_connection()

        def callback(ch, method, properties, body):
            callback_function(body) # ERROR HANDLING!!!!!!!!!!!!!!!!!!!!!!!!!!!!! NACK!!!!!!!!!!!!!
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False
        )
        self.channel.start_consuming()

    def consume_one_message(self, queue_name: str):
        self._validate_connection()

        method_frame, header_frame, body = self.channel.basic_get(
            queue=queue_name
        )

        if method_frame:
            self.channel.basic_ack(method_frame.delivery_tag)
            return body

    def consume_n_messages(self, queue_name: str, n: int):
        self._validate_connection()
        messages = []

        for i in range(n):
            messages.append(self.consume_one_message(queue_name))

        return messages

    def _declare_exchange(self):
        self._validate_connection()

        self.channel.exchange_declare(
            exchange=Settings.RABBITMQ_EXCHANGE_NAME,
            exchange_type='direct',
            durable=True,
        )

    def _declare_queue(self, queue_name):
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(
            exchange=Settings.RABBITMQ_EXCHANGE_NAME,
            queue=queue_name,
            routing_key=queue_name
        )

    def _validate_connection(self):
        if not self.channel or not self.connection:
            raise RabbitMQNotInitialisedException()


"""
queue_names = ("one", "two")
single_message = json.dumps({"key": "value"})
batch_messages = [json.dumps({"key": f"value{i}"}) for i in range(5)]

# Initialize RabbitMQ with queues
with RabbitMQ(queues=queue_names) as q:
    logger.info("publsihing to one")
    q.publish_message("one", single_message)
    logger.info("publsihing to two")
    q.publish_messages("two", batch_messages)

    logger.info(f"first {q.consume_one_message('one')}")
    logger.info(f"second {q.consume_n_messages('two', 3)}")

    def callback(message):
        logger.info(f"continual {message}")
    q.continually_consume_messages("two", callback)
"""
