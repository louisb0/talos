import unittest
from unittest.mock import patch, Mock

import logging
from talos.queuing import RabbitMQ
from talos.exceptions.queuing import *
import pika


class TestRabbitMQ(unittest.TestCase):
    """
    This class is pretty messy but gets the job done as is the way it goes
    The E2E test definitely shouldn't be in this class either

    Coverage:
        * tests object __init__ sets correct fields, accept string/tuple queues
        * test connect() creates a BlockingConnection with correct config,
          declares the respective queues and exchanges
        * test that disconnect() closes the channel and connection object
        * tests __enter__ and __exit__ function accordingly, connecting and disconnecting
        * tests _validate_connection() and _validate_queue() functionality
        * tests that required funcs call _validate_connection()
    (E2E)
        * use publish_message
        * use publish_messages
        * consume one message
        * consume n messages
        * continually consume remaining messages, raise exception to exit
    """

    def setUp(self):
        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

    @patch("talos.queuing.rabbitmq.RabbitMQ.connect")
    def test_init_sets_fields(self, mock_connect):
        # not intended usage

        with self.subTest(msg="tuple_init"):
            q = RabbitMQ(("queue1", "queue2"))
            self.assertEqual(q.connection, None)
            self.assertEqual(q.channel, None)
            self.assertEqual(q.queues, ("queue1", "queue2"))

        with self.subTest(msg="string_init"):
            q = RabbitMQ("queue1")
            self.assertEqual(q.connection, None)
            self.assertEqual(q.channel, None)
            self.assertEqual(q.queues, ("queue1",))

    @patch("talos.queuing.rabbitmq.RabbitMQ._declare_queue")
    @patch("talos.queuing.rabbitmq.RabbitMQ._declare_exchange")
    @patch("pika.BlockingConnection")
    def test_connect(self, mock_connection, mock_declare_exchange, mock_declare_queue):
        mock_connection.return_value.channel.return_value = Mock()

        # not intended usage
        q = RabbitMQ(("queue",))
        q.connect()

        self.assertIsNotNone(q.connection)
        self.assertIsNotNone(q.channel)

        mock_connection.assert_called_once_with(
            pika.ConnectionParameters(**RabbitMQ.CONFIG)
        )
        mock_declare_exchange.assert_called_once()
        mock_declare_queue.assert_called_once_with("queue")

    @patch("talos.queuing.rabbitmq.RabbitMQ._declare_queue")
    @patch("talos.queuing.rabbitmq.RabbitMQ._declare_exchange")
    @patch("pika.BlockingConnection")
    def test_disconnect(self, mock_connection, mock_declare_exchange, mock_declare_queue):
        mock_connection.return_value.channel.return_value = Mock()

        q = RabbitMQ("queue",)
        q.connect()
        mock_connection.is_open = True
        mock_connection.channel.is_open = True
        q.disconnect()

        q.connection.close.assert_called_once()
        q.channel.close.assert_called_once()

    @patch("talos.queuing.rabbitmq.RabbitMQ.connect")
    @patch("talos.queuing.rabbitmq.RabbitMQ.disconnect")
    def test_context_manager(self, mock_connect, mock_disconnect):
        mock_connect.return_value.channel.return_value = Mock()

        with RabbitMQ(("queue", )) as q:
            pass

        mock_connect.assert_called_once()
        mock_disconnect.assert_called_once()

    def test_validate_connection(self):
        with self.assertRaises(NotInitialisedException):
            q = RabbitMQ(("queue",))
            q._validate_connection()

    def test_validate_queue(self):
        with self.assertRaises(UnknownQueueException):
            q = RabbitMQ(("queue",))
            q._validate_queue("not_a_queue")

    @patch("pika.BlockingConnection")
    def test_validation(self, mock_connection):
        mock_connection.return_value.channel.return_value = Mock()
        # we use tuple unpacking shorthand (consume_one_message) so if it's none an exception is raised
        mock_connection.return_value.channel.return_value.basic_get.return_value = (
            Mock(),)*3

        # messy and repetitive. nice
        with self.subTest(method="publish_message"):
            with patch("talos.queuing.rabbitmq.RabbitMQ._validate_connection") as vcon:
                with patch("talos.queuing.rabbitmq.RabbitMQ._validate_queue") as vq:
                    with RabbitMQ("queue1") as q:
                        q.publish_message("queue1", "{}")

                    vcon.assert_called()  # called in _declare_exchange, _once() bad
                    vq.assert_called_with("queue1")

        with self.subTest(method="continually_consume_messages"):
            with patch("talos.queuing.rabbitmq.RabbitMQ._validate_connection") as vcon:
                with patch("talos.queuing.rabbitmq.RabbitMQ._validate_queue") as vq:
                    with RabbitMQ("queue1") as q:
                        q.continually_consume_messages("queue1", lambda _: _)

                    vcon.assert_called()  # called in _declare_exchange, _once() bad
                    vq.assert_called_once_with("queue1")

        with self.subTest(method="continually_consume_messages"):
            with patch("talos.queuing.rabbitmq.RabbitMQ._validate_connection") as vcon:
                with patch("talos.queuing.rabbitmq.RabbitMQ._validate_queue") as vq:
                    with RabbitMQ("queue1") as q:
                        q.consume_one_message("queue1")

                    vcon.assert_called()  # called in _declare_exchange, _once() bad
                    vq.assert_called_once_with("queue1")

    def test_e2e(self):
        RabbitMQ.CONFIG = {
            "host": "rabbit",
            "port": "5672"
        }

        with RabbitMQ(("queue1", "queue2")) as q:
            q.publish_message(
                queue_name="queue1",
                message="m1"
            )

            messages = ["m2", "m3", "m4"]
            q.publish_messages(
                queue_name="queue2",
                messages=messages
            )

            m_one = q.consume_one_message("queue1")
            self.assertEqual(m_one.decode(), "m1")

            m_two = q.consume_n_messages(
                queue_name="queue2",
                n=1
            )
            self.assertEqual(m_two[0].decode(), "m2")

            def callback(message):
                self.assertIn(message.decode(), ["m3", "m4"])

                if message.decode() == "m4":
                    raise InterruptedError()

            with self.assertRaises(RabbitMQFatalException):
                q.continually_consume_messages(
                    queue_name="queue2",
                    callback_function=callback
                )

            q.channel.queue_delete("queue1")
            q.channel.queue_delete("queue2")
