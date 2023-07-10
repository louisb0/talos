import unittest
from unittest.mock import patch
import logging

from talos.components import ConsumerComponent


class ConcreteConsumerComponent(ConsumerComponent):
    def handle_critical_error(self):
        pass

    def _handle_one_pass(self):
        pass


class TestConsumerComponent(unittest.TestCase):
    """
    Coverage:
        * __init__ sets the fields correctly in ConsumerComponent
        * run() calls super().run()  and calls RabbitMQ.continually_consume_messages()
          with the correct parameters
        * RabbitMQ(...) context manager created with correct parameter(s)
        * an exception raised from handle_one_pass_with_retry() causes route_error()
          to be called with the exception as a parameter
        * route_error() being called results in handle_critical_error() being called
    """

    def setUp(self):
        self.test_component = ConcreteConsumerComponent(
            retry_attempts=5,
            time_between_attempts=0,
            producing_queue='test_queue'
        )

        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

        # stops an actual connection attempt
        self.mock_rabbitmq_connect = patch(
            "talos.queuing.rabbitmq.RabbitMQ.connect", return_value=None
        ).start()

        # stops 30s sleeps for the test case
        self.mock_sleep = patch(
            "talos.components.base_component.time.sleep", return_value=None
        ).start()

    def tearDown(self):
        self.mock_rabbitmq_connect.stop()
        self.mock_sleep.stop()

    def test_init(self):
        self.assertEqual(self.test_component.retry_attempts, 5)
        self.assertEqual(self.test_component.time_between_attempts, 0)
        self.assertEqual(self.test_component.producing_queue, 'test_queue')

    @patch("talos.queuing.rabbitmq.RabbitMQ.continually_consume_messages", return_value=None)
    @patch.object(logging.getLogger("talos.logger"), 'info')
    def test_run(self, mock_logger, mock_consume):
        self.test_component.run()

        # from super().__init__
        mock_logger.assert_called_once()
        self.mock_sleep.assert_called_once()

        # primary purpose of class, consumption
        mock_consume.assert_called_once_with(
            queue_name=self.test_component.producing_queue,
            callback_function=self.test_component.handle_one_pass_with_retry
        )

    @patch("talos.queuing.rabbitmq.RabbitMQ.__init__")
    def test_rabbitmq_params(self, mock_rabbitmq):
        self.test_component.run()

        mock_rabbitmq.assert_called_once_with(
            (self.test_component.producing_queue,)
        )

    @patch.object(ConcreteConsumerComponent, "route_error")
    @patch.object(ConcreteConsumerComponent, "handle_one_pass_with_retry", side_effect=Exception)
    def test_error_propagation(self, mock_handle_pass, mock_route_error):
        self.test_component.run()

        mock_route_error.assert_called_once()
        self.assertIsInstance(
            mock_route_error.call_args.args[0], Exception
        )

    @patch.object(ConcreteConsumerComponent, "handle_critical_error")
    def test_error_handling(self, mock_handle_error):
        self.test_component.route_error(Exception)

        mock_handle_error.assert_called_once()
