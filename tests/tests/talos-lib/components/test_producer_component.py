import unittest
from unittest.mock import patch
import logging

from talos.components import ProducerComponent


class ConcreteProducerComponent(ProducerComponent):
    def handle_critical_error(self):
        pass

    def _handle_one_pass(self):
        pass


class TestProducerComponent(unittest.TestCase):
    """
    Coverage:
        * __init__ sets the fields correctly in ProducerComponent
        * run() calls super().run() and calls handle_one_pass_with_retry
        * an exception raised from handle_one_pass() causes route_error() to be
          called with the exception as a parameter
        * route_error() being called results in handle_fatal_error() being called

    The last two are duplicates of TestConsumerComponent, or vice versa, this is because
    the logic for error propogation lays in BaseComponent, but testing it requires
    a concrete implementation of one subclass ({Producer/Consumer}Component).
    """

    def setUp(self):
        self.test_component = ConcreteProducerComponent(
            retry_attempts=5,
            time_between_attempts=0,
        )

        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

        # stops 30s sleeps for the test case
        self.mock_sleep = patch(
            "talos.components.base_component.time.sleep", return_value=None
        ).start()

    def tearDown(self):
        self.mock_sleep.stop()

    def test_init(self):
        self.assertEqual(self.test_component.retry_attempts, 5)
        self.assertEqual(self.test_component.time_between_attempts, 0)

    @patch.object(ConcreteProducerComponent, "route_error", side_effect=Exception("break"))
    @patch.object(ConcreteProducerComponent, "handle_one_pass_with_retry", side_effect=Exception("break"))
    @patch.object(logging.getLogger("talos.logger"), 'info')
    def test_run(self, mock_logger, mock_one_pass, mock_route_error):
        try:
            self.test_component.run()
        except Exception as e:
            self.assertEqual(str(e), "break")

        # from super().__init__
        mock_logger.assert_called_once()
        self.mock_sleep.assert_called_once()

        # primary purpose of class, consumption
        mock_one_pass.assert_called_once()

    @patch.object(ConcreteProducerComponent, "route_error", side_effect=Exception("break"))
    @patch.object(ConcreteProducerComponent, "handle_one_pass_with_retry", side_effect=Exception, return_value=None)
    def test_error_propagation(self, mock_handle_pass, mock_route_error):
        try:
            self.test_component.run()
        except Exception as e:
            self.assertEqual(str(e), "break")

        mock_route_error.assert_called_once()
        self.assertIsInstance(
            mock_route_error.call_args.args[0], Exception
        )

    @patch.object(ConcreteProducerComponent, "handle_critical_error")
    def test_error_handling(self, mock_handle_error):
        self.test_component.route_error(Exception)

        mock_handle_error.assert_called_once()
