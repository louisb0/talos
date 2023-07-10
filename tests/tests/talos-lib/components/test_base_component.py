import unittest
from unittest.mock import patch
import logging

from talos.components import BaseComponent
from talos.exceptions.base import NonFatalException


class ConcreteBaseComponent(BaseComponent):
    """
    Coverage:
        * __init__ sets the fields correctly in BaseComponent
        * run() makes a log message and sleeps
        * a NonFatalException followed by success results in two calls,
          i.e. we retry a NonFatalException in _handle_one_pass
        * if `retry_attempts` NonFatalExceptions are raised, then we reraise
          the exception, and that `retry_attempts` calls were made
    """

    def handle_critical_error(self):
        pass

    def _handle_one_pass(self):
        pass


class TestBaseComponent(unittest.TestCase):
    def setUp(self):
        self.test_component = ConcreteBaseComponent(
            retry_attempts=5,
            time_between_attempts=0
        )

        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

    def test_init_sets_fields(self):
        self.assertEqual(self.test_component.retry_attempts, 5)
        self.assertEqual(self.test_component.time_between_attempts, 0)

    @patch('time.sleep')
    @patch.object(logging.getLogger("talos.logger"), 'info')
    def test_run_calls_logger_and_sleep(self, mock_info, mock_sleep):
        self.test_component.run()

        mock_info.assert_called_once()
        mock_sleep.assert_called_once()

    @patch.object(ConcreteBaseComponent, '_handle_one_pass', side_effect=[NonFatalException, None])
    def test_retry_on_non_fatal(self, mock_handle_one_pass):
        self.test_component.handle_one_pass_with_retry()

        self.assertEqual(mock_handle_one_pass.call_count, 2)

    def test_raises_after_exceeded_retries(self):
        side_effects = [NonFatalException] * self.test_component.retry_attempts
        with patch.object(ConcreteBaseComponent, '_handle_one_pass', side_effect=side_effects) as mock_handle_one_pass:
            with self.assertRaises(NonFatalException):
                self.test_component.handle_one_pass_with_retry()

            self.assertEqual(mock_handle_one_pass.call_count,
                             self.test_component.retry_attempts)