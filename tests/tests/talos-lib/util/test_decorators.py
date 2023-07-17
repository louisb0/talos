import unittest
from unittest.mock import patch, Mock
import logging

from talos.util.decorators import *

class CustomException(Exception):
    pass

class TestDecorators(unittest.TestCase):
    """
    The retry tests definitely could be improved. A lot of it is testing functionality of
    tenacity which we could generally assume to be correct.

    Coverage:
        * test_log_and_reraise
            * if to_catch is raised and is not in to_exclude, log reraise as custom
            * if to_catch is raised and in to_exclude, no log reraise as original
        * test_retry_fixed
            * if a targetted exception is raised by decorated function, retried X times
              and reraised
            * if a not targetted exception is raised by decorated function, it is not retried
              and is reraised
        * test_retry_exponential
            * same as above, but check for wait_exp callks
    """
    def setUp(self):
        logging.getLogger("talos.logger").setLevel(logging.CRITICAL)

    def test_log_and_reraise(self):
        test_decorator = log_and_reraise_exception(
            to_catch=(KeyError,),
            should_raise=(CustomException),
            to_exclude=(ValueError,)
        )

        with self.subTest(msg="test_catch_reraise_log"):
            with patch.object(logging.getLogger("talos.logger"), 'exception') as mock_log:
                @test_decorator
                def test_func():
                    raise KeyError()
                
                with self.assertRaises(CustomException):
                    test_func()

                mock_log.assert_called_once()

        with self.subTest(msg="test_exclude_no_reraise_log"):
            with patch.object(logging.getLogger("talos.logger"), 'exception') as mock_log:
                @test_decorator
                def test_func():
                    raise ValueError()
                
                with self.assertRaises(ValueError):
                    test_func()

                mock_log.assert_not_called()
    

    def test_retry_fixed(self):
        test_retry_fixed_decorator = retry_fixed(
            retry_attempts=2,
            time_between_attempts=0,
            exception_types=(ValueError,)
        )

        with self.subTest("should_retry_fixed"):
            with patch("tenacity.wait.wait_fixed.__call__") as mock_wait:
                test_func = Mock(side_effect=ValueError())
                retry_func = test_retry_fixed_decorator(test_func)
                
                with self.assertRaises(ValueError):
                    retry_func()
                self.assertEqual(test_func.call_count, 2)
                mock_wait.assert_called()
        
        with self.subTest("should_not_retry_fixed"):
            with patch("tenacity.wait.wait_fixed.__call__") as mock_wait:
                test_func = Mock(side_effect=KeyError())
                retry_func = test_retry_fixed_decorator(test_func)

                with self.assertRaises(KeyError):
                    retry_func()
                self.assertEqual(test_func.call_count, 1)
                mock_wait.assert_not_called()

    def test_retry_exponential(self):
        test_retry_exponential_decorator = retry_exponential(
            minimum_wait_time=1,
            maximum_wait_time=2,
            exception_types=(ValueError,)
        )

        with self.subTest("should_retry_fixed"):
            with patch("tenacity.wait.wait_exponential.__call__") as mock_exp_wait:
                test_func = Mock(side_effect=[ValueError(), None])
                retry_func = test_retry_exponential_decorator(test_func)
                
                retry_func()
                
                mock_exp_wait.assert_called()
        
        with self.subTest("should_not_retry_fixed"):
            with patch("tenacity.wait.wait_exponential.__call__") as mock_exp_wait:
                test_func = Mock(side_effect=KeyError())
                retry_func = test_retry_exponential_decorator(test_func)

                with self.assertRaises(KeyError):
                    retry_func()

                self.assertEqual(test_func.call_count, 1)
                mock_exp_wait.assert_not_called()