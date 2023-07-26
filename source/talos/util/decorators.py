from typing import Tuple, Type, Callable, Any

from tenacity import retry, stop_after_attempt, stop_after_delay, wait_fixed, wait_exponential, retry_if_exception_type

from talos.logger import logger

import traceback

def log_and_reraise_exception(to_catch: Tuple[Type[BaseException]], should_raise: Exception, to_exclude: Tuple[Type[BaseException]] = None):
    """
    Decorator for logging and reraising exceptions. It catches specific exceptions and raises a custom one instead.

    Args:
        to_catch (Tuple[Type[BaseException]]): A tuple of exception classes to catch.
        should_raise (Exception): The type of exception to raise in place of the caught exceptions.
        to_exclude (Tuple[Type[BaseException]]): A tuple of exceptions to exclude reraising. Prevents higher-level decorators reraising with wrong type.

    Returns:
        Callable: The decorated function.
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except to_catch as ex:
                if to_exclude and isinstance(ex, to_exclude):
                    raise

                logger.error(f"An error occurred in {func.__name__}(): {str(ex)}", extra={
                    "json_fields": {
                        "traceback": traceback.format_exc()
                    }
                })
                raise should_raise() from ex
        return wrapper

    return decorator


def retry_fixed(retry_attempts: int, time_between_attempts: int, exception_types: Tuple[Type[BaseException]] = (Exception,)) -> retry:
    """
    Decorator which wraps the tenacity retry function, used for fixed duration between retries.

    Args:
        retry_attempts (int): The number of times to retry before reraising.
        time_between_attempts (int): The fixed-duration in seconds between retry attempts.
        exception_types (Tuple[Type[BaseException]]): The exceptions which should be retried.
    """
    def log_retry(retry_state):
        logger.notice(f"Retrying with fixed duration: {retry_state}.")

    return retry(
        stop=stop_after_attempt(retry_attempts),
        wait=wait_fixed(time_between_attempts),
        retry=retry_if_exception_type(exception_types),
        before_sleep=log_retry,
        reraise=True
    )


def retry_exponential(minimum_wait_time: int, maximum_wait_time: int, exception_types: Tuple[Type[BaseException]] = (Exception,)) -> retry:
    """
    Decorator which wraps the tenacity retry function, used for exponential duration between retries.
    Reraises after 3 minutes of retries. time_between_attempts = 2^(num_failures).

    Args:
        minimum_wait_time (int): The initial wait time between retries.
        maximum_wait_time (int): The maximum time to wait between retries, before fixed time is used.
        exception_types (Tuple[Type[BaseException]]): The exceptions which should be retried.
    """
    def log_retry(retry_state):
        logger.notice(f"Retrying with exponential duration: {retry_state}.")

    return retry(
        stop=stop_after_delay(3 * 60),
        wait=wait_exponential(min=minimum_wait_time, max=maximum_wait_time),
        retry=retry_if_exception_type(exception_types),
        before_sleep=log_retry,
        reraise=True
    )
