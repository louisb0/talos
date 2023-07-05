from typing import Tuple, Type, Callable, Any
from talos.logger import logger


class FatalException(Exception):
    pass


class NonFatalException(Exception):
    pass


def log_and_reraise_exception(to_catch: Tuple[Type[BaseException]], should_raise: Exception):
    """
    Decorator for logging and reraising exceptions. It catches specific exceptions and raises a new one instead.

    Args:
        to_catch (Tuple[Type[BaseException]]): A tuple of exception classes to catch.
        should_raise (Exception): The type of exception to raise in place of the caught exceptions.

    Returns:
        Callable: The decorated function.

    Example Usage:
        @handle_exceptions((TypeError, ValueError), CustomException)
        def might_fail(x):
            if not isinstance(x, int):
                raise TypeError("Only integers are allowed")
            if x < 0:
                raise ValueError("Only non-negative integers are allowed")
            return x

    Note:
        The function decorated with @handle_exceptions should handle `should_raise` exception.
    """

    def decorator(func: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except to_catch as ex:
                logger.exception(
                    f"An error occurred in {func.__name__}()."
                )
                raise should_raise() from ex
        return wrapper

    return decorator
