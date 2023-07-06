from typing import Tuple, Type, Callable, Any
from talos.logger import logger


class FatalException(Exception):
    pass


class NonFatalException(Exception):
    pass


def log_and_reraise_exception(to_catch: Tuple[Type[BaseException]], should_raise: Exception, to_exclude: Tuple[Type[BaseException]] = None):
    """
    Decorator for logging and reraising exceptions. It catches specific exceptions and raises a custom one instead.

    Args:
        to_catch (Tuple[Type[BaseException]]): A tuple of exception classes to catch.
        should_raise (Exception): The type of exception to raise in place of the caught exceptions.
        to_exclude (Tuple[Type[BaseException]]): A tuple of exceptions to exclude reraising. Prevents higher-decorators reraising with wrong type.

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

                logger.exception(
                    f"An error occurred in {func.__name__}()."
                )
                raise should_raise() from ex
        return wrapper

    return decorator