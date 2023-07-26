import requests

from talos.exceptions.base import FatalException, NonFatalException
from talos.util.decorators import log_and_reraise_exception

NON_FATAL_EXCEPTIONS = (
    requests.exceptions.HTTPError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ProxyError,
    requests.exceptions.Timeout,
    requests.exceptions.ConnectTimeout,
    requests.exceptions.ReadTimeout,
    requests.exceptions.TooManyRedirects,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
    requests.exceptions.RetryError,
    requests.exceptions.SSLError,
    requests.exceptions.InvalidJSONError,
    requests.exceptions.JSONDecodeError,
)

FATAL_EXCEPTIONS = (
    requests.exceptions.RequestException,
    requests.exceptions.URLRequired,
    requests.exceptions.MissingSchema,
    requests.exceptions.InvalidSchema,
    requests.exceptions.InvalidURL,
    requests.exceptions.InvalidHeader,
    requests.exceptions.InvalidProxyURL,
    requests.exceptions.StreamConsumedError,
    requests.exceptions.UnrewindableBodyError,
    Exception
)


class APIFatalException(FatalException):
    pass


class APINonFatalException(NonFatalException):
    pass

class TokenNotFound(APINonFatalException):
    pass

class InvalidRequestType(APIFatalException):
    pass


log_reraise_non_fatal_exception = log_and_reraise_exception(
    to_catch=NON_FATAL_EXCEPTIONS,
    should_raise=APINonFatalException
)

log_reraise_fatal_exception = log_and_reraise_exception(
    to_catch=FATAL_EXCEPTIONS,
    should_raise=APIFatalException,
    to_exclude=(APINonFatalException,)
)
