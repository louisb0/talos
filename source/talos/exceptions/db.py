import psycopg2

from talos.exceptions.base import FatalException, NonFatalException, log_and_reraise_exception

NON_FATAL_EXCEPTIONS = (
    psycopg2.OperationalError,
    psycopg2.InternalError
)


class DatabaseFatalException(FatalException):
    pass


class DatabaseNonFatalException(NonFatalException):
    pass


class DatabaseNotInitialisedException(DatabaseFatalException):
    pass


log_reraise_non_fatal_exception = log_and_reraise_exception(
    to_catch=NON_FATAL_EXCEPTIONS,
    should_raise=DatabaseNonFatalException
)


log_reraise_fatal_exception = log_and_reraise_exception(
    to_catch=(Exception, ),
    should_raise=DatabaseFatalException,
    to_exclude=(DatabaseNonFatalException,)
)
