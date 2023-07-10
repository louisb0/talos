from talos.exceptions.base import FatalException, NonFatalException
from talos.util.decorators import log_and_reraise_exception

NON_FATAL_EXCEPTIONS = (
    OSError
)

class FileUtilityFatalError(FatalException):
    pass

class FileUtilityNonFatalError(NonFatalException):
    pass

log_reraise_non_fatal_exception = log_and_reraise_exception(
    to_catch=NON_FATAL_EXCEPTIONS,
    should_raise=FileUtilityNonFatalError
)

log_reraise_fatal_exception = log_and_reraise_exception(
    to_catch=(Exception, ),
    should_raise=FileUtilityFatalError,
    to_exclude=(FileUtilityNonFatalError)
)