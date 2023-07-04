from talos.exceptions.base import FatalException, NonFatalException

class DatabaseFatalException(FatalException):
    pass

class DatabaseNonFatalException(NonFatalException):
    pass

class DatabaseNotInitialisedException(DatabaseFatalException):
    pass