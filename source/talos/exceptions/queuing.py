import pika.exceptions
import socket

from talos.exceptions.base import FatalException, NonFatalException
from talos.util.decorators import log_and_reraise_exception

NON_FATAL_EXCEPTIONS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.AMQPHeartbeatTimeout,
    pika.exceptions.ConnectionBlockedTimeout,
    pika.exceptions.ConnectionOpenAborted,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.ChannelClosedByBroker,
    pika.exceptions.StreamLostError,
    socket.gaierror,
    socket.herror
)

BAD_MESSAGE_EXCEPTIONS = (
    pika.exceptions.BodyTooLongError,
    pika.exceptions.InvalidFieldTypeException,
    pika.exceptions.InvalidFrameError,
    pika.exceptions.NackError,
    pika.exceptions.UnroutableError,
    pika.exceptions.DuplicateConsumerTag,
    pika.exceptions.DuplicateGetOkCallback,
)


class RabbitMQFatalException(FatalException):
    pass


class RabbitMQNonFatalException(NonFatalException):
    pass


class NotInitialisedException(RabbitMQFatalException):
    pass


class UnknownQueueException(RabbitMQFatalException):
    pass


log_reraise_non_fatal_exception = log_and_reraise_exception(
    to_catch=NON_FATAL_EXCEPTIONS,
    should_raise=RabbitMQNonFatalException
)


log_reraise_fatal_exception = log_and_reraise_exception(
    to_catch=(Exception,),
    should_raise=RabbitMQFatalException,
    to_exclude=(RabbitMQNonFatalException)
)
