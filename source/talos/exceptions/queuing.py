import pika.exceptions
import socket

from talos.exceptions.base import FatalException, NonFatalException, log_and_reraise_exception
from talos.logger import logger

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


class BadMessageException(RabbitMQFatalException):
    pass


class NotInitialisedException(RabbitMQFatalException):
    pass


class UnknownQueueException(RabbitMQFatalException):
    pass


log_reraise_non_fatal_exception = log_and_reraise_exception(
    to_catch=NON_FATAL_EXCEPTIONS,
    should_raise=RabbitMQNonFatalException
)

log_reraise_bad_message_exception = log_and_reraise_exception(
    to_catch=BAD_MESSAGE_EXCEPTIONS,
    should_raise=BadMessageException,
    to_exclude=(RabbitMQNonFatalException,)
)

log_reraise_fatal_exception = log_and_reraise_exception(
    to_catch=(Exception,),
    should_raise=RabbitMQFatalException,
    to_exclude=(RabbitMQNonFatalException, BadMessageException)
)
