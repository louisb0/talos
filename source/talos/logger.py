import logging

import google.cloud.logging
from talos.config import Settings

Settings.validate()

logging.getLogger("pika").setLevel(logging.CRITICAL)
logging.getLogger("requests").setLevel(logging.CRITICAL)
logging.getLogger("urllib3").setLevel(logging.CRITICAL)
logging.getLogger('google.cloud').setLevel(logging.INFO)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

if not Settings.IS_DEV:
    client = google.cloud.logging.Client()
    cloud_handler = google.cloud.logging.handlers.CloudLoggingHandler(
        client,
        name=Settings.COMPONENT_NAME
    )
    logger.addHandler(cloud_handler)
else:
    stdout_handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '[%(levelname)s] [%(filename)s:%(lineno)d] [%(asctime)s] - %(message)s'
    )
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)


class LogLevel:
    DEFAULT = 0
    DEBUG = 100
    INFO = 200
    NOTICE = 300
    WARNING = 400
    ERROR = 500
    CRITICAL = 600
    ALERT = 700
    EMERGENCY = 800


logging.addLevelName(LogLevel.NOTICE, "NOTICE")
logging.Logger.notice = lambda inst, msg, * \
    args, **kwargs: inst.log(LogLevel.NOTICE, msg, *args, **kwargs)

logging.addLevelName(LogLevel.ALERT, "ALERT")
logging.Logger.alert = lambda inst, msg, * \
    args, **kwargs: inst.log(LogLevel.ALERT, msg, *args, **kwargs)

logging.addLevelName(LogLevel.ALERT, "EMERGENCY")
logging.Logger.emergency = lambda inst, msg, * \
    args, **kwargs: inst.log(LogLevel.EMERGENCY, msg, *args, **kwargs)
