import os

from talos.exceptions.config import EnvironmentVariableNotSetException

class Settings:
    DB_HOSTNAME = os.getenv("DB_HOSTNAME")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_PORT = os.getenv("DB_PORT")
    
    RABBITMQ_HOSTNAME = os.getenv("RABBITMQ_HOSTNAME")
    RABBITMQ_MANAGEMENT_PORT = os.getenv("RABBITMQ_MANAGEMENT_PORT")
    RABBITMQ_SERVICE_PORT = os.getenv("RABBITMQ_SERVICE_PORT")
    RABBITMQ_EXCHANGE_NAME = os.getenv("RABBITMQ_EXCHANGE_NAME")

    STARTUP_SLEEP_TIME_SECS = int(os.getenv("STARTUP_SLEEP_TIME_SECS"))
    SECONDS_BETWEEN_RESCANS = int(os.getenv("SECONDS_BETWEEN_RESCANS"))

    SUBSCRIPTIONS_TABLE = os.getenv("SUBSCRIPTIONS_TABLE")
    FILESTORE_TABLE = os.getenv("FILESTORE_TABLE")
    RESCANS_TABLE = os.getenv("RESCANS_TABLE")
    RESCAN_RESPONSE_TABLE = os.getenv("RESCAN_RESPONSE_TABLE")
    SCRAPED_POST_TABLE = os.getenv("SCRAPED_POST_TABLE")

    RESCAN_QUEUE = os.getenv("RESCAN_QUEUE")

    MAX_POSTS_PER_REQUEST = int(os.getenv("MAX_POSTS_PER_REQUEST"))
    USER_AGENT = os.getenv("USER_AGENT")
    RESPONSE_STORAGE_PATH = os.getenv("RESPONSE_STORAGE_PATH")

    @classmethod
    def validate(cls):
        for attr, value in cls.__dict__.items():
            if attr.isupper() and value is None:
                raise EnvironmentVariableNotSetException(attr)