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
    RESCAN_PRODUCER_SLEEP_TIME_SECS = int(os.getenv("RESCAN_PRODUCER_SLEEP_TIME_SECS"))
    TIME_BETWEEN_POST_RESCANS = int(os.getenv("TIME_BETWEEN_POST_RESCANS"))

    SUBSCRIPTIONS_TABLE = os.getenv("SUBSCRIPTIONS_TABLE")
    SUBREDDIT_RESCAN_TABLE = os.getenv("SUBREDDIT_RESCAN_TABLE")
    POST_RESCAN_TABLE = os.getenv("POST_RESCAN_TABLE")
    INITIAL_POSTS_TABLE = os.getenv("INITIAL_POSTS_TABLE")
    UPDATED_POSTS_TABLE = os.getenv("UPDATED_POSTS_TABLE")
    SCRAPED_COMMENTS_TABLE = os.getenv("SCRAPED_COMMENTS_TABLE")

    SUBREDDIT_RESCAN_QUEUE = os.getenv("SUBREDDIT_RESCAN_QUEUE")
    POST_RESCAN_QUEUE = os.getenv("POST_RESCAN_QUEUE")
    
    MAX_POSTS_PER_REQUEST = int(os.getenv("MAX_POSTS_PER_REQUEST"))
    REQUESTS_PER_TOKEN = int(os.getenv("REQUESTS_PER_TOKEN"))
    USER_AGENT = os.getenv("USER_AGENT")

    COMPONENT_NAME = os.getenv("TALOS_COMPONENT_NAME")
    IS_DEV = os.getenv("IS_DEV").lower() in ("1", "true", "t")

    @classmethod
    def validate(cls):
        for attr, value in cls.__dict__.items():
            if attr.isupper() and value is None:
                raise EnvironmentVariableNotSetException(attr)