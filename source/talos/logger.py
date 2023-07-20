import logging

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] [%(filename)s:%(lineno)d] [%(asctime)s] - %(message)s')

logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)