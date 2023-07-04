import logging

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] [%(asctime)s] - %(message)s')
logging.getLogger("pika").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)