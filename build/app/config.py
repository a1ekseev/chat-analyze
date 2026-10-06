import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)

RABBITMQ_URL = os.getenv('RABBITMQ_URL')
RABBITMQ_EXCHANGE_NAME = os.getenv('RABBITMQ_EXCHANGE_NAME')
RABBITMQ_QUEUE_NAME = os.getenv('RABBITMQ_QUEUE_NAME')
RABBITMQ_ROUTING_KEY = os.getenv('RABBITMQ_ROUTING_KEY', 'my_routing_key')

POSTGRESQL_URL = os.getenv('POSTGRESQL_URL')

API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv('API_HASH')
SESSION_NAME = os.getenv('SESSION_NAME')