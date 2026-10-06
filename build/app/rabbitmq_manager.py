import json
import aio_pika
import logging

import config
from db_manager import save_message_to_db
from telethon import TelegramClient

logger = logging.getLogger(__name__)


class RabbitMQManager:
    def __init__(self, db_pool, telegram_client: TelegramClient):
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None
        self.db_pool = db_pool
        self.telegram_client = telegram_client

    async def connect(self, rabbitmq_url, exchange_name, queue_name, routing_key):
        try:
            self.connection = await aio_pika.connect_robust(rabbitmq_url)
            self.channel = await self.connection.channel()
            self.exchange = await self.channel.declare_exchange(
                exchange_name, aio_pika.ExchangeType.DIRECT
            )
            self.queue = await self.channel.declare_queue(queue_name, durable=True)
            await self.queue.bind(self.exchange, routing_key=routing_key)
            logger.info("RabbitMQ connection established")
        except Exception as e:
            logger.error(f"Error establishing connection to RabbitMQ: {e}")

    async def send_message(self, message):
        try:
            message_body = json.dumps(message).encode("utf-8")

            await self.exchange.publish(aio_pika.Message(body=message_body), routing_key=config.RABBITMQ_ROUTING_KEY)

            logger.info(f"Message sent to queue: {message}")
        except Exception as e:
            logger.error(f"Error sending message to RabbitMQ: {e}")

    async def consume_messages(self):
        try:
            async with self.queue.iterator() as queue:
                async for message in queue:
                    try:
                        # Обработка сообщения
                        message_data = json.loads(message.body.decode("utf-8"))
                        logger.info(f"Received message: {message_data}")

                        # Сохранение в БД
                        if await save_message_to_db(self.db_pool, message_data):
                            chat_id = message_data.get("chat_id")
                            message_id = message_data.get("message_id")

                            if chat_id and message_id:
                                await self.telegram_client.send_read_acknowledge(chat_id, max_id=message_id)
                                logger.info(f"Marked message as read: chat_id={chat_id}, message_id={message_id}")

                            await message.ack()
                        else:
                            logger.error("Error saving message to DB, not acknowledging.")
                            await message.nack(requeue=False)
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON: {e}")
                        await message.nack(requeue=False)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        await message.nack(requeue=False)
        except Exception as e:
            logger.error(f"Error consuming messages from RabbitMQ: {e}")

    async def close(self):
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed.")
