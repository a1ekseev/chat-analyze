import logging
import asyncio
from telethon import TelegramClient, events
from rabbitmq_manager import RabbitMQManager
from db_manager import get_db_pool, create_table_if_not_exists
import config


logger = logging.getLogger(__name__)


client = TelegramClient(config.SESSION_NAME, config.API_ID, config.API_HASH)


def get_sender_name(sender):
    if hasattr(sender, 'first_name') and sender.first_name:
        return f"{sender.first_name} {sender.last_name or ''}".strip()
    elif hasattr(sender, 'title') and sender.title:
        return sender.title
    return "unknown"


def get_username_from_chat(chat, sender):
    if chat:
        return chat.username
    elif hasattr(sender, 'username'):
        return sender.username
    return None


def format_datetime(date_time):
    if date_time:
        return date_time.strftime('%Y-%m-%d %H:%M:%S.') + f'{date_time.microsecond:06d}'
    return None


async def handle_message(event, rabbitmq_manager):
    sender = await event.get_sender()
    sender_name = get_sender_name(sender)

    if event.chat and hasattr(event.chat, 'title'):
        chat_name = event.chat.title
        chat_type = 'channel' if event.chat.megagroup is False else 'group'
    else:
        chat_name = sender_name
        chat_type = 'private_chat'

    chat_username = get_username_from_chat(event.chat, sender)

    # Если сообщение содержит только медиа
    message = event.text or None
    created_at = format_datetime(event.message.date)

    # Только для каналов
    count_view = event.message.views or 0

    # Только для каналов
    updated_at = format_datetime(event.message.edit_date)

    rabbitmq_message = {
        "chat_id": event.chat_id,
        "message_id": event.message.id,
        "chat_name": chat_name,
        "chat_type": chat_type,
        "chat_username": chat_username,
        "message": message,
        "views": count_view,
        "created_at": created_at,
        "updated_at": updated_at,
    }

    await rabbitmq_manager.send_message(rabbitmq_message)


@client.on(events.NewMessage)
async def handle_new_message(event):
    await handle_message(event, rabbitmq_manager)


@client.on(events.MessageEdited)
async def handle_message_edit(event):
    await handle_message(event, rabbitmq_manager)


async def main():
    global rabbitmq_manager

    db_pool = await get_db_pool(config.POSTGRESQL_URL)

    await create_table_if_not_exists(db_pool)

    rabbitmq_manager = RabbitMQManager(db_pool, client)

    await rabbitmq_manager.connect(config.RABBITMQ_URL,
                                   config.RABBITMQ_EXCHANGE_NAME,
                                   config.RABBITMQ_QUEUE_NAME,
                                   config.RABBITMQ_ROUTING_KEY)

    asyncio.create_task(rabbitmq_manager.consume_messages())

    async with client:
        logger.info("Telegram Client started")
        await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
