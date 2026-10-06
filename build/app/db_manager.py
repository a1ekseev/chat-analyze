from datetime import datetime

import asyncpg
import logging

logger = logging.getLogger(__name__)


async def get_db_pool(postgresql_url: str):
    try:
        pool = await asyncpg.create_pool(dsn=postgresql_url)
        logger.info("Подключение к PostgreSQL установлено")
        return pool
    except Exception as e:
        logger.error(f"Ошибка при подключении к PostgreSQL: {e}")
        raise


async def create_table_if_not_exists(pool: asyncpg.Pool):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS messages
        (
            chat_id       BIGINT                   NOT NULL,
            message_id    BIGINT                   NOT NULL,
            chat_username TEXT,
            chat_name     TEXT,
            chat_type     TEXT                     NOT NULL,
            message       TEXT,
            views         BIGINT                   NOT NULL,
            created_at    TIMESTAMP WITH TIME ZONE NOT NULL,
            updated_at    TIMESTAMP WITH TIME ZONE,
            PRIMARY KEY  (chat_id, message_id, created_at)
        );
    """

    try:
        async with pool.acquire() as connection:
            await connection.execute(create_table_query)
            logger.info("Таблица 'messages' успешно создана или уже существует")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise


async def save_message_to_db(connection: asyncpg.Connection, message_data):
    try:
        created_at = datetime.strptime(message_data["created_at"], '%Y-%m-%d %H:%M:%S.%f') if message_data[
            "created_at"] else None
        updated_at = datetime.strptime(message_data["updated_at"], '%Y-%m-%d %H:%M:%S.%f') if message_data[
            "updated_at"] else None

        insert_query = """
        INSERT INTO messages (
            chat_id, message_id, chat_username, chat_name, chat_type, message, views, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (chat_id, message_id, created_at) DO UPDATE
            SET message    = $10,
                updated_at = $11;
        """

        await connection.execute(insert_query,
                                 message_data["chat_id"],
                                 message_data["message_id"],
                                 message_data["chat_username"],
                                 message_data["chat_name"],
                                 message_data["chat_type"],
                                 message_data["message"],
                                 message_data["views"],
                                 created_at,
                                 updated_at,
                                 message_data["message"],
                                 updated_at
                                 )

        logger.info(f"Сообщение {message_data['message_id']} сохранено в базе данных")
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении сообщения в базе данных: {e}")
        return False
