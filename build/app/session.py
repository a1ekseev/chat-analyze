from telethon.sync import TelegramClient

API_ID = ***REMOVED***
API_HASH = '***REMOVED***' 
SESSION_NAME = 'assist2person'

def main():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    with client:
        print("Сессия создана и подключена успешно!")
        me = client.get_me()  # Получаем информацию о текущем аккаунте
        print(f"Имя пользователя: {me.first_name}")
        print(f"Никнейм: @{me.username}" if me.username else "Нет никнейма")

if __name__ == '__main__':
    main()