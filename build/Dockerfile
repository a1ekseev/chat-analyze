# Базовый образ с Python
FROM python:3.13-slim

# Устанавливаем утилиты и зависимости для установки Poetry
RUN apt-get update && apt-get install -y curl && \
    curl -sSL https://install.python-poetry.org | python3 - && \
    apt-get remove -y curl && apt-get autoremove -y && apt-get clean

# Добавляем Poetry в PATH
ENV PATH="/root/.local/bin:$PATH"

# Указываем рабочую директорию в контейнере
WORKDIR /app

# Копируем файлы с зависимостями для Poetry
COPY pyproject.toml poetry.lock /app

# Устанавливаем зависимости в контейнер
RUN poetry config virtualenvs.create false && \
    poetry install --no-root --only main

# Копируем исходный код приложения
COPY /app /app

# Указываем команду для запуска приложения
CMD ["python", "message_listner.py"]