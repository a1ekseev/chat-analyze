# chat-analyze

Юзербот Telegram (MTProto, обычный аккаунт) на Go. Ловит новые и отредактированные сообщения во всех
чатах аккаунта (личка, группы, супергруппы, форумы, каналы, комментарии), сохраняет их в PostgreSQL
(таблица `messages`, upsert) и помечает чат или тему прочитанным до этого сообщения.

Очередь заданий и сессия Telegram хранятся в PostgreSQL, внешних брокеров нет.

## Быстрый старт

Нужны Docker с compose и [just](https://github.com/casey/just).

```sh
cp .env.example .env     # заполните TG_API_ID, TG_API_HASH, TG_PHONE
just login               # один раз: вход в аккаунт (код и пароль 2FA вводятся интерактивно)
just up                  # postgres + миграции + сервис
```

`TG_API_ID` и `TG_API_HASH` выдаются на <https://my.telegram.org>. Сессия сохраняется в таблице `tg_session`.
Без сессии `run` сразу завершается с кодом 1 и ошибкой `telegram auth required` /
`telegram session not found, run "chat-analyze login"`, не подключаясь к Telegram.

Сервисы `migrate`, `app` и `login` используют один образ `chat-analyze:local`; `just up` собирает его
с версией и коммитом (`VERSION`, `COMMIT`), они видны в логе `service starting`.

Остановка: `just down`. Данные лежат в томе `pgdata`.

## Подкоманды

| Команда | Что делает |
|---|---|
| `chat-analyze run` | запуск сервиса (по умолчанию); без сессии завершается ошибкой, нужен `login` |
| `chat-analyze login` | интерактивный вход, сохраняет сессию в БД (сам применяет миграции) |
| `chat-analyze migrate up\|down\|status` | миграции схемы; нужен только `DATABASE_URL` |
| `chat-analyze healthcheck` | запрос к `/healthz` на `HTTP_ADDR`, код выхода 0/1 (для `HEALTHCHECK`) |
| `chat-analyze version` | версия и коммит |

Через just: `just login`, `just migrate status`, `just migrate down` и т. д.

## Конфигурация

Переменные окружения (шаблон в `.env.example`). Для compose дополнительно `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_DB`.

| Переменная | По умолчанию | Описание |
|---|---|---|
| `DATABASE_URL` | — | обязательна, URL PostgreSQL |
| `TG_API_ID` | — | обязательна, > 0 |
| `TG_API_HASH` | — | обязательна, 32 hex-символа |
| `TG_SESSION_NAME` | `default` | имя сессии в `tg_session` |
| `TG_PHONE` | — | только для `login`, формат E.164 |
| `WORKERS` | `1` | число воркеров, 1..64 (1 = строго последовательно) |
| `JOBS_MAX_ATTEMPTS` | `10` | число попыток задания до статуса dead |
| `JOBS_LEASE` | `2m` | срок аренды задания воркером, не меньше 1s |
| `JOBS_POLL_INTERVAL` | `1s` | интервал опроса очереди, не меньше 100ms |
| `JOBS_RETENTION` | `168h` | сколько хранить выполненные задания |
| `JOBS_DEAD_RETENTION` | `720h` | сколько хранить задания в статусе dead (их payload содержит текст сообщения), > 0 |
| `MARK_READ` | `true` | помечать сообщения прочитанными |
| `HTTP_ADDR` | `:8080` | адрес HTTP-сервера (`/healthz`) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | `json` или `text` |
| `TG_LOG_LEVEL` | `warn` | уровень внутренних логов gotd: `debug`, `info`, `warn`, `error` |
| `SHUTDOWN_TIMEOUT` | `30s` | время на корректную остановку, не меньше 1s |

`SHUTDOWN_TIMEOUT` должен быть меньше времени, которое оркестратор даёт на остановку до SIGKILL
(в `docker-compose.yml` у `app` стоит `stop_grace_period: 40s`; у `docker stop` по умолчанию 10s,
у Kubernetes `terminationGracePeriodSeconds` 30s). Если сервис не успевает остановиться за
`SHUTDOWN_TIMEOUT`, он завершается с кодом 1 (`shutdown timed out`).

Размер пула соединений с БД: не меньше `WORKERS + 4` (каждый воркер держит соединение во время задания,
ещё одно постоянно занято под `LISTEN`). Явный `pool_max_conns` в `DATABASE_URL`
(например `...?sslmode=disable&pool_max_conns=20`) имеет приоритет.

## Пропуски в каналах (backfill)

Если Telegram не может отдать пропущенные апдейты канала (`channel too long`), сервис ставит в ту же
очередь задание `backfill_channel` с `min_id` = последний сохранённый `message_id` канала. Отдельный воркер
догружает историю (`messages.getHistory`, до 1000 страниц по 100) и ставит сообщения в очередь
`save_message` по возрастанию. Задание переживает ошибки, `FLOOD_WAIT` и перезапуск сервиса: короткий
`FLOOD_WAIT` пережидается внутри задания с повтором той же страницы (в сумме до 30 минут, в логе
`flood wait`), более долгий возвращает задание в очередь; остановка сервиса возвращает задание в очередь,
не расходуя попытку (`JOBS_MAX_ATTEMPTS`). Повторный
`channel too long` до его выполнения не сдвигает `min_id` вперёд (`min_id=0` — «истории нет» — не затирает
известную границу). Если сообщений канала в БД ещё нет, берётся
только последняя страница (100 сообщений, в логе `backfill limited`, `reason=no_history`).

## Health

`GET /healthz`: `200 {"status":"ok"}`, если БД отвечает и Telegram-клиент подключён и авторизован;
иначе `503 {"status":"fail","db":"…","telegram":"…"}`.

## Логи

JSON в stdout (`LOG_FORMAT=text` для чтения глазами), ключи в snake_case: `time`, `level`, `msg`
(статическая строка), `component`, далее атрибуты. Уровни: `LOG_LEVEL` для кода сервиса и
`TG_LOG_LEVEL` для внутренних логов библиотеки gotd (по умолчанию `warn`). Сообщения об одном
сообщении Telegram связаны парой `chat_id` + `message_id`, об одном задании - `job_id` + `version`.

**Никогда не попадает в логи:**

- содержимое сообщений: `message`, `raw_text`, `entities`, подписи медиа, цитаты, названия тем;
- персональные данные: названия чатов, имена отправителей, `username`, телефоны, `sender_id`;
- секреты: `TG_API_HASH`, пароль из `DATABASE_URL`, телефон/код/пароль 2FA при `login`, байты сессии,
  `auth_key`, `access_hash`;
- сырые структуры: payload заданий, `tg.Message`/`tg.Updates` целиком, аргументы SQL-запросов,
  `Detail`/`Message`/`Where` ошибок PostgreSQL.

В логах только метаданные: идентификаторы (`chat_id`, `message_id`, `job_id`), типы, длины текста,
длительности, коды ошибок. Это обеспечено типами (`Secret`, `LogValuer`), единой точкой `internal/logx`,
линтерами и сквозным тестом с маркером в тексте.

## Разработка и тесты

Нужен Go 1.27.3 (`toolchain` в `go.mod` подтянется сам), `just`, Docker.

```sh
just lint               # golangci-lint (с тегом integration, как в CI)
just test               # unit-тесты (-race)
just test-integration   # интеграционные и сквозные тесты в docker compose
just vuln               # govulncheck
just ci                 # всё вместе
```

Интеграционные тесты (build tag `integration`) идут против настоящего PostgreSQL и запускаются в compose:
`just test-integration` поднимает временный postgres на tmpfs (`docker-compose.test.yml`) и прогоняет
`go test -tags=integration -race ./...`.

Их можно запустить и против локального PostgreSQL, указав `TEST_DATABASE_URL` (без неё тесты падают, а не
пропускаются; каждый тест создаёт собственную схему):

```sh
TEST_DATABASE_URL='postgres://postgres:test@localhost:5432/test?sslmode=disable' \
  go test -tags=integration -race -count=1 ./...
```

`just test-integration` использует отдельное имя проекта compose (`chat-analyze-test`) и не трогает
контейнеры, сеть и тома основного стека, поднятого через `just up`.

## CI

GitHub Actions (`.github/workflows/ci.yml`): lint (в том числе с тегом `integration`), unit с покрытием,
govulncheck, интеграционные тесты в compose и сборка Docker-образа. Образ публикуется в
`ghcr.io/a1ekseev/chat-analyze` только при push в `master` и тегов `v*`.

## Безопасность

Старые файл сессии и ключи `API_ID`/`API_HASH` (Python-версия) лежали в истории git. История переписана,
но это не отзывает уже скомпрометированные секреты. Обязательно:

1. завершите старый сеанс в Telegram (Настройки - Устройства - завершить сеанс);
2. перевыпустите `API_ID`/`API_HASH` на <https://my.telegram.org>.

Файл `.env` и `*.session` находятся в `.gitignore`, не коммитьте их.
